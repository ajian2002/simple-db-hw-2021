package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Debug;
import simpledb.transaction.TransactionId;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/*
LogFile 实现了 SimpleDb 的恢复子系统。这堂课是能够根据需要写入不同的日志记录，但它是
调用者有责任确保预写日志记录和遵循两阶段锁定规则。
<p>
<u> Locking note: </u>
<p>
这里的很多方法都是同步的（防止并发log写从发生）； BufferPool 中的很多方法也是
同步（出于类似原因。）问题是 BufferPool 写入日志记录（在页面上刷新）和
日志文件刷新 BufferPool页面（关于检查点和恢复）。这可能导致死锁。
为了因此，任何需要访问 BufferPool 的 LogFile 操作不得声明为同步的，并且必须以如下块开头：

<p>
<pre>
    synchronized (Database.getBufferPool()) {
       synchronized (this) {

       ..

       }
    }
</pre>

*/

/**
 * <p> 日志文件的格式如下：
 *
 * <ul>
 *
 * <li> 文件的第一个长整数表示文件的偏移量
 * 最后写入的检查点，如果没有检查点，则为 -1
 *
 * <li> 日志中的所有附加数据均由日志记录组成。日志
 * 记录是可变长度的。
 *
 * <li> 每条日志记录都以整数类型和长整数开头
 * 交易编号。
 *
 * <li> 每条日志记录都以一个长整数文件偏移量结束，表示
 * 记录开始的日志文件中的位置。
 *
 * <li> 有五种记录类型：ABORT、COMMIT、UPDATE、BEGIN 和
 * 检查点
 *
 * <li> ABORT、COMMIT 和 BEGIN 记录不包含其他数据
 *
 * <li>更新记录由两个条目组成，一个前图像和一个
 * 后图像。这些图像是序列化的 Page 对象，可以
 * 使用 LogFile.readPageData() 和 LogFile.writePageData() 访问
 * 方法。有关示例，请参见 LogFile.print()。
 *
 * <li> CHECKPOINT 记录由当时的活动事务组成
 * 检查点被采取并且他们在磁盘上的第一条日志记录。格式
 * 记录的数量是事务数的整数计数，以及
 * 作为长整数事务 id 和长整数首记录偏移量
 * 对于每个活跃的交易。
 *
 * </ul>
 */
public class LogFile {

    final File logFile;
    private RandomAccessFile raf;
    Boolean recoveryUndecided; // 没有调用recover()，也没有追加到日志

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final long NO_CHECKPOINT_ID = -1;

    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;

    long currentOffset = -1;//protected by this int pageSize;
    int totalRecords = 0; // for PatchTest protected by this

    final Map<Long, Long> tidToFirstLogRecord = new HashMap<>();

    /**
     * Constructor.
     * Initialize and back the log file with the specified file.
     * We're not sure yet whether the caller is creating a brand new DB,
     * in which case we should ignore the log file, or whether the caller
     * will eventually want to recover (after populating the Catalog).
     * So we make this decision lazily: if someone calls recover(), then
     * do it, while if someone starts adding log file entries, then first
     * throw out the initial log file contents.
     *
     * @param f The log file's name
     */
    public LogFile(File f) throws IOException {
        this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;

        // install shutdown hook to force cleanup on close
        // Runtime.getRuntime().addShutdownHook(new Thread() {
        // public void run() { shutdown(); }
        // });

        //XXX WARNING -- there is nothing that verifies that the specified
        // log file actually corresponds to the current catalog.
        // This could cause problems since we log tableids, which may or
        // may not match tableids in the current catalog.
    }

    // 我们即将附加一条日志记录。如果我们不确定是否
    //     DB想要进行恢复，我们现在确定——它没有。所以截断
    //     日志。
    void preAppend() throws IOException {
        totalRecords++;
        if (recoveryUndecided)
        {
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    public synchronized int getTotalRecords() {
        return totalRecords;
    }

    /**
     * Write an abort record to the log for the specified tid, force
     * the log to disk, and perform a rollback
     *
     * @param tid The aborting transaction.
     */
    public void logAbort(TransactionId tid) throws IOException {
        //在继续之前必须有缓冲池锁，因为这调用回滚

        synchronized (Database.getBufferPool())
        {

            synchronized (this)
            {
                preAppend();
                //Debug.log("ABORT");
                //我们应该验证这是一个实时交易吗？

                // 必须在此处执行此操作，因为回滚仅适用于
                //                 实时交易（需要 tidToFirstLogRecord）
                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                force();
                tidToFirstLogRecord.remove(tid.getId());
            }
        }
    }

    /**
     * Write a commit record to disk for the specified tid,
     * and force the log to disk.
     *
     * @param tid The committing transaction.
     */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        Debug.log("COMMIT " + tid.getId());
        //should we verify that this is a live transaction?

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }

    /**
     * Write an UPDATE record to disk for the specified tid and page
     * (with provided         before and after images.)
     *
     * @param tid    The transaction performing the write
     * @param before The before image of the page
     * @param after  The after image of the page
     * @see Page#getBeforeImage
     */
    public synchronized void logWrite(TransactionId tid, Page before, Page after) throws IOException {
        Debug.log("WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /* update record conists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */
        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());

        writePageData(raf, before);
        writePageData(raf, after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("WRITE OFFSET = " + currentOffset);
    }

    void writePageData(RandomAccessFile raf, Page p) throws IOException {
        PageId pid = p.getId();
        int[] pageInfo = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int j : pageInfo)
        {
            raf.writeInt(j);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        //        Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try
        {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object[] idArgs = new Object[numIdArgs];
            for (int i = 0; i < numIdArgs; i++)
            {
                idArgs[i] = raf.readInt();
            }
            pid = (PageId) idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page) pageConsts[0].newInstance(pageArgs);

            //            Debug.log("READ PAGE OF TYPE " + pageClassName + ", table = " + newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException | InvocationTargetException | IllegalAccessException | InstantiationException e)
        {
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /**
     * Write a BEGIN record for the specified transaction
     *
     * @param tid The transaction that is beginning
     */
    public synchronized void logXactionBegin(TransactionId tid) throws IOException {
        Debug.log("BEGIN");
        if (tidToFirstLogRecord.get(tid.getId()) != null)
        {
            System.err.print("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }
        preAppend();
        raf.writeInt(BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("BEGIN OFFSET = " + currentOffset);
    }

    /**
     * Checkpoint the log and write a checkpoint record.
     */
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        synchronized (Database.getBufferPool())
        {
            synchronized (this)
            {
                //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                Database.getBufferPool().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1); //no tid , but leave space for convenience

                //write list of outstanding transactions
                raf.writeInt(keys.size());
                while (els.hasNext())
                {
                    Long key = els.next();
                    Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + key);
                    raf.writeLong(key);
                    //Debug.log("WRITING CHECKPOINT TRANSACTION OFFSET: " + tidToFirstLogRecord.get(key));
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                //Debug.log("CP OFFSET = " + currentOffset);
            }
        }

        logTruncate();
    }

    /**
     * Truncate any unneeded portion of the log to reduce its space
     * consumption
     */
    public synchronized void logTruncate() throws IOException {
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong();

        long minLogRecord = cpLoc;

        if (cpLoc != -1L)
        {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            @SuppressWarnings("unused") long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD)
            {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt();

            for (int i = 0; i < numOutstanding; i++)
            {
                @SuppressWarnings("unused") long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord)
                {
                    minLogRecord = firstLogRecord;
                }
            }
        }

        // we can truncate everything before minLogRecord
        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);

        //have to rewrite log records since offsets are different after truncation
        while (true)
        {
            try
            {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();

                Debug.log("NEW START = " + newStart);

                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type)
                {
                    case UPDATE_RECORD:
                        Page before = readPageData(raf);
                        Page after = readPageData(raf);

                        writePageData(logNew, before);
                        writePageData(logNew, after);
                        break;
                    case CHECKPOINT_RECORD:
                        int numXactions = raf.readInt();
                        logNew.writeInt(numXactions);
                        while (numXactions-- > 0)
                        {
                            long xid = raf.readLong();
                            long xoffset = raf.readLong();
                            logNew.writeLong(xid);
                            logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                        }
                        break;
                    case BEGIN_RECORD:
                        tidToFirstLogRecord.put(record_tid, newStart);
                        break;
                }

                //all xactions finish with a pointer
                logNew.writeLong(newStart);
                raf.readLong();

            } catch (EOFException e)
            {
                break;
            }
        }

        Debug.log("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        raf.close();
        logFile.delete();
        newFile.renameTo(logFile);
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();
        //print();
    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * @param tid The transaction to rollback
     */
    public void rollback(TransactionId tid) throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool())
        {
            synchronized (this)
            {
                preAppend();
                if (tid == null) throw new NoSuchElementException("tid is null");
                print();
                rollback(tid.getId());
            }
        }
    }

    private void rollback(long tid) throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool())
        {
            synchronized (this)
            {
                preAppend();
                var off = findBeginUnCommit(tid);
                if (off == -1) throw new RuntimeException("off = -1 " + " tid=" + tid);
                raf.seek(off);
                int open = 0;
                while (open != -1)
                {
                    var type = raf.readInt();
                    long cpTid = raf.readLong();
                    switch (type)
                    {
                        case ABORT_RECORD ->
                        {
                            if (tid == cpTid)
                            {
                                if (open == 1) open = -1;
                                else System.out.println("abort but open !=1");
                            }
                            raf.readLong();
                        }
                        case UPDATE_RECORD ->
                        {
                            var before = readPageData(raf);
                            var after = readPageData(raf);
                            raf.readLong();
                            if (cpTid == tid)
                            {
                                if (open != 1) System.out.println("update but open !=1");
                                else
                                    Database.getCatalog().getDatabaseFile(before.getId().getTableId()).writePage(before);
                            }
                        }
                        case BEGIN_RECORD ->
                        {
                            if (tid == cpTid)
                            {
                                if (open == 0) open = 1;
                                else System.out.println("begin but not fitst");
                            }
                            raf.readLong();
                        }
                        case COMMIT_RECORD ->
                        {
                            if (tid == cpTid)
                            {
                                System.out.println("??????");
                            }
                            raf.readLong();
                        }
                        case CHECKPOINT_RECORD ->
                        {
                            int numTransactions = raf.readInt();
                            while (numTransactions-- > 0)
                            {
                                raf.readLong();
                                raf.readLong();
                            }
                            raf.readLong();
                        }

                    }
                }
            }
        }
    }

    /**
     * Shutdown the logging system, writing out whatever state
     * is necessary so that start up can happen quickly (without
     * extensive recovery.)
     */
    public synchronized void shutdown() {
        try
        {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e)
        {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool())
        {
            synchronized (this)
            {
                recoveryUndecided = false;
                // some code goes here
                //                raf.seek(0);
                //                long recover = raf.readLong();
                //                if (recover != -1) raf.seek(recover);
                //
                //                while (true)
                //                {
                //                    try
                //                    {
                //                        int cpType = raf.readInt();
                //                        long cpTid = raf.readLong();
                //
                //                        switch (cpType)
                //                        {
                //                            case BEGIN_RECORD ->
                //                            {
                //                                var temp = raf.readLong();
                //                                tidToFirstLogRecord.put(cpTid, temp);
                //                            }
                //                            case ABORT_RECORD ->
                //                            {
                //                                raf.readLong();
                //                                rollback(cpTid);
                //                                tidToFirstLogRecord.remove(cpTid);
                //                            }
                //                            case COMMIT_RECORD ->
                //                            {
                //                                raf.readLong();
                //                                //redo
                //                                redo(cpTid);
                //                                tidToFirstLogRecord.remove(cpTid);
                //                            }
                //                            case CHECKPOINT_RECORD ->
                //                            {
                //                                int numTransactions = raf.readInt();
                //                                while (numTransactions-- > 0)
                //                                {
                //                                    long ttid = raf.readLong();
                //                                    long firstRecord = raf.readLong();
                //                                    tidToFirstLogRecord.put(ttid, firstRecord);
                //                                }
                //                                raf.readLong();
                //                            }
                //                            case UPDATE_RECORD ->
                //                            {
                //                                //continue
                //                                Page before = readPageData(raf);
                //                                Page after = readPageData(raf);
                //                                raf.readLong();
                //                            }
                //                        }
                //
                //                    } catch (EOFException e)
                //                    {
                //                        //                    e.printStackTrace();
                //                        break;
                //                    }
                //                }

            }
        }


    }

    private void redo(long cpTid) {

    }

    /**
     * Print out a human readable represenation of the log
     */
    public void print() throws IOException {
        long curOffset = raf.getFilePointer();

        raf.seek(0);

        System.out.println("0: checkpoint record at offset " + raf.readLong());

        while (true)
        {
            try
            {
                int cpType = raf.readInt();
                long cpTid = raf.readLong();

                System.out.println((raf.getFilePointer() - (INT_SIZE + LONG_SIZE)) + ": RECORD TYPE " + cpType);
                System.out.println((raf.getFilePointer() - LONG_SIZE) + ": TID " + cpTid);

                switch (cpType)
                {
                    case BEGIN_RECORD:
                        System.out.println(" (BEGIN)");
                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                        break;
                    case ABORT_RECORD:
                        System.out.println(" (ABORT)");
                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                        break;
                    case COMMIT_RECORD:
                        System.out.println(" (COMMIT)");
                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                        break;

                    case CHECKPOINT_RECORD:
                        System.out.println(" (CHECKPOINT)");
                        int numTransactions = raf.readInt();
                        System.out.println((raf.getFilePointer() - INT_SIZE) + ": NUMBER OF OUTSTANDING RECORDS: " + numTransactions);

                        while (numTransactions-- > 0)
                        {
                            long tid = raf.readLong();
                            long firstRecord = raf.readLong();
                            System.out.println((raf.getFilePointer() - (LONG_SIZE + LONG_SIZE)) + ": TID: " + tid);
                            System.out.println((raf.getFilePointer() - LONG_SIZE) + ": FIRST LOG RECORD: " + firstRecord);
                        }
                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                        break;
                    case UPDATE_RECORD:
                        System.out.println(" (UPDATE)");

                        long start = raf.getFilePointer();
                        Page before = readPageData(raf);

                        long middle = raf.getFilePointer();
                        Page after = readPageData(raf);

                        System.out.println(start + ": before image table id " + before.getId().getTableId());
                        System.out.println((start + INT_SIZE) + ": before image page number " + before.getId().getPageNumber());
                        System.out.println((start + INT_SIZE) + " TO " + (middle - INT_SIZE) + ": page data");

                        System.out.println(middle + ": after image table id " + after.getId().getTableId());
                        System.out.println((middle + INT_SIZE) + ": after image page number " + after.getId().getPageNumber());
                        System.out.println((middle + INT_SIZE) + " TO " + (raf.getFilePointer()) + ": page data");

                        System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                        break;
                }

            } catch (EOFException e)
            {
                //e.printStackTrace();
                break;
            }
        }

        // Return the file pointer to its original position
        raf.seek(curOffset);
    }

    public synchronized void force() throws IOException {
        raf.getChannel().force(true);
    }

    private long findBeginUnCommit(long tid) throws IOException {
        long current = raf.getFilePointer();
        long last = -1L;
        var first = tidToFirstLogRecord.get(tid);
        try
        {
            if (first == null)
            {
                raf.seek(0);
                var point = raf.readLong();
                if (point != -1) raf.seek(point);
            }
            else
            {
                raf.seek(first);
            }
            oo:
            while (true)
            {
                try
                {
                    int cpType = raf.readInt();
                    long cpTid = raf.readLong();

                    switch (cpType)
                    {
                        case BEGIN_RECORD:
                            if (cpTid == tid)
                            {
                                last = raf.readLong();
                                System.out.println("last=" + last);
                            }
                            else raf.readLong();
                            break;
                        case ABORT_RECORD:

                            var t = raf.readLong();
                            if (cpTid == tid)
                            {
                                return last;
                            }
                            break;
                        case COMMIT_RECORD:
                            raf.readLong();
                            if (cpTid == tid)
                            {
                                last = -1L;
                                System.out.println("last=" + last);
                            }
                            break;
                        case CHECKPOINT_RECORD:
                            int numTransactions = raf.readInt();
                            while (numTransactions-- > 0)
                            {
                                long ttid = raf.readLong();
                                long firstRecord = raf.readLong();
                                if (ttid == tid)
                                {
                                    last = firstRecord;
                                }
                            }
                            raf.readLong();
                            break;
                        case UPDATE_RECORD:
                            Page before = readPageData(raf);
                            Page after = readPageData(raf);
                            raf.readLong();
                            break;
                    }

                } catch (EOFException e)
                {
                    //                    e.printStackTrace();
                    break;
                }
            }


        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        raf.seek(current);
        return last;
    }
}
