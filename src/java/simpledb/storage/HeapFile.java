package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private BufferPool pool;
    private File file;
    private TupleDesc td;
    private RandomAccessFile raf;
    private ArrayList<HeapPage> pages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        //        this.it = new
        this.pool = Database.getBufferPool();

        try
        {
            this.raf = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }

    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int id = pid.getTableId();
        int pagenum = pid.getPageNumber();
        try
        {
            int off = pagenum * BufferPool.getPageSize();
            raf.seek(off);
            byte[] bytes = new byte[BufferPool.getPageSize()];
            raf.read(bytes, off, BufferPool.getPageSize());
            var page = new HeapPage(new HeapPageId(id, pagenum), bytes);
            //            pool.
            return page;
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int number = 0;
        try
        {
            number = (int) (raf.length() / BufferPool.getPageSize());
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return number;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new AbstractDbFileIterator() {
            boolean open = false;
            int pages;
            int currentPage;
            Iterator<Tuple> it = null;

            @Override
            public void close() {
                super.close();
                open = false;
            }

            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if (!open) return null;
                if (it == null)
                {
                    updateIt(currentPage);
                    if (it == null) return null;
                }
                if (it.hasNext()) return it.next();
                currentPage++;
                it = null;
                return readNext();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (open) return;
                pages = numPages();
                currentPage = 0;
                open = true;
                //                updateIt(currentPage);
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (!open) return;
                currentPage = 0;
                updateIt(currentPage);
            }


            private void updateIt(int currentPage) throws DbException, TransactionAbortedException {
                if (!open) return;
                if (currentPage >= pages) return;
                PageId pid = new HeapPageId(getId(), currentPage);
                //                HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                HeapPage p = (HeapPage) pool.getPage(tid, pid, Permissions.READ_ONLY);
                if (p == null) p = (HeapPage) readPage(pid);
                if (p != null) it = p.iterator();
                else it = null;
            }
        };
    }

}

