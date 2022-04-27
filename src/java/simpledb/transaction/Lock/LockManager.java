package simpledb.transaction.Lock;

import simpledb.common.Permissions;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.utils.LogPrint;

import java.util.*;

/**
 * 锁管理器,基于PageID
 */
public class LockManager {


    private HashMap<PageId, Data> dataLockMap = new HashMap<PageId, Data>();

    private synchronized Data getData(PageId pid) {
        if (dataLockMap.containsKey(pid))
        {
            return dataLockMap.get(pid);
        }
        var data = new Data(pid);
        dataLockMap.put(pid, data);
        return data;
    }

    private synchronized List<LockStatus> getLockStatus(PageId pid) {
        return getData(pid).getLists();
    }

    public synchronized void getWriteLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        var data = getData(pid);
        var lockStatus = getLockStatus(pid);
        LockStatus temp = null;
        boolean has = false;
        for (LockStatus d : lockStatus)
        {
            if (d.tid.equals(tid))
            {
                temp = d;
                has = true;
                break;
            }
        }
        if (has)
        {
            if (temp.isReadLock)
            {
                TryLock(data, tid, temp, "[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":已有读锁,申请写锁:", true);
                BlockLock(data, tid, temp, "[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":已有读锁,阻塞申请写锁:", true);
            }
            //            else System.out.println("已有写锁,申请写锁");
            //            else logger.info("已有写锁,申请写锁");
        }
        else
        {
            temp = NewLock(data, tid, temp, "[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":未持有锁,申请写锁:", true);
            BlockLock(data, tid, temp, "[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":未持有锁,阻塞申请写锁:", true);
        }
        data.lists.set(data.lists.indexOf(temp), temp);
    }

    public synchronized void getReadLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        var data = getData(pid);
        LockStatus temp = null;
        boolean has = false;
        for (LockStatus ls : data.getLists())
        {
            if (ls.tid.equals(tid))
            {
                temp = ls;
                has = true;
                break;
            }
        }
        if (has)
        {
            if (temp.isWriteLock)
            {
                //                {
                //                    s = "已有写锁,申请读锁,测试要求不实现" + "[" + pid.getTableId() % 100 + ":" + pid.getPageNumber() + "]" + Thread.currentThread().getName();
                //                    //                    System.out.println(s);
                //                    logger.info(s);
                //                }

            }
            // else          System.out.println("已有读锁,申请读锁");
            // else          logger.info("已有读锁,申请读锁");

        }
        else
        {
            temp = NewLock(data, tid, temp, "[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":未持有锁,申请读锁:", false);
            BlockLock(data, tid, temp, "[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":未持有锁,阻塞申请读锁:", false);
        }
        data.lists.set(data.lists.indexOf(temp), temp);
    }

    public synchronized void releaseReadWriteLock(PageId pid, TransactionId tid) {
        var data = getData(pid);
        var lockStatus = getLockStatus(pid);
        for (LockStatus ls : lockStatus)
        {
            if (tid.equals(ls.tid))
            {
                if (ls.gettedLock)
                {
                    try
                    {
                        ls.lock.unlock(tid);
                        LogPrint.print("[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":释放" + (ls.gettedLock && ls.isReadLock ? "读锁" : ls.gettedLock && ls.isWriteLock ? "写锁" : "未知锁") + Arrays.toString(Thread.currentThread().getStackTrace()));
                    } catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }
                lockStatus.remove(ls);
                break;
            }
        }
    }

    public synchronized boolean hasLock(PageId pid, TransactionId tid) {
        var data = getData(pid);
        var lockStatus = getLockStatus(pid);
        for (LockStatus ls : lockStatus)
        {
            if (tid.equals(ls.tid))
            {
                if (ls.gettedLock) return true;
                break;
            }
        }
        return false;
    }

    public synchronized Permissions whichLock(PageId pid, TransactionId tid) {
        if (hasLock(pid, tid))
        {
            var data = getData(pid);
            var lockStatus = getLockStatus(pid);
            for (LockStatus ls : lockStatus)
            {
                if (tid.equals(ls.tid))
                {
                    if (ls.gettedLock)
                    {
                        return ls.isReadLock ? Permissions.READ_ONLY : Permissions.READ_WRITE;
                    }
                }
            }

        }
        return null;
    }

    public synchronized List<PageId> getPagesByTid(TransactionId tid) {
        List<PageId> pages = new ArrayList<PageId>();
        dataLockMap.values().forEach(e -> {
            e.getLists().forEach(ee -> {
                if (ee.tid.equals(tid)) pages.add(e.getPid());
            });
        });
        return pages;
    }

    private synchronized void BlockLock(LockManager.Data data, TransactionId tid, LockStatus temp, String s, boolean write) throws TransactionAbortedException {
        if (!temp.gettedLock)
        {
            try
            {
                if (!temp.gettedLock && temp.lock != null)
                {
                    temp.lock.lock(tid);
                }

                if (write) temp.setWriteLock();
                else temp.setReadLock();

                data.lists.set(data.lists.indexOf(temp), temp);
            } catch (TransactionAbortedException e)
            {
                throw e;
            } finally
            {
                if (write) LogPrint.print(s + (temp.gettedLock && temp.isWriteLock ? "成功" : "失败"));
                else LogPrint.print(s + (temp.gettedLock && temp.isReadLock ? "成功" : "失败"));
            }
        }
    }

    private synchronized void TryLock(LockManager.Data data, TransactionId tid, LockStatus temp, String s, boolean write) throws TransactionAbortedException {
        LogPrint.print(s);
        if (write)
        {
            temp.setWriteLock();
            LogPrint.print(s + (temp.gettedLock && temp.isWriteLock ? "成功" : "失败"));
        }
        else
        {
            temp.setReadLock();
            LogPrint.print(s + (temp.gettedLock && temp.isReadLock ? "成功" : "失败"));
        }
    }

    private synchronized LockStatus NewLock(LockManager.Data data, TransactionId tid, LockStatus temp, String s, boolean write) throws TransactionAbortedException {
        LogPrint.print(s);
        temp = new LockStatus(tid, data);
        if (write)
        {
            temp.setWriteLock();
            LogPrint.print(s + (temp.gettedLock && temp.isWriteLock ? "成功" : "失败"));
        }
        else
        {
            temp.setReadLock();
            LogPrint.print(s + (temp.gettedLock && temp.isReadLock ? "成功" : "失败"));
        }
        data.lists.add(temp);
        return temp;
    }


    private class LockStatus {
        private TransactionId tid;
        private LockManager.Data d;
        private Lock lock;
        private boolean isReadLock = false;
        private boolean isWriteLock = false;
        private boolean gettedLock = false;

        public LockStatus(TransactionId tid, LockManager.Data d) {
            this.tid = tid;
            this.d = d;
        }

        @Override
        public String toString() {
            return "LockStatus{" + "tid=" + tid + (gettedLock ? (isReadLock ? "ReadLock" : isWriteLock ? "WriteLock" : "持有但未知") : "未持有锁") + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof LockStatus)) return false;
            LockStatus that = (LockStatus) o;
            return tid.equals(that.tid) && d.equals(that.d);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tid, d);
        }


        public synchronized void setReadLock() throws TransactionAbortedException {
            LogPrint.print("[" + "pn=" + d.pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + "设置读锁前" + d.lock.toString());
            var r = d.getRlock();
            gettedLock = r.tryLock(tid);
            lock = r;
            isReadLock = true;
            isWriteLock = false;
            LogPrint.print("[" + "pn=" + d.pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + "设置读锁后" + d.lock.toString());
        }

        public synchronized void setWriteLock() throws TransactionAbortedException {
            LogPrint.print("[" + "pn=" + d.pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + "设置写锁前" + d.lock.toString());
            var w = d.getWlock();
            //单一读锁可升级为写锁
            gettedLock = w.tryLock(tid);
            //            if (gettedLock || lock == null)
            //            {
            lock = w;
            isWriteLock = true;
            isReadLock = false;
            //            }
            LogPrint.print(("[" + "pn=" + d.pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + "设置写锁后" + d.lock.toString()));
        }

    }

    private class Data {
        private final ReadWriteLock lock = new FakeReadWriteLock();
        private final ArrayList<LockStatus> lists = new ArrayList<>();
        private PageId pid = null;

        public Data(PageId pid) {
            this.pid = pid;
        }

        public ArrayList<LockStatus> getLists() {
            return lists;
        }

        public PageId getPid() {
            return pid;
        }

        public Lock getRlock() {
            return lock.readLock();
        }

        public Lock getWlock() {
            return lock.writeLock();
        }
    }
}

