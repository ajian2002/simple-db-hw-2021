package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 锁管理器,基于PageID
 */
public class LockManager {
    private HashMap<PageId, Data> dataLockMap = new HashMap<PageId, Data>();

    private Data getData(PageId pid) {
        if (dataLockMap.containsKey(pid))
        {
            return dataLockMap.get(pid);
        }
        var data = new Data(pid);
        dataLockMap.put(pid, data);
        return data;
    }

    private List<LockStatus> getLockStatus(PageId pid) {
        return getData(pid).getLists();
    }

    public void getWriteLock(PageId pid, TransactionId tid) {
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
                System.out.println("已有读锁,申请写锁" + pid.getTableId() + ":" + pid.getPageNumber() + " " + Thread.currentThread().getId());
                temp.setWriteLock();
                if (!temp.gettedLock)
                {
                    temp.lock.lock();
                    temp.gettedLock = true;
                }
            }
            //            else System.out.println("已有写锁,申请写锁");
        }
        else
        {
            temp = new LockStatus(tid, data);
            temp.setWriteLock();
            data.lists.add(temp);
            if (!temp.gettedLock)
            {
                temp.lock.lock();
                temp.gettedLock = true;
            }
        }
        data.lists.set(data.lists.indexOf(temp), temp);
    }

    public void getReadLock(PageId pid, TransactionId tid) {
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
                System.out.println("已有写锁,申请读锁,测试要求不实现" + pid.getTableId() + ":" + pid.getPageNumber() + " " + Thread.currentThread().getId());
                //                temp.setReadLock();
                //                data.lists.set(data.lists.indexOf(temp), temp);
                //                if (!temp.gettedLock)
                //                {
                //                    temp.lock.lock();
                //                    temp.gettedLock = true;
                //                }
            }
            // else          System.out.println("已有读锁,申请读锁");

        }
        else
        {
            temp = new LockStatus(tid, data);
            temp.setReadLock();
            data.lists.add(temp);
            if (!temp.gettedLock)
            {
                temp.lock.lock();
                temp.gettedLock = true;
            }
        }
        data.lists.set(data.lists.indexOf(temp), temp);
    }

    public void releaseReadWriteLock(PageId pid, TransactionId tid) {
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
                        ls.lock.unlock();
                    } catch (Exception ignored)
                    {

                    }
                }
                lockStatus.remove(ls);
                System.out.println("释放 " + (ls.isReadLock ? "读锁" : "写锁") + pid.getTableId() + ":" + pid.getPageNumber() + " " + Thread.currentThread().getId());
                break;
            }
        }
    }

    public boolean hasLock(PageId pid, TransactionId tid) {
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

    public List<PageId> getPagesByTid(TransactionId tid) {
        List<PageId> pages = new ArrayList<PageId>();
        dataLockMap.values().forEach(e -> {
            e.getLists().forEach(ee -> {
                if (ee.tid.equals(tid)) pages.add(e.getPid());
            });
        });
        return pages;
    }

    private class Data {
        private final ReadWriteLock lock = new ReentrantReadWriteLock();
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

    private class LockStatus {
        private TransactionId tid;
        private Data d;
        private Lock lock;
        private boolean isReadLock = false;
        private boolean isWriteLock = false;
        private boolean gettedLock = false;

        public LockStatus(TransactionId tid, Data d) {
            this.tid = tid;
            this.d = d;
        }

        @Override
        public String toString() {
            return "LockStatus{" + "tid=" + tid + ", d=" + d + ", lock=" + lock + ", isReadLock=" + isReadLock + ", isWriteLock=" + isWriteLock + ", gettedLock=" + gettedLock + '}';
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

        public void setReadLock() {
            if (gettedLock)
            {
                //                if (isWriteLock)
                //                {
                //                    if (d.lists.size() == 1 && d.lists.get(0).tid.equals(tid))
                //                    {
                //                        try
                //                        {
                //                            d.getRlock().lock();
                //                            lock.unlock();
                //                            lock = d.getRlock();
                //                            isReadLock = true;
                //                            isWriteLock = false;
                //                        } catch (Exception e)
                //                        {
                //
                //                        }
                //                    }
                //                }
                //                var w = d.getWlock();
                //                var r = d.getRlock();
                //                if (!gettedLock)
                //                {
                //                    lock.lock();
                //                    gettedLock = true;
                //                }
                //                w.unlock();
                //                lock = r;

            }
            else if (!gettedLock && isWriteLock)
            {
                //                if (d.lists.size() == 1 && d.lists.get(0).tid.equals(tid))
                //                {
                //                    try
                //                    {
                //                        d.getRlock().lock();
                //                        lock.unlock();
                //                        lock = d.getRlock();
                //                        isReadLock = true;
                //                        isWriteLock = false;
                //                    } catch (Exception e)
                //                    {
                gettedLock = lock.tryLock();
                //                    }
                //                }
            }
            else if (!gettedLock && isReadLock)
            {
                var r = d.getRlock();
                gettedLock = r.tryLock();
                lock = r;
                isReadLock = true;
                isWriteLock = false;
            }
            else if (!gettedLock && !isReadLock && !isWriteLock)
            {
                var r = d.getRlock();
                gettedLock = r.tryLock();
                lock = r;
                isReadLock = true;
                isWriteLock = false;
            }

        }

        public void setWriteLock() {
            var w = d.getWlock();
            //单一读锁可升级为写锁
            if (gettedLock && isReadLock && d.lists.size() == 1 && d.lists.get(0).tid.equals(tid))
            {
                try
                {
                    lock.unlock();
                } catch (Exception ignored)
                {

                }
            }
            gettedLock = w.tryLock();
            lock = w;
            isWriteLock = true;
            isReadLock = false;
        }

    }

}
