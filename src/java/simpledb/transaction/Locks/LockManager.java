package simpledb.transaction.Locks;

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


    private final Map<PageId, Data> dataLockMap = Collections.synchronizedMap(new HashMap<>());
    public final Map<TransactionId, Set<PageId>> tidLockMap = Collections.synchronizedMap(new HashMap<>());

    private Data getData(PageId pid) {
        if (pid == null) return null;
        if (dataLockMap.containsKey(pid)) return dataLockMap.get(pid);
        else
        {
            synchronized (dataLockMap)
            {
                if (!dataLockMap.containsKey(pid))
                {
                    var data = new Data(pid);
                    dataLockMap.put(pid, data);
                    return data;
                }
                else return dataLockMap.get(pid);
            }
        }
    }

    private List<LockStatus> getLockStatus(PageId pid) {
        return getData(pid).getLists();
    }

    public void getWriteLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        var data = getData(pid);
        var lockStatus = getLockStatus(pid);
        LockStatus temp = null;
        boolean has = false;
        synchronized (tidLockMap)
        {
            if (!tidLockMap.containsKey(tid))
            {
                var newList = Collections.synchronizedSet(new HashSet<PageId>());
                newList.add(pid);
                tidLockMap.put(tid, newList);
            }
            else
            {
                var oldList = tidLockMap.get(tid);
                oldList.add(pid);
                //                tidLockMap.put(tid, oldList);
            }
        }
        synchronized (lockStatus)//锁链遍历查找
        {
            for (LockStatus ls : lockStatus)
            {
                synchronized (ls)
                {
                    if (ls.tid.equals(tid))
                    {
                        temp = ls;
                        has = true;
                        break;
                    }
                }
            }
        }

        if (has)
        {
            synchronized (temp)
            {
                if (temp.isReadLock)
                {
                    TryLock(data, tid, temp, "[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":已有读锁,申请写锁:", true);
                    BlockLock(data, tid, temp, "[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":已有读锁,阻塞申请写锁:", true);
                }
                else return;//            else System.out.println("已有写锁,申请写锁");
            }
        }
        else
        {
            temp = NewLock(data, tid, temp, "[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":未持有锁,申请写锁:", true);
            BlockLock(data, tid, temp, "[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":未持有锁,阻塞申请写锁:", true);
        }

        synchronized (lockStatus)
        {
            if (has) lockStatus.set(lockStatus.indexOf(temp), temp);
            else lockStatus.add(temp);
        }
    }


    public void getReadLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        var data = getData(pid);
        var lockStatus = getLockStatus(pid);
        LockStatus temp = null;
        boolean has = false;
        synchronized (tidLockMap)
        {
            if (!tidLockMap.containsKey(tid))
            {
                var newList = Collections.synchronizedSet(new HashSet<PageId>());
                newList.add(pid);
                tidLockMap.put(tid, newList);
            }
            else
            {
                var oldList = tidLockMap.get(tid);
                synchronized (oldList)
                {
                    oldList.add(pid);
                }
                //                tidLockMap.put(tid, oldList);
            }
        }
        synchronized (lockStatus)//锁链遍历查找
        {
            for (LockStatus ls : lockStatus)
            {
                synchronized (ls)
                {
                    if (ls.tid.equals(tid))
                    {
                        temp = ls;
                        has = true;
                        break;
                    }
                }
            }
        }
        if (!has)
        {
            temp = NewLock(data, tid, temp, "[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":未持有锁,申请读锁:", false);
            BlockLock(data, tid, temp, "[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":未持有锁,阻塞申请读锁:", false);
        }
        else
            //            if (temp.isWriteLock) //"已有写锁,申请读锁,测试要求不实现"
            // else          System.out.println("已有读锁,申请读锁");
            return;
        synchronized (lockStatus)
        {
            if (has) lockStatus.set(lockStatus.indexOf(temp), temp);
            else lockStatus.add(temp);
        }

    }


    public void releaseReadWriteLock(PageId pid, TransactionId tid) {

        var lockStatus = getLockStatus(pid);
        synchronized (lockStatus)
        {
            for (LockStatus ls : lockStatus)
            {
                synchronized (ls)
                {
                    if (tid.equals(ls.tid))
                    {
                        //                        if (ls.gettedLock)
                        {
                            //                            ls.lock.unlock(tid);
                            ls.d.unlock(tid);
                            //?
                            LogPrint.print("[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":释放" + (ls.gettedLock && ls.isReadLock ? "读锁" : ls.gettedLock && ls.isWriteLock ? "写锁" : "未知锁"));
                        }
                        lockStatus.remove(ls);

                        synchronized (tidLockMap.get(tid))
                        {
                            tidLockMap.get(tid).remove(pid);
                        }
                        break;
                    }
                }
            }
        }
    }

    public boolean hasLock(PageId pid, TransactionId tid) {
        var lockStatus = getLockStatus(pid);
        synchronized (lockStatus)
        {
            for (LockStatus ls : lockStatus)
            {
                synchronized (ls)
                {
                    if (tid.equals(ls.tid))
                    {
                        if (ls.gettedLock) return true;
                        break;
                    }
                }
            }
        }
        return false;
    }

    public Permissions whichLock(PageId pid, TransactionId tid) {

        var lockStatus = getLockStatus(pid);
        synchronized (lockStatus)
        {
            for (LockStatus ls : lockStatus)
            {
                synchronized (ls)
                {
                    if (tid.equals(ls.tid))
                    {
                        if (ls.gettedLock) return ls.isReadLock ? Permissions.READ_ONLY : Permissions.READ_WRITE;
                        break;
                    }
                }
            }
        }
        return null;
    }

    public List<PageId> getPagesByTid(TransactionId tid) {
        //        List<PageId> pages = new ArrayList<PageId>();
        //        synchronized (dataLockMap)
        //        {
        //            for (Data e : dataLockMap.values())
        //            {
        //                var lockStatus = e.getLists();
        //                synchronized (lockStatus)
        //                {
        //                    for (LockStatus ls : lockStatus)
        //                    {
        //
        //                        if (ls.gettedLock && ls.tid.equals(tid))
        //                        {
        //                            pages.add(e.getPid());
        //                            break;
        //                        }
        //
        //                    }
        //                }
        //            }
        //        }
        synchronized (tidLockMap)
        {
            ArrayList<PageId> newList = new ArrayList<>();
            if (tidLockMap.containsKey(tid))
            {
                newList.addAll(tidLockMap.get(tid));
            }
            return newList;
        }
        //        return pages;
    }

    private void BlockLock(LockManager.Data data, TransactionId tid, LockStatus temp, String s, boolean write) throws TransactionAbortedException {
        synchronized (temp)
        {
            if (!temp.gettedLock)
            {
                try
                {
                    if (!temp.gettedLock && temp.lock != null) temp.lock.lock(tid);
                    if (write) temp.setWriteLock();
                    else temp.setReadLock();
                } finally
                {
                    if (write) LogPrint.print(s + (temp.gettedLock && temp.isWriteLock ? "成功" : "失败"));
                    else LogPrint.print(s + (temp.gettedLock && temp.isReadLock ? "成功" : "失败"));
                }
            }
        }

    }

    private void TryLock(LockManager.Data data, TransactionId tid, LockStatus temp, String s, boolean write) throws TransactionAbortedException {
        LogPrint.print(s);
        synchronized (temp)
        {
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
    }

    private LockStatus NewLock(LockManager.Data data, TransactionId tid, LockStatus temp, String s, boolean write) throws TransactionAbortedException {
        LogPrint.print(s);
        temp = new LockStatus(tid, data);
        synchronized (temp)
        {
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
        private final List<LockStatus> lists = Collections.synchronizedList(new ArrayList<>());
        private PageId pid = null;

        public Data(PageId pid) {
            this.pid = pid;
        }

        public List<LockStatus> getLists() {
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

        public void unlock(TransactionId tid) {
            lock.UnlockALL(tid);
        }
    }
}

