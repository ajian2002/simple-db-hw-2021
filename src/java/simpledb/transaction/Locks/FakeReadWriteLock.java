package simpledb.transaction.Locks;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.utils.LogPrint;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class FakeReadWriteLock implements ReadWriteLock {
    private static final Long TIMEOUT = 500L;
    private static final Long SLEEPTIME = 50L;
    private static final Long RANDOMTIME = 100L;
    private final Set<TransactionId> rlist = Collections.synchronizedSet(new HashSet<>());
    private final Set<TransactionId> wlist = Collections.synchronizedSet(new HashSet<>());
    private final Lock r = new ReadLock(this);
    private final Lock w = new WriteLock(this);

    public Lock readLock() {
        return r;
    }


    public Lock writeLock() {
        return w;
    }

    @Override
    public synchronized void UnlockALL(TransactionId tid) {
        rDelete(tid);
        wDelete(tid);
    }

    private synchronized int wSize() {
        return wlist.size();
    }

    private synchronized int rSize() {
        return rlist.size();
    }

    private synchronized void rDelete(TransactionId tid) {
        rlist.remove(tid);
    }

    private synchronized void wDelete(TransactionId tid) {
        wlist.remove(tid);
    }

    private synchronized boolean canSetRead(TransactionId tid) {
        if (wSize() == 0)
        {
            setRead(tid);
            return true;
        }
        return false;
    }

    private synchronized boolean canSetWrite(TransactionId tid) {
        if (wSize() == 0)
        {
            if ((rSize() == 1 && hasRead(tid)) || (rSize() == 0))
            {
                setWrite(tid);
                return true;
            }
        }
        return false;
    }

    private synchronized void setRead(TransactionId tid) {
        if (hasRead(tid)) return;
        if (hasWrite(tid))
        {
            wlist.remove(tid);
        }
        rlist.add(tid);
    }

    private synchronized void setWrite(TransactionId tid) {
        if (hasWrite(tid)) return;
        if (hasRead(tid))
        {
            rlist.remove(tid);
        }
        wlist.add(tid);
    }

    private synchronized boolean allEmpty() {
        return wSize() == 0 && rSize() == 0;
    }

    private synchronized boolean hasLock(TransactionId tid) {
        return rlist.contains(tid) || wlist.contains(tid);
    }

    private synchronized boolean hasRead(TransactionId tid) {
        return rlist.contains(tid);
    }

    private synchronized boolean hasWrite(TransactionId tid) {
        return wlist.contains(tid);
    }

    @Override
    public String toString() {
        StringBuilder sbr = new StringBuilder("{");
        rlist.forEach(e -> sbr.append(",").append(e.getId()));
        sbr.append("}");

        StringBuilder sbw = new StringBuilder("{");
        wlist.forEach(e -> sbw.append(",").append(e.getId()));
        sbw.append("}");
        return "{" + "r:" + rlist.size() + "w:" + wlist.size() + " " + (rlist.size() != 0 ? ("r=" + sbr) : "") + (wlist.size() != 0 ? "w=" + sbw : "") + '}';
    }

    private static class ReadLock implements Lock {
        @Override
        public String toString() {
            return f.toString();
        }

        private FakeReadWriteLock f;

        public ReadLock(FakeReadWriteLock f) {
            this.f = f;
        }


        @Override
        public void lock(TransactionId tid) throws TransactionAbortedException {
            if (tid == null) return;
            if (f.hasLock(tid)) return;
            if (!f.canSetRead(tid))
            {
                ThreadLocal<Long> time = new ThreadLocal<>();
                time.set(System.currentTimeMillis());
                var rt = (int) (Math.random() * RANDOMTIME);
                var count = 1;
                while (!f.canSetRead(tid))
                {
                    LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + "等待加读锁");
                    LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + f.toString());
                    if (System.currentTimeMillis() - time.get() > TIMEOUT)
                    {
                        LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + "加读锁失败,事务中断");
                        throw new TransactionAbortedException();
                    }
                    else
                    {
                        LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":阻塞申请读锁:" + "失败" + count++);
                    }
                    try
                    {
                        Thread.sleep(SLEEPTIME + rt);
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
                LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + "加读锁成功");
            }
        }


        @Override
        public boolean tryLock(TransactionId tid) throws TransactionAbortedException {
            if (tid == null) return false;
            if (f.hasLock(tid)) return true;
            return f.canSetRead(tid);
        }

        @Override
        public void unlock(TransactionId tid) {
            if (tid == null) return;
            f.rDelete(tid);
        }
    }

    private static class WriteLock implements Lock {
        @Override
        public String toString() {
            return f.toString();
        }

        private FakeReadWriteLock f;

        public WriteLock(FakeReadWriteLock f) {
            this.f = f;
        }

        @Override
        public void lock(TransactionId tid) throws TransactionAbortedException {

            if (tid == null) return;
            if (f.hasWrite(tid)) return;
            if (!f.canSetWrite(tid))
            {
                ThreadLocal time = new ThreadLocal();
                time.set(System.currentTimeMillis());
                var rt = (int) (Math.random() * RANDOMTIME);
                var count = 1;
                while (!f.canSetWrite(tid))
                {
                    LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + "等待加写锁");
                    LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + f.toString());

                    if (System.currentTimeMillis() - (Long) (time.get()) > TIMEOUT)
                    {
                        LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + "等待加写锁失败,事务中断");
                        throw new TransactionAbortedException();
                    }
                    else
                    {
                        LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":阻塞申请写锁:" + "失败" + count++);
                    }
                    try
                    {
                        Thread.sleep(SLEEPTIME + rt);
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
                LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + "加写锁成功");
            }
        }


        @Override
        public boolean tryLock(TransactionId tid) throws TransactionAbortedException {
            if (tid == null) return false;
            if (f.hasWrite(tid)) return true;
            //                if (f.rlist.contains(tid)) f.rlist.remove(tid);
            return f.canSetWrite(tid);
        }

        @Override
        public void unlock(TransactionId tid) {
            if (tid == null) return;
            f.wDelete(tid);
        }
    }


}

