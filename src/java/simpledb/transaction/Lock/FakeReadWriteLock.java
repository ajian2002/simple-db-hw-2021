package simpledb.transaction.Lock;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.utils.LogPrint;

import java.util.HashSet;
import java.util.Set;

public class FakeReadWriteLock implements ReadWriteLock {
    private static final Long TIMEOUT = 500L;
    private static final Long SLEEPTIME = 100L;
    private static final Long RANDOMTIME = 10L;
    private final Set<TransactionId> rlist = new HashSet<>();
    private final Set<TransactionId> wlist = new HashSet<>();
    private final Lock r = new ReadLock(this);
    private final Lock w = new WriteLock(this);

    public Lock readLock() {
        return r;
    }


    public Lock writeLock() {
        return w;
    }

    @Override
    public String toString() {
        StringBuilder sbr = new StringBuilder("{");
        rlist.forEach(e -> sbr.append(",").append(e.getId()));
        sbr.append("}");
        StringBuilder sbw = new StringBuilder();
        wlist.forEach(e -> sbw.append(",").append(e.getId()));
        sbr.append("}");
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
        public synchronized void lock(TransactionId tid) throws TransactionAbortedException {
            if (tid == null) return;
            if (f.rlist.contains(tid)) return;
            if (f.wlist.contains(tid)) return;
            if (f.wlist.size() == 0)
            {
                f.rlist.add(tid);
            }
            else
            {
                ThreadLocal time = new ThreadLocal();
                time.set(System.currentTimeMillis());
                var rt = (int) (Math.random() * RANDOMTIME);
                while (f.wlist.size() != 0)
                {
                    try
                    {
                        Thread.sleep(SLEEPTIME + rt);
                        if (System.currentTimeMillis() - (Long) (time.get()) > TIMEOUT)
                        {
                            throw new TransactionAbortedException();
                        }
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
                lock(tid);
            }
        }

        @Override
        public synchronized boolean tryLock(TransactionId tid) throws TransactionAbortedException {
            if (tid == null) return false;
            if (f.rlist.contains(tid)) return true;
            if (f.wlist.contains(tid)) return true;
            if (f.wlist.size() == 0)
            {
                lock(tid);
                return true;
            }
            else return false;

        }

        @Override
        public synchronized void unlock(TransactionId tid) {
            if (tid == null) return;
            f.rlist.remove(tid);
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
        public synchronized void lock(TransactionId tid) throws TransactionAbortedException {

            if (tid == null) return;
            if (f.wlist.contains(tid)) return;
            if (f.rlist.contains(tid)) f.rlist.remove(tid);
            if (f.rlist.size() == 0 && f.wlist.size() == 0)
            {
                f.wlist.add(tid);
                //                LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":阻塞申请写锁:" + "成功");
            }
            else
            {
                ThreadLocal time = new ThreadLocal();
                time.set(System.currentTimeMillis());
                var rt = (int) (Math.random() * RANDOMTIME);
                var count = 1;
                while (f.rlist.size() != 0 || f.wlist.size() != 0)
                {
                    try
                    {
                        Thread.sleep(SLEEPTIME + rt);
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                    if (System.currentTimeMillis() - (Long) (time.get()) > TIMEOUT)
                    {
                        throw new TransactionAbortedException();
                    }
                    else
                    {
                        LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":阻塞申请写锁:" + "失败" + count++);
                    }
                }
                lock(tid);
            }
        }

        @Override
        public synchronized boolean tryLock(TransactionId tid) throws TransactionAbortedException {
            if (tid == null) return false;
            if (f.wlist.contains(tid)) return true;
            if (f.rlist.contains(tid)) f.rlist.remove(tid);
            if (f.rlist.size() == 0 && f.wlist.size() == 0)
            {
                lock(tid);
                return true;
            }
            return false;
        }

        @Override
        public synchronized void unlock(TransactionId tid) {
            if (tid == null) return;
            f.wlist.remove(tid);
        }
    }


}

