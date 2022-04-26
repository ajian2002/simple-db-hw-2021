package simpledb.transaction;

import java.util.ArrayList;

public class FakeReadWriteLock implements ReadWriteLock {
    private static final Long TIMEOUT = 3000L;
    private final ArrayList<TransactionId> rlist = new ArrayList<>();
    private final ArrayList<TransactionId> wlist = new ArrayList<>();
    //    private final java.util.concurrent.locks.Lock l = new ReentrantLock();
    //    private final java.util.concurrent.locks.Condition wNotZero = l.newCondition();
    //    private final java.util.concurrent.locks.Condition rNotZero = l.newCondition();
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
                //                rNotZero.signal();
            }
            else
            {
                //                l.lock();
                //                try
                //                {
                ThreadLocal time = new ThreadLocal();
                time.set(System.currentTimeMillis());
                while (f.wlist.size() != 0)
                {
                    try
                    {
                        var rt = (int) (Math.random() * 10);
                        Thread.sleep(300 + rt);
                        if (System.currentTimeMillis() - (Long) (time.get()) > TIMEOUT + rt)
                        {
                            System.out.println("申请读锁失败,抛出异常 " + Thread.currentThread().getName());
                            throw new TransactionAbortedException();
                        }
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }

                //                        wNotZero.await();
                lock(tid);
                //                } catch (InterruptedException e)
                //                {
                //                    e.printStackTrace();
                //                } finally
                //                {
                //                    l.unlock();
                //                }
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
            if (f.rlist.contains(tid) && f.wlist.size() == 0)
            {
                f.rlist.remove(tid);
                lock(tid);
            }
            if (f.rlist.size() == 0 && f.wlist.size() == 0)
            {
                f.wlist.add(tid);
                //                wNotZero.signal();
            }
            else
            {
                ThreadLocal time = new ThreadLocal();
                time.set(System.currentTimeMillis());
                //                l.lock();
                //                try
                //                {
                while (f.rlist.size() != 0 || f.wlist.size() != 0)
                {
                    try
                    {
                        var rt = (int) (Math.random() * 10);
                        Thread.sleep(300 + rt);
                        if (System.currentTimeMillis() - (Long) (time.get()) > TIMEOUT + rt)
                        {
                            throw new TransactionAbortedException();
                        }
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
                ;
                //                    rNotZero.await();
                lock(tid);
                //                } catch (InterruptedException e)
                //                {
                //                    e.printStackTrace();
                //                } finally
                //                {
                //                    l.unlock();
                //                }
            }
        }

        @Override
        public synchronized boolean tryLock(TransactionId tid) throws TransactionAbortedException {
            if (tid == null) return false;
            if (f.wlist.contains(tid)) return true;
            if (f.rlist.contains(tid) && f.wlist.size() == 0)
            {
                f.rlist.remove(tid);
                return tryLock(tid);
            }
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

