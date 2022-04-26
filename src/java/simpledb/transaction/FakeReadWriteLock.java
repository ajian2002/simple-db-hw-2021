package simpledb.transaction;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class FakeReadWriteLock implements ReadWriteLock {
    private final ArrayList<TransactionId> rlist = new ArrayList<>();
    private final ArrayList<TransactionId> wlist = new ArrayList<>();
    private final java.util.concurrent.locks.Lock l = new ReentrantLock();
    private final java.util.concurrent.locks.Condition wNotZero = l.newCondition();
    private final java.util.concurrent.locks.Condition rNotZero = l.newCondition();
    private final Lock r = new ReadLock(this);
    private final Lock w = new WriteLock(this);

    public Lock readLock() {
        return r;
    }

    public Lock writeLock() {
        return w;
    }

    private static class ReadLock implements Lock {

        private FakeReadWriteLock f;

        public ReadLock(FakeReadWriteLock f) {
            this.f = f;
        }


        @Override
        public void lock(TransactionId tid) {
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
                while (f.wlist.size() != 0) ;
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
        public boolean tryLock(TransactionId tid) {
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
        public void unlock(TransactionId tid) {
            if (tid == null) return;
            f.rlist.remove(tid);
        }
    }

    private static class WriteLock implements Lock {

        private FakeReadWriteLock f;

        public WriteLock(FakeReadWriteLock f) {
            this.f = f;
        }

        @Override
        public void lock(TransactionId tid) {
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
                //                l.lock();
                //                try
                //                {
                while (f.rlist.size() != 0 || f.wlist.size() != 0) ;
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
        public boolean tryLock(TransactionId tid) {
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
        public void unlock(TransactionId tid) {
            if (tid == null) return;
            f.wlist.remove(tid);
        }
    }


}

