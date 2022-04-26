package simpledb.transaction.Lock;

public interface ReadWriteLock {
    Lock readLock();

    Lock writeLock();
}