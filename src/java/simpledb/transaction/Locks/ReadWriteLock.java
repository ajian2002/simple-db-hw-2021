package simpledb.transaction.Locks;

public interface ReadWriteLock {
    Lock readLock();

    Lock writeLock();
}