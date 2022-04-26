package simpledb.transaction;

public interface ReadWriteLock {
    Lock readLock();

    Lock writeLock();
}

