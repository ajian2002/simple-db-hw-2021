package simpledb.transaction.Locks;

import simpledb.transaction.TransactionId;

public interface ReadWriteLock {
    Lock readLock();

    Lock writeLock();

    void UnlockALL(TransactionId tid);
}