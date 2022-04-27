package simpledb.transaction.Lock;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public interface Lock {
    void lock(TransactionId tid) throws TransactionAbortedException;

    boolean tryLock(TransactionId tid) throws TransactionAbortedException;

    void unlock(TransactionId tid);

}