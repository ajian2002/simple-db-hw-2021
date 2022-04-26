package simpledb.transaction;

public interface Lock {
    void lock(TransactionId tid) throws TransactionAbortedException;

    boolean tryLock(TransactionId tid) throws TransactionAbortedException;

    void unlock(TransactionId tid);

}
