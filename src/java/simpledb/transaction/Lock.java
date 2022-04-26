package simpledb.transaction;

public interface Lock {
    void lock(TransactionId tid);

    boolean tryLock(TransactionId tid);

    void unlock(TransactionId tid);

}
