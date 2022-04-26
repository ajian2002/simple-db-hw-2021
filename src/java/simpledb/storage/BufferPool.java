package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;
    private final LockManager lockManager = new LockManager();
    private final PagesManager pagesManager;
    ;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        pagesManager = new PagesManager(numPages);
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {
        switch (perm)
        {
            case READ_ONLY -> {
                lockManager.getReadLock(pid, tid);
            }
            case READ_WRITE -> {
                lockManager.getWriteLock(pid, tid);
            }
        }

        // some code goes here
        var page = pagesManager.get(pid);
        if (page != null) return page;
        try
        {
            var f = Database.getCatalog().getDatabaseFile(pid.getTableId());
            var p = f.readPage(pid);
            if (p != null) pagesManager.put(p);
            return p;
        } catch (NoSuchElementException | IndexOutOfBoundsException e)
        {
            return null;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseReadWriteLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.getPagesByTid(tid).forEach(pid -> {
            unsafeReleasePage(tid, pid);
        });

    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.hasLock(pid, tid);
    }

    public Permissions whichLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.whichLock(pid, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        //        var pages = lockManager.getPagesByTid(tid);
        if (commit)
        {
            transactionComplete(tid);
        }
        else
        {
            //??????TODO
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException, TransactionAbortedException {
        var list = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        pagesManager.putAll(list);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        var list = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        pagesManager.putAll(list);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        pagesManager.forEachPageId(pid -> {
            try
            {
                if (Objects.requireNonNull(pagesManager.get(pid)).isDirty() != null)
                {
                    flushPage(pid);
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        });
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        pagesManager.delete(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        var page = pagesManager.get(pid);
        if (page != null)
        {
            page.markDirty(false, null);
            Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.getPagesByTid(tid).forEach(pid -> {
            try
            {
                // 脏
                if (Objects.requireNonNull(pagesManager.get(pid)).isDirty() != null)
                {
                    flushPage(pid);
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        });
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        var pid = pagesManager.getLRUPage();
        try
        {
            //不脏
            if (Objects.requireNonNull(pagesManager.get(pid)).isDirty() == null)
            {
                flushPage(pid);
                discardPage(pid);
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private class PagesManager {

        private HashMap<PageId, PageAndTime> pages;
        private TreeMap<Long, PageId> times;
        private int numPages;

        public PagesManager(int numPages) {
            this.numPages = numPages;
            times = new TreeMap<>(Comparator.naturalOrder());
            pages = new HashMap<>(numPages);
        }

        public Page get(PageId pid) {
            if (pid == null) return null;
            var pa = pages.get(pid);
            if (pa == null) return null;
            times.remove(pa.time);
            put(pa.page);
            return pa.page;
        }

        public void put(Page page) {
            if (page == null) return;
            var time = System.currentTimeMillis();
            if (pages.size() == numPages)
            {
                try
                {
                    evictPage();
                } catch (DbException e)
                {
                    e.printStackTrace();
                }
            }
            pages.put(page.getId(), new PageAndTime(time, page, page.getId()));
            times.put(time, page.getId());
        }

        public void putAll(Collection<? extends Page> p) {
            p.forEach(this::put);
        }

        public void delete(PageId pageId) {
            var pa = pages.get(pageId);
            if (pa == null) return;
            times.remove(pa.time);
            pages.remove(pageId);
        }

        public void forEachPageId(Consumer<PageId> action) {
            //            assertNotNull(action);
            if (action == null) return;
            pages.keySet().forEach(action);
        }

        public void forEachPageAndTime(Consumer<PageAndTime> action) {
            //            assertNotNull(action);
            if (action == null) return;
            pages.values().forEach(action);
        }

        public PageId getLRUPage() {
            //            System.out.println("LRU First page: " + times.firstKey());
            //            System.out.println("LRU Last page: " + times.lastKey());
            var enty = times.firstEntry();
            if (enty == null) return null;
            return enty.getValue();
        }

        private class PageAndTime {
            Long time;
            Page page;
            PageId id;

            public PageAndTime(Long time, Page page, PageId id) {
                this.time = time;
                this.page = page;
                this.id = id;
            }
        }
    }

}
