package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.Lock.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.utils.LogPrint;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.Assert.assertNotNull;

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
    private LockManager lockManager;
    private PagesManager pagesManager;
    ;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        lockManager = new LockManager();
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
        Page page = pagesManager.get(pid);
        if (page == null)
        {
            DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = f.readPage(pid);
            if (page != null) pagesManager.put(page);
            else
            {
                System.out.println("pid=" + pid.getTableId() + ":" + pid.getPageNumber());
                return null;
            }
        }

        try
        {
            switch (perm)
            {
                case READ_ONLY -> {
                    lockManager.getReadLock(pid, tid);
                }
                case READ_WRITE -> {
                    lockManager.getWriteLock(pid, tid);
                }
            }
        } catch (TransactionAbortedException e)
        {
            LogPrint.print("[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":事务中断");
            throw e;
            //            transactionComplete(tid, false);
            //            System.out.println("[" + "pn=" + pid.getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":事务中断,锁释放完成");
        }
        return page;
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
    public synchronized void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseReadWriteLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        try
        {
            flushPages(tid);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        lockManager.getPagesByTid(tid).forEach(pid -> {
            unsafeReleasePage(tid, pid);
        });

    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return lockManager.hasLock(pid, tid);
    }

    public Permissions whichLock(TransactionId tid, PageId pid) {
        return lockManager.whichLock(pid, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit) {
        if (commit)
        {
            transactionComplete(tid);
            LogPrint.print("[" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + ":事务完成");
            //            + Arrays.toString(Thread.currentThread().getStackTrace()
        }
        else
        {
            //恢复脏页面
            lockManager.getPagesByTid(tid).forEach(this::discardPage);
            //放锁
            lockManager.getPagesByTid(tid).forEach(pid -> {
                unsafeReleasePage(tid, pid);
            });
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
        list.forEach(page -> {
            page.markDirty(true, tid);
        });
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
        list.forEach(page -> {
            page.markDirty(true, tid);
        });
        pagesManager.putAll(list);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public void flushAllPages() throws IOException {
        // some code goes here
        pagesManager.forEachPageId(pid -> {
            try
            {
                if (pagesManager.get(pid).isDirty() != null)
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
    public void discardPage(PageId pid) {
        pagesManager.delete(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid) throws IOException {
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
    public void flushPages(TransactionId tid) throws IOException {
        var values = lockManager.getPagesByTid(tid);
        //        LogPrint.print("[" + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + "size=" + values.size());
        for (PageId pid : values)
        {
            try
            {
                var p = pagesManager.get(pid);
                assertNotNull("p 没了", p);
                // 脏
                LogPrint.print("[" + "pn=" + p.getId().getPageNumber() + ":" + "tid=" + tid.getId() % 100 + "]" + Thread.currentThread().getName() + " 事物关联页" + p.getId().getPageNumber() + " " + ((p.isDirty() != null) ? "脏" : "不脏"));
                if (p.isDirty() != null)
                {
                    flushPage(pid);
                }
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        //        var pid = pagesManager.getLRUPage();
        PageId pid = pagesManager.getNotDirtyLRUPage();
        if (pid == null) return;
        try
        {
            //不脏
            flushPage(pid);
            discardPage(pid);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private class PagesManager {

        private final Map<PageId, PageAndTime> pages;
        private final Map<Long, PageId> times;
        private final int numPages;

        public PagesManager(int numPages) {
            this.numPages = numPages;
            times = new TreeMap<>(Comparator.naturalOrder());
            pages = Collections.synchronizedMap(new HashMap<>(numPages));
        }

        public synchronized Page get(PageId pid) {
            if (pid == null) return null;
            var pa = pages.get(pid);
            if (pa == null) return null;
            var pp = times.get(pa.time);
            times.remove(pa.time);
            times.put(System.currentTimeMillis(), pp);
            return pa.page;
        }

        public synchronized void put(Page page) throws DbException {
            if (page == null) return;
            var time = System.currentTimeMillis();
            if (!pages.containsKey(page.getId())) if (pages.size() == numPages) evictPage();
            pages.put(page.getId(), new PageAndTime(time, page, page.getId()));
            times.put(time, page.getId());
        }

        public void putAll(Collection<? extends Page> p) throws DbException {
            for (Page page : p)
            {
                put(page);
            }
        }

        public synchronized void delete(PageId pageId) {
            var pa = pages.get(pageId);
            if (pa == null) return;
            times.remove(pa.time);
            pages.remove(pageId);
        }

        public synchronized void forEachPageId(Consumer<PageId> action) {
            if (action == null) return;
            pages.keySet().forEach(action);
        }

        public synchronized void forEachPageAndTime(Consumer<PageAndTime> action) {
            if (action == null) return;
            pages.values().forEach(action);
        }

        public PageId getLRUPage() {
            //            System.out.println("LRU First page: " + times.firstKey());
            //            System.out.println("LRU Last page: " + times.lastKey());
            //            var enty = times.firstEntry();
            //            if (enty == null) return null;
            //            return enty.getValue();
            return null;
        }

        public synchronized PageId getNotDirtyLRUPage() throws DbException {
            boolean allDirty = false;
            for (PageAndTime pageAndTime : pages.values())
            {
                PageId e = pageAndTime.id;
                if (pages.get(e).page.isDirty() == null) return e;
                allDirty = true;
            }
            if (allDirty)
            {
                throw new DbException("all page is dirty");
            }
            return null;
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
