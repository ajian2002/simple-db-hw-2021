package simpledb.index;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.execution.IndexPredicate;
import simpledb.execution.Predicate.Op;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * BTreeFile is an implementation of a DbFile that stores a B+ tree.
 * Specifically, it stores a pointer to a root page,
 * a set of internal pages, and a set of leaf pages, which contain a collection of tuples
 * in sorted order. BTreeFile works closely with BTreeLeafPage, BTreeInternalPage,
 * and BTreeRootPtrPage. The format of these pages is described in their constructors.
 *
 * @author Becca Taft
 * @see BTreeLeafPage#BTreeLeafPage
 * @see BTreeInternalPage#BTreeInternalPage
 * @see BTreeHeaderPage#BTreeHeaderPage
 * @see BTreeRootPtrPage#BTreeRootPtrPage
 */
public class BTreeFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    private final int tableid;
    private final int keyField;

    /**
     * Constructs a B+ tree file backed by the specified file.
     *
     * @param f   - the file that stores the on-disk backing store for this B+ tree
     *            file.
     * @param key - the field which index is keyed on
     * @param td  - the tuple descriptor of tuples in the file
     */
    public BTreeFile(File f, int key, TupleDesc td) {
        this.f = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.keyField = key;
        this.td = td;
    }

    /**
     * Returns the File backing this BTreeFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this BTreeFile. Implementation note:
     * you will need to generate this tableid somewhere and ensure that each
     * BTreeFile has a "unique id," and that you always return the same value for
     * a particular BTreeFile. We suggest hashing the absolute file name of the
     * file underlying the BTreeFile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this BTreeFile.
     */
    public int getId() {
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * Read a page from the file on disk. This should not be called directly
     * but should be called from the BufferPool via getPage()
     *
     * @param pid - the id of the page to read from disk
     * @return the page constructed from the contents on disk
     */
    public Page readPage(PageId pid) {
        BTreePageId id = (BTreePageId) pid;

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f)))
        {
            if (id.pgcateg() == BTreePageId.ROOT_PTR)
            {
                byte[] pageBuf = new byte[BTreeRootPtrPage.getPageSize()];
                int retval = bis.read(pageBuf, 0, BTreeRootPtrPage.getPageSize());
                if (retval == -1)
                {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retval < BTreeRootPtrPage.getPageSize())
                {
                    throw new IllegalArgumentException("Unable to read " + BTreeRootPtrPage.getPageSize() + " bytes from BTreeFile");
                }
                Debug.log(1, "BTreeFile.readPage: read page %d", id.getPageNumber());
                return new BTreeRootPtrPage(id, pageBuf);
            }
            else
            {
                byte[] pageBuf = new byte[BufferPool.getPageSize()];
                if (bis.skip(BTreeRootPtrPage.getPageSize() + (long) (id.getPageNumber() - 1) * BufferPool.getPageSize()) != BTreeRootPtrPage.getPageSize() + (long) (id.getPageNumber() - 1) * BufferPool.getPageSize())
                {
                    throw new IllegalArgumentException("Unable to seek to correct place in BTreeFile");
                }
                int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
                if (retval == -1)
                {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retval < BufferPool.getPageSize())
                {
                    throw new IllegalArgumentException("Unable to read " + BufferPool.getPageSize() + " bytes from BTreeFile");
                }
                Debug.log(1, "BTreeFile.readPage: read page %d", id.getPageNumber());
                if (id.pgcateg() == BTreePageId.INTERNAL)
                {
                    return new BTreeInternalPage(id, pageBuf, keyField);
                }
                else if (id.pgcateg() == BTreePageId.LEAF)
                {
                    return new BTreeLeafPage(id, pageBuf, keyField);
                }
                else
                { // id.pgcateg() == BTreePageId.HEADER
                    return new BTreeHeaderPage(id, pageBuf);
                }
            }
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        // Close the file on success or error
        // Ignore failures closing the file
    }

    /**
     * Write a page to disk.  This should not be called directly but should
     * be called from the BufferPool when pages are flushed to disk
     *
     * @param page - the page to write to disk
     */
    public void writePage(Page page) throws IOException {
        BTreePageId id = (BTreePageId) page.getId();

        byte[] data = page.getPageData();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        if (id.pgcateg() == BTreePageId.ROOT_PTR)
        {
            rf.write(data);
            rf.close();
        }
        else
        {
            rf.seek(BTreeRootPtrPage.getPageSize() + (long) (page.getId().getPageNumber() - 1) * BufferPool.getPageSize());
            rf.write(data);
            rf.close();
        }
    }

    /**
     * Returns the number of pages in this BTreeFile.
     */
    public int numPages() {
        // we only ever write full pages
        return (int) ((f.length() - BTreeRootPtrPage.getPageSize()) / BufferPool.getPageSize());
    }

    /**
     * Returns the index of the field that this B+ tree is keyed on
     */
    public int keyField() {
        return keyField;
    }

    /**
     * Recursive function which finds and locks the leaf page in the B+ tree corresponding to
     * the left-most page possibly containing the key field f. It locks all internal
     * nodes along the path to the leaf node with READ_ONLY permission, and locks the
     * leaf node with permission perm.
     * <p>
     * If f is null, it finds the left-most leaf page -- used for the iterator
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid        - the current page being searched
     * @param perm       - the permissions with which to lock the leaf page
     * @param f          - the field to search for
     * @return the left-most leaf page possibly containing the key field f
     */
    private BTreeLeafPage findLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm, Field f) throws DbException, TransactionAbortedException {
        //        if (pid == null) return null;
        var oldp = getPage(tid, dirtypages, pid, perm);
        if (oldp == null) return null;
        if (!(oldp instanceof BTreePage)) throw new DbException("not Btree Page");
        BTreeLeafPage result = null;
        //        System.out.println(BTreePageId.categToString(pid.pgcateg()) + " " + "find=" + f + " ");
        if (BTreePageId.categToString(pid.pgcateg()).equals("LEAF"))
        {
            BTreeLeafPage temp = (BTreeLeafPage) oldp;
            return temp;

            //            if (f == null)
            //            {
            //                result = temp;
            //                return result;
            //            }
            //            var it = temp.iterator();
            //            while (it.hasNext())
            //            {
            //                var t = it.next();
            //                var itt = t.fields();
            //                while (itt.hasNext())
            //                {
            //                    var current = itt.next();
            //                    //                    System.out.print("  current=" + current);
            //                    if (f.compare(Op.EQUALS, current))
            //                    {
            //                        //                        System.out.println("");
            //                        return temp;
            //                    }
            //                }
            //            }
            //            System.out.println("temp=" + temp);
            //            System.out.println("leaf中未找到");
            //            return temp;
            //            var rpid = temp.getRightSiblingId();
            //            return findLeafPage(tid, dirtypages, rpid, perm, f);

        }
        else if (BTreePageId.categToString(pid.pgcateg()).equals("INTERNAL"))
        {
            BTreeInternalPage temp = (BTreeInternalPage) oldp;
            var it = temp.iterator();
            BTreeEntry entry = null;
            if (f == null)
            {
                if (it.hasNext())
                {
                    entry = it.next();
                    return findLeafPage(tid, dirtypages, entry.getLeftChild(), perm, f);
                }
                else throw new DbException("internal page 无数据");
            }
            else
            {
                while (it.hasNext())
                {
                    entry = it.next();
                    var current = entry.getKey();
                    if (f.equals(current) || f.compare(Op.LESS_THAN_OR_EQ, current))
                        return findLeafPage(tid, dirtypages, entry.getLeftChild(), perm, f);
                }
                if (entry == null) throw new DbException("internal page 无数据");
                else return findLeafPage(tid, dirtypages, entry.getRightChild(), perm, f);
            }
        }
        else throw new DbException("page type" + BTreePageId.categToString(pid.pgcateg()));
    }

    /**
     * Convenience method to find a leaf page when there is no dirtypages HashMap.
     * Used by the BTreeFile iterator.
     *
     * @param tid - the transaction id
     * @param pid - the current page being searched
     * @param f   - the field to search for
     * @return the left-most leaf page possibly containing the key field f
     */
    BTreeLeafPage findLeafPage(TransactionId tid, BTreePageId pid, Field f) throws DbException, TransactionAbortedException {
        return findLeafPage(tid, new HashMap<>(), pid, Permissions.READ_ONLY, f);
    }

    /**
     * Split a leaf page to make room for new tuples and recursively split the parent node
     * as needed to accommodate a new entry. The new entry should have a key matching the key field
     * of the first tuple in the right-hand page (the key is "copied up"), and child pointers
     * pointing to the two leaf pages resulting from the split.  Update sibling pointers and parent
     * pointers as needed.
     * <p>
     * Return the leaf page into which a new tuple with key field "field" should be inserted.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the leaf page to split
     * @param field      - the key field of the tuple to be inserted after the split is complete. Necessary to know
     *                   which of the two pages to return.
     * @return the leaf page into which the new tuple should be inserted
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getParentWithEmptySlots(TransactionId, Map, BTreePageId, Field)
     */
    public BTreeLeafPage splitLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage page, Field field) throws DbException, IOException, TransactionAbortedException {

        //        通过在现有页面的右侧添加一个新页面来拆分叶子页面
        //         页面并将一半的元组移动到新页面。向上复制中间键
        //         进入父页面，并根据需要递归拆分父页面以适应
        //         新条目。 getParentWithEmtpySlots() 在这里很有用。别忘了更新
        //         所有受影响叶页的兄弟指针。返回 a 进入的页面
        //         应插入具有给定键字段的元组。
        var currentPage = page;
        BTreeLeafPage newPage = (BTreeLeafPage) getEmptyPage(tid, dirtypages, BTreePageId.LEAF);
        var oldit = currentPage.reverseIterator();
        int movecount = currentPage.getNumTuples() / 2;
        //move
        dirtypages.put(currentPage.getId(), currentPage);
        for (int i = 0; i < movecount && oldit.hasNext(); i++)
        {
            var t = oldit.next();
            currentPage.deleteTuple(t);
            newPage.insertTuple(t);
        }


        //getFirst
        Tuple firstTuple = null;
        var pp = newPage.iterator();
        if (pp.hasNext()) firstTuple = pp.next();
        else throw new DbException("newPage is empty");
        var newParent = getParentWithEmptySlots(tid, dirtypages, currentPage.getParentId(), firstTuple.getField(keyField()));
        dirtypages.put(newParent.getId(), newParent);

        BTreeLeafPage rr = currentPage.getRightSiblingId() == null ? null : (BTreeLeafPage) getPage(tid, dirtypages, currentPage.getRightSiblingId(), Permissions.READ_WRITE);
        if (rr != null) rr.setLeftSiblingId(newPage.getId());
        newPage.setRightSiblingId(rr != null ? rr.getId() : null);
        currentPage.setRightSiblingId(newPage.getId());
        newPage.setLeftSiblingId(currentPage.getId());


        //getParent&&copy

        newParent.insertEntry(new BTreeEntry(firstTuple.getField(keyField()), currentPage.getId(), newPage.getId()));
        updateParentPointers(tid, dirtypages, newParent);
        if (field.compare(Op.LESS_THAN_OR_EQ, firstTuple.getField(keyField()))) return currentPage;
        else return newPage;
    }

    /**
     * Split an internal page to make room for new entries and recursively split its parent page
     * as needed to accommodate a new entry. The new entry for the parent should have a key matching
     * the middle key in the original internal page being split (this key is "pushed up" to the parent).
     * The child pointers of the new parent entry should point to the two internal pages resulting
     * from the split. Update parent pointers as needed.
     * <p>
     * Return the internal page into which an entry with key field "field" should be inserted
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the internal page to split
     * @param field      - the key field of the entry to be inserted after the split is complete. Necessary to know
     *                   which of the two pages to return.
     * @return the internal page into which the new entry should be inserted
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getParentWithEmptySlots(TransactionId, Map, BTreePageId, Field)
     * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
     */
    public BTreeInternalPage splitInternalPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page, Field field) throws DbException, IOException, TransactionAbortedException {
        //          通过在现有页面的右侧添加新页面来拆分内部页面
        //         页面并将一半条目移动到新页面。向上推中间键
        //         进入父页面，并根据需要递归拆分父页面以适应
        //         新条目。 getParentWithEmtpySlots() 在这里很有用。别忘了更新
        //         所有子移动到新页面的父指针。更新父指针（）
        //         在这里会有用。返回具有给定键字段的条目所在的页面
        //         应该插入。

        var currentPage = page;
        BTreeInternalPage newPage = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);
        var oldit = currentPage.reverseIterator();
        int movecount = currentPage.getNumEntries() / 2 + 1;
        dirtypages.put(currentPage.getId(), currentPage);
        for (int i = 0; i < movecount && oldit.hasNext(); i++)
        {
            var t = oldit.next();
            currentPage.deleteKeyAndRightChild(t);
            newPage.insertEntry(t);
        }
        updateParentPointers(tid, dirtypages, newPage);
        updateParentPointers(tid, dirtypages, currentPage);


        BTreeEntry firstEntry = null;
        var pp = newPage.iterator();
        if (pp.hasNext()) firstEntry = pp.next();
        else throw new DbException("newPage is empty");

        var newParent = getParentWithEmptySlots(tid, dirtypages, currentPage.getParentId(), firstEntry.getKey());
        newParent.insertEntry(new BTreeEntry(firstEntry.getKey(), currentPage.getId(), newPage.getId()));
        newPage.deleteKeyAndLeftChild(firstEntry);
        updateParentPointers(tid, dirtypages, newPage);
        updateParentPointers(tid, dirtypages, newParent);
        if (field.compare(Op.LESS_THAN_OR_EQ, firstEntry.getKey())) return currentPage;
        else return newPage;
    }

    /**
     * Method to encapsulate the process of getting a parent page ready to accept new entries.
     * This may mean creating a page to become the new root of the tree, splitting the existing
     * parent page if there are no empty slots, or simply locking and returning the existing parent page.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param parentId   - the id of the parent. May be an internal page or the RootPtr page
     * @param field      - the key of the entry which will be inserted. Needed in case the parent must be split
     *                   to accommodate the new entry
     * @return the parent page, guaranteed to have at least one empty slot
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #splitInternalPage(TransactionId, Map, BTreeInternalPage, Field)
     */
    private BTreeInternalPage getParentWithEmptySlots(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId parentId, Field field) throws DbException, IOException, TransactionAbortedException {

        BTreeInternalPage parent = null;

        // create a parent node if necessary
        // this will be the new root of the tree
        if (parentId.pgcateg() == BTreePageId.ROOT_PTR)
        {
            parent = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);

            // update the root pointer
            BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
            BTreePageId prevRootId = rootPtr.getRootId(); //save prev id before overwriting.
            rootPtr.setRootId(parent.getId());

            // update the previous root to now point to this new root.
            BTreePage prevRootPage = (BTreePage) getPage(tid, dirtypages, prevRootId, Permissions.READ_WRITE);
            prevRootPage.setParentId(parent.getId());
        }
        else
        {
            // lock the parent page
            parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, Permissions.READ_WRITE);
        }

        // split the parent if needed
        if (parent.getNumEmptySlots() == 0)
        {
            parent = splitInternalPage(tid, dirtypages, parent, field);
        }

        return parent;

    }

    /**
     * Helper function to update the parent pointer of a node.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid        - id of the parent node
     * @param child      - id of the child node to be updated with the parent pointer
     * @throws DbException
     * @throws TransactionAbortedException
     */
    private void updateParentPointer(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, BTreePageId child) throws DbException, TransactionAbortedException {

        BTreePage p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_ONLY);

        if (!p.getParentId().equals(pid))
        {
            p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_WRITE);
            p.setParentId(pid);
        }

    }

    /**
     * Update the parent pointer of every child of the given page so that it correctly points to
     * the parent
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the parent page
     * @throws DbException
     * @throws TransactionAbortedException
     * @see #updateParentPointer(TransactionId, Map, BTreePageId, BTreePageId)
     */
    private void updateParentPointers(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page) throws DbException, TransactionAbortedException {
        Iterator<BTreeEntry> it = page.iterator();
        BTreePageId pid = page.getId();
        BTreeEntry e = null;
        while (it.hasNext())
        {
            e = it.next();
            updateParentPointer(tid, dirtypages, pid, e.getLeftChild());
        }
        if (e != null)
        {
            updateParentPointer(tid, dirtypages, pid, e.getRightChild());
        }
    }

    /**
     * Method to encapsulate the process of locking/fetching a page.  First the method checks the local
     * cache ("dirtypages"), and if it can't find the requested page there, it fetches it from the buffer pool.
     * It also adds pages to the dirtypages cache if they are fetched with read-write permission, since
     * presumably they will soon be dirtied by this transaction.
     * <p>
     * This method is needed to ensure that page updates are not lost if the same pages are
     * accessed multiple times.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid        - the id of the requested page
     * @param perm       - the requested permissions on the page
     * @return the requested page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    Page getPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm) throws DbException, TransactionAbortedException {
        if (dirtypages.containsKey(pid))
        {
            return dirtypages.get(pid);
        }
        else
        {
            Page p = Database.getBufferPool().getPage(tid, pid, perm);
            if (perm == Permissions.READ_WRITE)
            {
                dirtypages.put(pid, p);
            }
            return p;
        }
    }

    /**
     * Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
     * May cause pages to split if the page where tuple t belongs is full.
     *
     * @param tid - the transaction id
     * @param t   - the tuple to insert
     * @return a list of all pages that were dirtied by this operation. Could include
     * many pages since parent pointers will need to be updated when an internal node splits.
     * @see #splitLeafPage(TransactionId, Map, BTreeLeafPage, Field)
     */
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        Map<PageId, Page> dirtypages = new HashMap<>();

        // get a read lock on the root pointer page and use it to locate the root page
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId rootId = rootPtr.getRootId();

        if (rootId == null)
        { // the root has just been created, so set the root pointer to point to it
            rootId = new BTreePageId(tableid, numPages(), BTreePageId.LEAF);
            rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
            rootPtr.setRootId(rootId);
        }

        // 找到并锁定与关键字段对应的最左边的叶子页面，
        //         如果没有更多可用的插槽，则拆分叶子页面
        BTreeLeafPage leafPage = findLeafPage(tid, dirtypages, rootId, Permissions.READ_WRITE, t.getField(keyField));
        if (leafPage.getNumEmptySlots() == 0)
        {
            // System.out.println("before splitLeafPage");
            // System.out.println("c.getNumTuples()=" + leafPage.getNumTuples() + " " + "c.getNumEmptySlots()=" + leafPage.getNumEmptySlots());
            leafPage = splitLeafPage(tid, dirtypages, leafPage, t.getField(keyField));
            // BTreeLeafPage r = (BTreeLeafPage) dirtypages.get(leafPage.getRightSiblingId());
            // BTreeLeafPage l = (BTreeLeafPage) dirtypages.get(leafPage.getLeftSiblingId());
            // BTreePage p = (BTreePage) dirtypages.get(leafPage.getParentId());

            // System.out.println("after splitLeafPage");
            // System.out.println((l == null ? "l==null" : ("l.getNumTuples()=" + l.getNumTuples())) + " " + "c.getNumTuples()=" + leafPage.getNumTuples() + " " + (r == null ? "r==null" : ("r.getNumTuples()=" + r.getNumTuples())));
            // System.out.println(p == null ? "p==null" : "p.getNumEmptySlots()=" + p.getNumEmptySlots());
            // System.out.println((l == null ? "l==null" : ("l.getNumEmptySlots()=" + l.getNumEmptySlots())) + " " + "c.getNumEmptySlots()=" + leafPage.getNumEmptySlots() + " " + (r == null ? "r==null" : ("r.getNumEmptySlots()=" + r.getNumEmptySlots())));

        }

        // 将元组插入叶子页面
        leafPage.insertTuple(t);

        return new ArrayList<>(dirtypages.values());
    }

    /**
     * Handle the case when a B+ tree page becomes less than half full due to deletions.
     * If one of its siblings has extra tuples/entries, redistribute those tuples/entries.
     * Otherwise merge with one of the siblings. Update pointers as needed.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the page which is less than half full
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #handleMinOccupancyLeafPage(TransactionId, Map, BTreeLeafPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
     * @see #handleMinOccupancyInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
     */
    private void handleMinOccupancyPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePage page) throws DbException, IOException, TransactionAbortedException {
        BTreePageId parentId = page.getParentId();
        BTreeEntry leftEntry = null;
        BTreeEntry rightEntry = null;
        BTreeInternalPage parent = null;

        // 通过父母找到左右兄弟姐妹，所以我们确保他们有 与页面相同的父级。在父项中找到对应的条目          页面和兄弟姐妹
        if (parentId.pgcateg() != BTreePageId.ROOT_PTR)
        {
            parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, Permissions.READ_WRITE);
            Iterator<BTreeEntry> ite = parent.iterator();
            while (ite.hasNext())
            {
                BTreeEntry e = ite.next();
                if (e.getLeftChild().equals(page.getId()))
                {
                    rightEntry = e;
                    break;
                }
                else if (e.getRightChild().equals(page.getId()))
                {
                    leftEntry = e;
                }
            }
        }

        if (page.getId().pgcateg() == BTreePageId.LEAF)
        {
            handleMinOccupancyLeafPage(tid, dirtypages, (BTreeLeafPage) page, parent, leftEntry, rightEntry);
        }
        else
        { // BTreePageId.INTERNAL
            handleMinOccupancyInternalPage(tid, dirtypages, (BTreeInternalPage) page, parent, leftEntry, rightEntry);
        }
    }

    /**
     * Handle the case when a leaf page becomes less than half full due to deletions.
     * If one of its siblings has extra tuples, redistribute those tuples.
     * Otherwise merge with one of the siblings. Update pointers as needed.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the leaf page which is less than half full
     * @param parent     - the parent of the leaf page
     * @param leftEntry  - the entry in the parent pointing to the given page and its left-sibling
     * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #mergeLeafPages(TransactionId, Map, BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry)
     * @see #stealFromLeafPage(BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry, boolean)
     */
    private void handleMinOccupancyLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage page, BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry) throws DbException, IOException, TransactionAbortedException {
        BTreePageId leftSiblingId = null;
        BTreePageId rightSiblingId = null;
        if (leftEntry != null) leftSiblingId = leftEntry.getLeftChild();
        if (rightEntry != null) rightSiblingId = rightEntry.getRightChild();

        int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples() / 2; // ceiling
        if (leftSiblingId != null)
        {
            BTreeLeafPage leftSibling = (BTreeLeafPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
            // if the left sibling is at minimum occupancy, merge with it. Otherwise
            // steal some tuples from it
            if (leftSibling.getNumEmptySlots() >= maxEmptySlots)
            {
                mergeLeafPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            }
            else
            {
                stealFromLeafPage(page, leftSibling, parent, leftEntry, false);
            }
        }
        else if (rightSiblingId != null)
        {
            BTreeLeafPage rightSibling = (BTreeLeafPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
            // if the right sibling is at minimum occupancy, merge with it. Otherwise
            // steal some tuples from it
            if (rightSibling.getNumEmptySlots() >= maxEmptySlots)
            {
                mergeLeafPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }
            else
            {
                stealFromLeafPage(page, rightSibling, parent, rightEntry, true);
            }
        }
    }

    /**
     * Handle the case when an internal page becomes less than half full due to deletions.
     * If one of its siblings has extra entries, redistribute those entries.
     * Otherwise merge with one of the siblings. Update pointers as needed.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the internal page which is less than half full
     * @param parent     - the parent of the internal page
     * @param leftEntry  - the entry in the parent pointing to the given page and its left-sibling
     * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #mergeInternalPages(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
     * @see #stealFromLeftInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
     * @see #stealFromRightInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
     */
    private void handleMinOccupancyInternalPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page, BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry) throws DbException, IOException, TransactionAbortedException {
        BTreePageId leftSiblingId = null;
        BTreePageId rightSiblingId = null;
        if (leftEntry != null) leftSiblingId = leftEntry.getLeftChild();
        if (rightEntry != null) rightSiblingId = rightEntry.getRightChild();

        int maxEmptySlots = page.getMaxEntries() - page.getMaxEntries() / 2; // ceiling
        if (leftSiblingId != null)
        {
            BTreeInternalPage leftSibling = (BTreeInternalPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
            // if the left sibling is at minimum occupancy, merge with it. Otherwise
            // steal some entries from it
            if (leftSibling.getNumEmptySlots() >= maxEmptySlots)
            {
                mergeInternalPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            }
            else
            {
                stealFromLeftInternalPage(tid, dirtypages, page, leftSibling, parent, leftEntry);
            }
        }
        else if (rightSiblingId != null)
        {
            BTreeInternalPage rightSibling = (BTreeInternalPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
            // if the right sibling is at minimum occupancy, merge with it. Otherwise
            // steal some entries from it
            if (rightSibling.getNumEmptySlots() >= maxEmptySlots)
            {
                mergeInternalPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }
            else
            {
                stealFromRightInternalPage(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }
        }
    }


    /**
     * Steal tuples from a sibling and copy them to the given page so that both pages are at least
     * half full.  Update the parent's entry so that the key matches the key field of the first
     * tuple in the right-hand page.
     *
     * @param page           - the leaf page which is less than half full
     * @param sibling        - the sibling which has tuples to spare
     * @param parent         - the parent of the two leaf pages
     * @param entry          - the entry in the parent pointing to the two leaf pages
     * @param isRightSibling - whether the sibling is a right-sibling
     * @throws DbException
     */
    public void stealFromLeafPage(BTreeLeafPage page, BTreeLeafPage sibling, BTreeInternalPage parent, BTreeEntry entry, boolean isRightSibling) throws DbException {
        //将一些元组从同级移动到页面，以便
        //元组是均匀分布的。一定要更新
        //对应的父条目。
        if (page == null || sibling == null || parent == null || entry == null) return;
        int currentCount = page.getNumTuples();
        int siblingCount = sibling.getNumTuples();
        if (currentCount + siblingCount < page.getMaxTuples())
        {
            System.out.println("merge Leaf");
            return;
        }
        int changeCount = (siblingCount - currentCount) / 2;
        if (changeCount <= 0) return;
        Iterator<Tuple> it;
        if (isRightSibling)
        {
            it = sibling.iterator();
        }
        else
        {
            it = sibling.reverseIterator();
        }

        for (int i = 0; i < changeCount && it.hasNext(); i++)
        {
            var t = it.next();
            sibling.deleteTuple(t);
            page.insertTuple(t);
        }
        Tuple first = null;
        if (isRightSibling)
        {
            it = sibling.iterator();
        }
        else
        {
            it = page.iterator();
        }
        first = it.next();

        //        System.out.println(first.getField(keyField()));
        //        System.out.println(page.reverseIterator().next().getField(keyField()));
        //        System.out.println(sibling.iterator().next().getField(keyField()));
        page.setParentId(parent.getId());
        sibling.setParentId(parent.getId());
        entry.setKey(first.getField(keyField()));
        parent.updateEntry(entry);
    }

    /**
     * Steal entries from the left sibling and copy them to the given page so that both pages are at least
     * half full. Keys can be thought of as rotating through the parent entry, so the original key in the
     * parent is "pulled down" to the right-hand page, and the last key in the left-hand page is "pushed up"
     * to the parent.  Update parent pointers as needed.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param page        - the internal page which is less than half full
     * @param leftSibling - the left sibling which has entries to spare
     * @param parent      - the parent of the two internal pages
     * @param parentEntry - the entry in the parent pointing to the two internal pages
     * @throws DbException
     * @throws TransactionAbortedException
     * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
     */
    public void stealFromLeftInternalPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page, BTreeInternalPage leftSibling, BTreeInternalPage parent, BTreeEntry parentEntry) throws DbException, TransactionAbortedException {
        //将一些条目从左兄弟移动到页面，以便
        //条目是均匀分布的。一定要更新对应的父条目。一定要更新父级被移动的条目中所有子项的指针。
        if (page == null || leftSibling == null || parent == null || parentEntry == null) return;
        int currentCount = page.getNumEntries();
        int siblingCount = leftSibling.getNumEntries();
        if (currentCount + siblingCount < page.getNumEmptySlots() + leftSibling.getNumEmptySlots())
        {
            System.out.println("merge internalPage");
            return;
        }
        int changeCount = (siblingCount - currentCount) / 2;
        if (changeCount <= 0) return;

        //        page = (BTreeInternalPage) getPage(tid, dirtypages, page.getId(), Permissions.READ_WRITE);
        //        leftSibling = (BTreeInternalPage) getPage(tid, dirtypages, leftSibling.getId(), Permissions.READ_WRITE);
        //        parent = (BTreeInternalPage) getPage(tid, dirtypages, parent.getId(), Permissions.READ_WRITE);
        dirtypages.put(parent.getId(), parent);
        dirtypages.put(page.getId(), page);
        dirtypages.put(leftSibling.getId(), leftSibling);
        var pp = new BTreeEntry(parentEntry.getKey(), leftSibling.reverseIterator().next().getRightChild(), page.iterator().next().getLeftChild());
        page.insertEntry(pp);

        var it = leftSibling.reverseIterator();
        for (int i = 0; i < changeCount && it.hasNext(); i++)
        {
            var t = it.next();
            leftSibling.deleteKeyAndRightChild(t);
            if (i != changeCount - 1)
            {
                page.insertEntry(t);
            }
            else
            {
                parentEntry.setKey(t.getKey());
                parent.updateEntry(parentEntry);
            }
        }
        updateParentPointers(tid, dirtypages, leftSibling);
        updateParentPointers(tid, dirtypages, page);
        updateParentPointers(tid, dirtypages, parent);
        //        System.out.println(parentEntry.getKey());
        //        System.out.println(page.iterator().next().getKey());
        //        System.out.println(leftSibling.reverseIterator().next().getKey());

    }

    /**
     * Steal entries from the right sibling and copy them to the given page so that both pages are at least
     * half full. Keys can be thought of as rotating through the parent entry, so the original key in the
     * parent is "pulled down" to the left-hand page, and the last key in the right-hand page is "pushed up"
     * to the parent.  Update parent pointers as needed.
     *
     * @param tid          - the transaction id
     * @param dirtypages   - the list of dirty pages which should be updated with all new dirty pages
     * @param page         - the internal page which is less than half full
     * @param rightSibling - the right sibling which has entries to spare
     * @param parent       - the parent of the two internal pages
     * @param parentEntry  - the entry in the parent pointing to the two internal pages
     * @throws DbException
     * @throws TransactionAbortedException
     * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
     */
    public void stealFromRightInternalPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page, BTreeInternalPage rightSibling, BTreeInternalPage parent, BTreeEntry parentEntry) throws DbException, TransactionAbortedException {
        // some code goes here
        //        将一些条目从右兄弟移动到页面，以便
        //         条目是均匀分布的。一定要更新
        //         对应的父条目。一定要更新父级
        //         被移动的条目中所有子项的指针。

        if (page == null || rightSibling == null || parent == null || parentEntry == null) return;
        int currentCount = page.getNumEntries();
        int siblingCount = rightSibling.getNumEntries();
        if (currentCount + siblingCount < page.getNumEmptySlots() + rightSibling.getNumEmptySlots())
        {
            System.out.println("merge internalPage");
            return;
        }
        int changeCount = (siblingCount - currentCount) / 2;
        if (changeCount <= 0) return;

        dirtypages.put(parent.getId(), parent);
        dirtypages.put(page.getId(), page);
        dirtypages.put(rightSibling.getId(), rightSibling);
        var pp = new BTreeEntry(parentEntry.getKey(), page.reverseIterator().next().getRightChild(), rightSibling.iterator().next().getLeftChild());
        page.insertEntry(pp);

        var it = rightSibling.iterator();
        for (int i = 0; i < changeCount && it.hasNext(); i++)
        {
            var t = it.next();
            rightSibling.deleteKeyAndLeftChild(t);
            if (i != changeCount - 1)
            {
                page.insertEntry(t);
            }
            else
            {
                parentEntry.setKey(t.getKey());
                parent.updateEntry(parentEntry);
            }
        }
        updateParentPointers(tid, dirtypages, rightSibling);
        updateParentPointers(tid, dirtypages, page);
        updateParentPointers(tid, dirtypages, parent);

        //        System.out.println(parentEntry.getKey());
        //        System.out.println(page.iterator().next().getKey());
        //        System.out.println(leftSibling.reverseIterator().next().getKey());
    }

    /**
     * Merge two leaf pages by moving all tuples from the right page to the left page.
     * Delete the corresponding key and right child pointer from the parent, and recursively
     * handle the case when the parent gets below minimum occupancy.
     * Update sibling pointers as needed, and make the right page available for reuse.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param leftPage    - the left leaf page
     * @param rightPage   - the right leaf page
     * @param parent      - the parent of the two pages
     * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #deleteParentEntry(TransactionId, Map, BTreePage, BTreeInternalPage, BTreeEntry)
     */
    public void mergeLeafPages(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage leftPage, BTreeLeafPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry) throws DbException, IOException, TransactionAbortedException {


        //        将所有元组从右页移动到左页，更新
        //         兄弟指针，并使正确的页面可供重用。
        //         删除与正在合并的两个页面对应的父项中的条目 -
        //         deleteParentEntry() 在这里很有用

        var it = rightPage.iterator();
        while (it.hasNext())
        {
            var t = it.next();
                rightPage.deleteTuple(t);
                leftPage.insertTuple(t);
            }

        if (rightPage.getRightSiblingId() != null)
        {
            BTreeLeafPage rr = (BTreeLeafPage) getPage(tid, dirtypages, rightPage.getRightSiblingId(), Permissions.READ_WRITE);
            rr.setLeftSiblingId(leftPage.getId());
            leftPage.setRightSiblingId(rr.getId());
        }
        else
        {
            leftPage.setRightSiblingId(null);
        }
        dirtypages.put(rightPage.getId(), rightPage);
        setEmptyPage(tid, dirtypages, rightPage.getId().getPageNumber());

        dirtypages.put(parent.getId(), parent);
            updateParentPointer(tid, dirtypages, parent.getId(), leftPage.getId());
        deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);

    }

    /**
     * Merge two internal pages by moving all entries from the right page to the left page
     * and "pulling down" the corresponding key from the parent entry.
     * Delete the corresponding key and right child pointer from the parent, and recursively
     * handle the case when the parent gets below minimum occupancy.
     * Update parent pointers as needed, and make the right page available for reuse.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param leftPage    - the left internal page
     * @param rightPage   - the right internal page
     * @param parent      - the parent of the two pages
     * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #deleteParentEntry(TransactionId, Map, BTreePage, BTreeInternalPage, BTreeEntry)
     * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
     */
    public void mergeInternalPages(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage leftPage, BTreeInternalPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry) throws DbException, IOException, TransactionAbortedException {

        //        将所有条目从右页移动到左页，更新
        //         被移动的条目中子项的父指针，
        //         并使正确的页面可供重用
        //         删除与正在合并的两个页面对应的父项中的条目 -
        //         deleteParentEntry() 在这里很有用

        dirtypages.put(rightPage.getId(), rightPage);
        dirtypages.put(leftPage.getId(), leftPage);
        dirtypages.put(parent.getId(), parent);
        var pp = new BTreeEntry(parentEntry.getKey(), leftPage.reverseIterator().next().getRightChild(), rightPage.iterator().next().getLeftChild());
        leftPage.insertEntry(pp);

        var it = rightPage.iterator();
        ArrayList<BTreeEntry> listsInfo = new ArrayList<>();
        while (it.hasNext())
        {
            var t = it.next();
                rightPage.deleteKeyAndLeftChild(t);
                leftPage.insertEntry(t);
            }
        updateParentPointers(tid, dirtypages, leftPage);
        setEmptyPage(tid, dirtypages, rightPage.getId().getPageNumber());
        deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);
            updateParentPointers(tid, dirtypages, parent);
        //        if (leftPage.getParentId().equals(parent.getId()))
        //        {
        //            updateParentPointers(tid, dirtypages, parent);
        //            updateParentPointer(tid, dirtypages, parent.getId(), leftPage.getId());
        //        }
        //        else
        //        {
        //            dirtypages.put(parent.getId(), parent);
        //            setEmptyPage(tid, dirtypages, parent.getId().getPageNumber());
        //        }

    }

    /**
     * Method to encapsulate the process of deleting an entry (specifically the key and right child)
     * from a parent node.  If the parent becomes empty (no keys remaining), that indicates that it
     * was the root node and should be replaced by its one remaining child.  Otherwise, if it gets
     * below minimum occupancy for non-root internal nodes, it should steal from one of its siblings or
     * merge with a sibling.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param leftPage    - the child remaining after the key and right child are deleted
     * @param parent      - the parent containing the entry to be deleted
     * @param parentEntry - the entry to be deleted
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #handleMinOccupancyPage(TransactionId, Map, BTreePage)
     */
    private void deleteParentEntry(TransactionId tid, Map<PageId, Page> dirtypages, BTreePage leftPage, BTreeInternalPage parent, BTreeEntry parentEntry) throws DbException, IOException, TransactionAbortedException {

        // delete the entry in the parent.  If
        // the parent is below minimum occupancy, get some tuples from its siblings
        // or merge with one of the siblings
        parent.deleteKeyAndRightChild(parentEntry);
        int maxEmptySlots = parent.getMaxEntries() - parent.getMaxEntries() / 2; // ceiling
        if (parent.getNumEmptySlots() == parent.getMaxEntries())
        {
            // 这是父项中的最后一个条目。             在这种情况下，应该删除父节点（根节点），并合并             页面将成为新的根
            BTreePageId rootPtrId = parent.getParentId();
            if (rootPtrId.pgcateg() != BTreePageId.ROOT_PTR)
            {
                throw new DbException("attempting to delete a non-root node");
            }
            BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, rootPtrId, Permissions.READ_WRITE);
            leftPage.setParentId(rootPtrId);
            rootPtr.setRootId(leftPage.getId());

            // 释放父页面以供重用
            setEmptyPage(tid, dirtypages, parent.getId().getPageNumber());
        }
        else if (parent.getNumEmptySlots() > maxEmptySlots)
        {
            handleMinOccupancyPage(tid, dirtypages, parent);
        }
    }

    /**
     * Delete a tuple from this BTreeFile.
     * May cause pages to merge or redistribute entries/tuples if the pages
     * become less than half full.
     *
     * @param tid - the transaction id
     * @param t   - the tuple to delete
     * @return a list of all pages that were dirtied by this operation. Could include
     * many pages since parent pointers will need to be updated when an internal node merges.
     * @see #handleMinOccupancyPage(TransactionId, Map, BTreePage)
     */
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        Map<PageId, Page> dirtypages = new HashMap<>();

        BTreePageId pageId = new BTreePageId(tableid, t.getRecordId().getPageId().getPageNumber(), BTreePageId.LEAF);
        BTreeLeafPage page = (BTreeLeafPage) getPage(tid, dirtypages, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);

        // 如果页面低于最低占用率，则从其兄弟姐妹那里获取一些元组 或与其中一个兄弟姐妹合并
        int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples() / 2; // ceiling
        if (page.getNumEmptySlots() > maxEmptySlots)
        {
            handleMinOccupancyPage(tid, dirtypages, page);
        }

        return new ArrayList<>(dirtypages.values());
    }

    /**
     * Get a read lock on the root pointer page. Create the root pointer page and root page
     * if necessary.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @return the root pointer page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    BTreeRootPtrPage getRootPtrPage(TransactionId tid, Map<PageId, Page> dirtypages) throws DbException, IOException, TransactionAbortedException {
        synchronized (this)
        {
            if (f.length() == 0)
            {
                // 创建根指针页和根页
                BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));
                byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
                byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
                bw.write(emptyRootPtrData);
                bw.write(emptyLeafData);
                bw.close();
            }
        }

        // 在根指针页面上获得读锁
        return (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_ONLY);
    }

    /**
     * Get the page number of the first empty page in this BTreeFile.
     * Creates a new page if none of the existing pages are empty.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @return the page number of the first empty page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    public int getEmptyPageNo(TransactionId tid, Map<PageId, Page> dirtypages) throws DbException, IOException, TransactionAbortedException {
        // 在根指针页上获得读锁并使用它来定位第一个标题页
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId headerId = rootPtr.getHeaderId();
        int emptyPageNo = 0;

        if (headerId != null)
        {
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
            int headerPageCount = 0;
            // 尝试找到一个空槽的标题页
            while (headerPage != null && headerPage.getEmptySlot() == -1)
            {
                headerId = headerPage.getNextPageId();
                if (headerId != null)
                {
                    headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
                    headerPageCount++;
                }
                else
                {
                    headerPage = null;
                }
            }

            // 如果 headerPage 不为空，它必须有一个空槽
            if (headerPage != null)
            {
                headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
                int emptySlot = headerPage.getEmptySlot();
                headerPage.markSlotUsed(emptySlot, true);
                emptyPageNo = headerPageCount * BTreeHeaderPage.getNumSlots() + emptySlot;
            }
        }

        // 此时如果 headerId 为 null，则要么没有标题页，要么没有空闲插槽
        if (headerId == null)
        {
            synchronized (this)
            {
                // create the new page
                BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));
                byte[] emptyData = BTreeInternalPage.createEmptyPageData();
                bw.write(emptyData);
                bw.close();
                emptyPageNo = numPages();
            }
        }

        return emptyPageNo;
    }

    /**
     * Method to encapsulate the process of creating a new page.  It reuses old pages if possible,
     * and creates a new page if none are available.  It wipes the page on disk and in the cache and
     * returns a clean copy locked with read-write permission
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pgcateg    - the BTreePageId category of the new page.  Either LEAF, INTERNAL, or HEADER
     * @return the new empty page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getEmptyPageNo(TransactionId, Map)
     * @see #setEmptyPage(TransactionId, Map, int)
     */
    private Page getEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int pgcateg) throws DbException, IOException, TransactionAbortedException {
        // create the new page
        int emptyPageNo = getEmptyPageNo(tid, dirtypages);
        BTreePageId newPageId = new BTreePageId(tableid, emptyPageNo, pgcateg);

        // write empty page to disk
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(BTreeRootPtrPage.getPageSize() + (long) (emptyPageNo - 1) * BufferPool.getPageSize());
        rf.write(BTreePage.createEmptyPageData());
        rf.close();

        // make sure the page is not in the buffer pool	or in the local cache
        Database.getBufferPool().discardPage(newPageId);
        dirtypages.remove(newPageId);

        return getPage(tid, dirtypages, newPageId, Permissions.READ_WRITE);
    }

    /**
     * Mark a page in this BTreeFile as empty. Find the corresponding header page
     * (create it if needed), and mark the corresponding slot in the header page as empty.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param emptyPageNo - the page number of the empty page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getEmptyPage(TransactionId, Map, int)
     */
    public void setEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int emptyPageNo) throws DbException, IOException, TransactionAbortedException {

        //如果这是文件中的最后一页（而不是唯一一页），只需  截断文件         @TODO：注释掉是因为我们可能应该在其他地方这样做，以防事务中止......
        //		synchronized(this) {
        //			if(emptyPageNo == numPages()) {
        //				if(emptyPageNo <= 1) {
        //					// if this is the only page in the file, just return.
        //					// It just means we have an empty root page
        //					return;
        //				}
        //				long newSize = f.length() - BufferPool.getPageSize();
        //				FileOutputStream fos = new FileOutputStream(f, true);
        //				FileChannel fc = fos.getChannel();
        //				fc.truncate(newSize);
        //				fc.close();
        //				fos.close();
        //				return;
        //			}
        //		}


        // 否则，在根指针页面上获得一个读锁并使用它来定位第一个标题页
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId headerId = rootPtr.getHeaderId();
        BTreePageId prevId = null;
        int headerPageCount = 0;

        // 如果没有标题页，则创建第一个标题页并更新 BTreeRootPtrPage 中的头指针
        if (headerId == null)
        {
            rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);

            BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
            headerId = headerPage.getId();
            headerPage.init();
            rootPtr.setHeaderId(headerId);
        }

        // 遍历所有现有的标题页面以找到包含与 emptyPageNo 对应的插槽的页面
        while (headerId != null && (headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo)
        {
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
            prevId = headerId;
            headerId = headerPage.getNextPageId();
            headerPageCount++;
        }

        // 此时 headerId 应该为 null 或设置为包含对应于 emptyPageNo 的槽的 headerPage。添加标题页，直到我们有一个对应于 emptyPageNo 的插槽
        while ((headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo)
        {
            BTreeHeaderPage prevPage = (BTreeHeaderPage) getPage(tid, dirtypages, prevId, Permissions.READ_WRITE);

            BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
            headerId = headerPage.getId();
            headerPage.init();
            headerPage.setPrevPageId(prevId);
            prevPage.setNextPageId(headerId);

            headerPageCount++;
            prevId = headerId;
        }

        // 现在 headerId 应该设置为 headerPage 包含对应于 emptyPageNo 的插槽
        BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
        int emptySlot = emptyPageNo - headerPageCount * BTreeHeaderPage.getNumSlots();
        headerPage.markSlotUsed(emptySlot, false);
    }

    /**
     * get the specified tuples from the file based on its IndexPredicate value on
     * behalf of the specified transaction. This method will acquire a read lock on
     * the affected pages of the file, and may block until the lock can be
     * acquired.
     *
     * @param tid   - the transaction id
     * @param ipred - the index predicate value to filter on
     * @return an iterator for the filtered tuples
     */
    public DbFileIterator indexIterator(TransactionId tid, IndexPredicate ipred) {
        return new BTreeSearchIterator(this, tid, ipred);
    }

    /**
     * Get an iterator for all tuples in this B+ tree file in sorted order. This method
     * will acquire a read lock on the affected pages of the file, and may block until
     * the lock can be acquired.
     *
     * @param tid - the transaction id
     * @return an iterator for all the tuples in this file
     */
    public DbFileIterator iterator(TransactionId tid) {
        return new BTreeFileIterator(this, tid);
    }

}

/**
 * Helper class that implements the Java Iterator for tuples on a BTreeFile
 */
class BTreeFileIterator extends AbstractDbFileIterator {

    final TransactionId tid;
    final BTreeFile f;
    Iterator<Tuple> it = null;
    BTreeLeafPage curp = null;

    /**
     * Constructor for this iterator
     *
     * @param f   - the BTreeFile containing the tuples
     * @param tid - the transaction id
     */
    public BTreeFileIterator(BTreeFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
    }

    /**
     * Open this iterator by getting an iterator on the first leaf page
     */
    public void open() throws DbException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
        BTreePageId root = rootPtr.getRootId();
        curp = f.findLeafPage(tid, root, null);
        it = curp.iterator();
    }

    /**
     * Read the next tuple either from the current page if it has more tuples or
     * from the next page by following the right sibling pointer.
     *
     * @return the next tuple, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext()) it = null;

        while (it == null && curp != null)
        {
            BTreePageId nextp = curp.getRightSiblingId();
            if (nextp == null)
            {
                curp = null;
            }
            else
            {
                curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid, nextp, Permissions.READ_ONLY);
                it = curp.iterator();
                if (!it.hasNext()) it = null;
            }
        }

        if (it == null) return null;
        return it.next();
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * close the iterator
     */
    public void close() {
        super.close();
        it = null;
        curp = null;
    }
}

/**
 * Helper class that implements the DbFileIterator for search tuples on a
 * B+ Tree File
 */
class BTreeSearchIterator extends AbstractDbFileIterator {

    final TransactionId tid;
    final BTreeFile f;
    final IndexPredicate ipred;
    Iterator<Tuple> it = null;
    BTreeLeafPage curp = null;

    /**
     * Constructor for this iterator
     *
     * @param f     - the BTreeFile containing the tuples
     * @param tid   - the transaction id
     * @param ipred - the predicate to filter on
     */
    public BTreeSearchIterator(BTreeFile f, TransactionId tid, IndexPredicate ipred) {
        this.f = f;
        this.tid = tid;
        this.ipred = ipred;
    }

    /**
     * Open this iterator by getting an iterator on the first leaf page applicable
     * for the given predicate operation
     */
    public void open() throws DbException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
        BTreePageId root = rootPtr.getRootId();
        if (ipred.getOp() == Op.EQUALS || ipred.getOp() == Op.GREATER_THAN || ipred.getOp() == Op.GREATER_THAN_OR_EQ)
        {
            curp = f.findLeafPage(tid, root, ipred.getField());
        }
        else
        {
            curp = f.findLeafPage(tid, root, null);
        }
        it = curp.iterator();
    }

    /**
     * Read the next tuple either from the current page if it has more tuples matching
     * the predicate or from the next page by following the right sibling pointer.
     *
     * @return the next tuple matching the predicate, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException, NoSuchElementException {
        while (it != null)
        {

            while (it.hasNext())
            {
                Tuple t = it.next();
                if (t.getField(f.keyField()).compare(ipred.getOp(), ipred.getField()))
                {
                    return t;
                }
                else if (ipred.getOp() == Op.LESS_THAN || ipred.getOp() == Op.LESS_THAN_OR_EQ)
                {
                    // if the predicate was not satisfied and the operation is less than, we have
                    // hit the end
                    return null;
                }
                else if (ipred.getOp() == Op.EQUALS && t.getField(f.keyField()).compare(Op.GREATER_THAN, ipred.getField()))
                {
                    // if the tuple is now greater than the field passed in and the operation
                    // is equals, we have reached the end
                    return null;
                }
            }

            BTreePageId nextp = curp.getRightSiblingId();
            // if there are no more pages to the right, end the iteration
            if (nextp == null)
            {
                return null;
            }
            else
            {
                curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid, nextp, Permissions.READ_ONLY);
                it = curp.iterator();
            }
        }

        return null;
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * close the iterator
     */
    public void close() {
        super.close();
        it = null;
    }
}
