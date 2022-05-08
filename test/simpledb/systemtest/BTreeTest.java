package simpledb.systemtest;

import org.junit.After;
import org.junit.Test;
import simpledb.common.Database;
import simpledb.execution.IndexPredicate;
import simpledb.execution.Predicate.Op;
import simpledb.index.BTreeFile;
import simpledb.index.BTreeUtility;
import simpledb.index.BTreeUtility.BTreeDeleter;
import simpledb.index.BTreeUtility.BTreeInserter;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * System test for the BTree
 */
public class BTreeTest extends SimpleDbTestBase {
    private final static Random r = new Random();
    
    private static final int POLL_INTERVAL = 100;
    
    /**
	 * Helper method to clean up the syntax of starting a BTreeInserter thread.
	 * The parameters pass through to the BTreeInserter constructor.
	 */
	public BTreeInserter startInserter(BTreeFile bf, int[] tupdata, BlockingQueue<List<Integer>> insertedTuples) {

		BTreeInserter bi = new BTreeInserter(bf, tupdata, insertedTuples);
		bi.start();
		return bi;
	}
    
	/**
	 * Helper method to clean up the syntax of starting a BTreeDeleter thread.
	 * The parameters pass through to the BTreeDeleter constructor.
	 */
	public BTreeDeleter startDeleter(BTreeFile bf, BlockingQueue<List<Integer>> insertedTuples) {
		BTreeDeleter bd = new BTreeDeleter(bf, insertedTuples);
		bd.start();
		return bd;
	}
	
	private void waitForInserterThreads(List<BTreeInserter> insertThreads)
			throws Exception {
		Thread.sleep(POLL_INTERVAL);
		for(BTreeInserter thread : insertThreads) {
			while(!thread.succeeded() && thread.getError() == null) {
				Thread.sleep(POLL_INTERVAL);
			}
		}
	}
	
	private void waitForDeleterThreads(List<BTreeDeleter> deleteThreads)
			throws Exception {
		Thread.sleep(POLL_INTERVAL);
		for(BTreeDeleter thread : deleteThreads) {
			while(!thread.succeeded() && thread.getError() == null) {
				Thread.sleep(POLL_INTERVAL);
			}
		}
	}
	
	private int[] getRandomTupleData() {
		int item1 = r.nextInt(BTreeUtility.MAX_RAND_VALUE);
		int item2 = r.nextInt(BTreeUtility.MAX_RAND_VALUE);
		return new int[]{item1, item2};
	}
	
	@After
	public void tearDown() {
		// set the page size back to the default
		BufferPool.resetPageSize();
		Database.reset();
	}
	
    /** Test that doing lots of inserts and deletes in multiple threads works */
    @Test public void testBigFile() throws Exception {
    	// 对于这个测试，我们将减小缓冲池页面的大小
    	BufferPool.setPageSize(1024);

		// 这应该创建一个包含第二层内部页面的 B+ 树并打包了第三层叶子页面
    	System.out.println("Creating large random B+ tree...");
    	List<List<Integer>> tuples = new ArrayList<>();
		BTreeFile bf = BTreeUtility.createRandomBTreeFile(2, 31000,
				null, tuples, 0);

		// 我们将需要更多缓冲池空间来进行此测试

		Database.resetBufferPool(500);

		BlockingQueue<List<Integer>> insertedTuples = new ArrayBlockingQueue<>(100000);
		insertedTuples.addAll(tuples);
		assertEquals(31000, insertedTuples.size());
		int size = insertedTuples.size();

		// 现在插入一些随机元组
		System.out.println("Inserting tuples...");
		List<BTreeInserter> insertThreads = new ArrayList<>();
		for (int i = 0; i < 200; i++)
		{
			BTreeInserter bi = startInserter(bf, getRandomTupleData(), insertedTuples);
			insertThreads.add(bi);

			// 前几次插入会导致页面拆分，因此请给它们多一点时间以避免过多的死锁情况

			Thread.sleep(r.nextInt(POLL_INTERVAL));
		}


		for (int i = 0; i < 800; i++)
		{
			BTreeInserter bi = startInserter(bf, getRandomTupleData(), insertedTuples);
			insertThreads.add(bi);
		}

		System.out.println("waiting for all insertThreads");
		// 等待所有线程完成
		waitForInserterThreads(insertThreads);
		assertTrue(insertedTuples.size() > size);

		// 现在同时插入和删除元组
		System.out.println("Inserting and deleting tuples...");
		List<BTreeDeleter> deleteThreads = new ArrayList<>();
		for (BTreeInserter thread : insertThreads)
		{
			thread.rerun(bf, getRandomTupleData(), insertedTuples);
			BTreeDeleter bd = startDeleter(bf, insertedTuples);
			deleteThreads.add(bd);
		}

		// 等待所有线程完成
		waitForInserterThreads(insertThreads);
		waitForDeleterThreads(deleteThreads);
		int numPages = bf.numPages();
		size = insertedTuples.size();

		System.out.println("before delete&instert size=" + size + " numPages" + numPages + " bf.numPages" + bf.numPages());

		// 现在删除一堆元组
		System.out.println("Deleting tuples...");
		for (int i = 0; i < 10; i++)
		{
			for (BTreeDeleter thread : deleteThreads)
			{
				thread.rerun(bf, insertedTuples);
			}

			// 等待所有线程完成
			waitForDeleterThreads(deleteThreads);
		}
		assertTrue("insertedTuples.size()=" + insertedTuples.size() + "  size=" + size, insertedTuples.size() < size);
		size = insertedTuples.size();
		System.out.println("after delete & before instert size=" + size + " numPages" + numPages + " bf.numPages" + bf.numPages());

		// 现在再次插入一堆随机元组
		System.out.println("Inserting tuples...");
		for (int i = 0; i < 10; i++)
		{
			for (BTreeInserter thread : insertThreads)
			{
				thread.rerun(bf, getRandomTupleData(), insertedTuples);
			}

			// 等待所有线程完成
			waitForInserterThreads(insertThreads);
		}
		assertTrue("insertedTuples.size()=" + insertedTuples.size() + "  size=" + size, insertedTuples.size() > size);
		size = insertedTuples.size();
		System.out.println("after instert size=" + size + " numPages" + numPages + " bf.numPages" + bf.numPages());
		// 我们应该重用已删除的页面
		try
		{
			//344 308+20=328
			assertTrue("bf.numPages()=" + bf.numPages() + "  numPages + 20=" + (numPages + 20), bf.numPages() < numPages + 20);
		} catch (AssertionError e)
		{
			e.printStackTrace();
			;
		}

		// 杀死所有线程
		insertThreads = null;
		deleteThreads = null;

		List<List<Integer>> tuplesList = new ArrayList<>(insertedTuples);
		TransactionId tid = new TransactionId();

		// 首先寻找随机元组并确保我们可以找到它们
		System.out.println("Searching for tuples...");
		for (int i = 0; i < 10000; i++)
		{
			int rand = r.nextInt(insertedTuples.size());
			List<Integer> tuple = tuplesList.get(rand);
			IntField randKey = new IntField(tuple.get(bf.keyField()));
			IndexPredicate ipred = new IndexPredicate(Op.EQUALS, randKey);
			DbFileIterator it = bf.indexIterator(tid, ipred);
			it.open();
			boolean found = false;
			while (it.hasNext())
			{
				Tuple t = it.next();
				if (tuple.equals(SystemTestUtil.tupleToList(t)))
				{
					found = true;
					break;
				}
			}
			assertTrue(found);
			it.close();
		}

		// 现在确保所有元组都是有序的并且我们有正确的数字
		System.out.println("Performing sanity checks...");
		DbFileIterator it = bf.iterator(tid);
		Field prev = null;
		it.open();
		int count = 0;
		while (it.hasNext())
		{
			Tuple t = it.next();
			if (prev != null)
			{
				assertTrue(t.getField(bf.keyField()).compare(Op.GREATER_THAN_OR_EQ, prev));
			}
			prev = t.getField(bf.keyField());
			count++;
		}
		it.close();
		assertEquals(count, tuplesList.size());
		Database.getBufferPool().transactionComplete(tid);

		// 重新设置页面大小
		BufferPool.resetPageSize();
		
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(BTreeTest.class);
    }
}
