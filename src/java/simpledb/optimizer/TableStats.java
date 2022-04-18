package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    static final int IOCOSTPERPAGE = 1000;
    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();
    private final int ioCostPerPage;
    private final DbFile file;
    private ArrayList<Histogram> listsInfo;
    private int pageNumber;
    private int tupleNumber;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tupleNumber = 0;
        this.ioCostPerPage = ioCostPerPage;
        DbFile file = Database.getCatalog().getDatabaseFile(tableid);
        this.file = file;
        this.pageNumber = file.numPages();
        TupleDesc td = file.getTupleDesc();
        int numbers = td.numFields();
        Map<Integer, Field> min = new HashMap<>();
        Map<Integer, Field> max = new HashMap<>();
        var it = file.iterator(new TransactionId());
        try
        {
            it.open();
            while (it.hasNext())
            {
                var t = it.next();
                tupleNumber++;
                for (Integer i = 0; i < numbers; i++)
                {
                    switch (t.getField(i).getType())
                    {
                        case INT_TYPE -> {
                            IntField f = (IntField) (t.getField(i));
                            var minValue = ((IntField) (min.get(i))) == null ? Integer.MAX_VALUE : ((IntField) (min.get(i))).getValue();
                            var maxValue = ((IntField) (max.get(i))) == null ? Integer.MIN_VALUE : ((IntField) (max.get(i))).getValue();
                            if (minValue > f.getValue()) min.put(i, f);
                            if (maxValue < f.getValue()) max.put(i, f);
                        }
                        case STRING_TYPE -> {
                            StringField f = (StringField) (t.getField(i));
                            var minValue = ((StringField) (min.get(i))) == null ? String.valueOf(Integer.MAX_VALUE) : ((StringField) (min.get(i))).getValue();
                            var maxValue = ((StringField) (max.get(i))) == null ? String.valueOf(Integer.MIN_VALUE) : ((StringField) (min.get(i))).getValue();
                            if (minValue.compareToIgnoreCase(f.getValue()) < 0) min.put(i, f);
                            if (maxValue.compareToIgnoreCase(f.getValue()) < 0) max.put(i, f);
                        }
                    }
                }
            }
            listsInfo = new ArrayList<>(numbers);
            for (int i = 0; i < numbers; i++)
            {
                listsInfo.add(null);
            }
            for (int i = 0; i < numbers; i++)
            {
                switch (td.getFieldType(i))
                {
                    case INT_TYPE -> {
                        var minValue = ((IntField) (min.get(i))) == null ? Integer.MAX_VALUE : ((IntField) (min.get(i))).getValue();
                        var maxValue = ((IntField) (max.get(i))) == null ? Integer.MIN_VALUE : ((IntField) (max.get(i))).getValue();
                        listsInfo.set(i, new IntHistogram(10, minValue, maxValue));
                    }
                    case STRING_TYPE -> {
                        var minValue = ((StringField) (min.get(i))) == null ? String.valueOf(Integer.MAX_VALUE) : ((StringField) (min.get(i))).getValue();
                        var maxValue = ((StringField) (max.get(i))) == null ? String.valueOf(Integer.MIN_VALUE) : ((StringField) (min.get(i))).getValue();
                        listsInfo.set(i, new StringHistogram(10, minValue, maxValue));
                    }
                }
            }
            it.rewind();
            while (it.hasNext())
            {
                Tuple t = it.next();
                for (int i = 0; i < numbers; i++)
                {
                    switch (t.getField(i).getType())
                    {
                        case INT_TYPE -> {
                            IntField f = (IntField) (t.getField(i));
                            listsInfo.get(i).addValue(f.getValue());
                        }
                        case STRING_TYPE -> {
                            StringField f = (StringField) (t.getField(i));
                            listsInfo.get(i).addValue(f.getValue());
                        }
                    }
                }
            }
            it.close();
        } catch (TransactionAbortedException | DbException ex)
        {
            ex.printStackTrace();
        }
    }


    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try
    {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e)
        {
            e.printStackTrace();
        }

    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext())
        {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return ioCostPerPage * pageNumber;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * totalTuples());
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        //        return listsInfo.get(field).avgSelectivity();
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        switch (file.getTupleDesc().getFieldType(field))
        {
            case INT_TYPE -> {
                return listsInfo.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
            }
            case STRING_TYPE -> {
                return listsInfo.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
            }
        }
        return -1;

    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return tupleNumber;
    }

}
