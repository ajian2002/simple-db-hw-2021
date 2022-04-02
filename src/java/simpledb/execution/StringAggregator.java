package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield;
    private Op what;
    private Type gbfieldtype;
    private List<Tuple> all = new ArrayList<>();
    private Map<Field, List<Tuple>> groups = new HashMap<>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) throw new IllegalArgumentException("not is Count");
        this.afield = afield;
        this.gbfield = gbfield;
        this.what = what;
        this.gbfieldtype = gbfieldtype;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //        count.put(tup.getField(afield), count.get(tup.getField(afield)) == null ? 1 : count.get(tup.getField(afield)) + 1);
        //        count++;
        all.add(tup);
        if (gbfield != NO_GROUPING)
        {
            var key = tup.getField(gbfield);
            var value = groups.get(key);
            if (value == null) value = new ArrayList<>();
            value.add(tup);
            groups.put(key, value);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc td;
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbfield != NO_GROUPING)
        {
            //分组
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            groups.forEach((k, v) -> {
                var t = new Tuple(td);
                t.setField(0, k);
                t.setField(1, new IntField(getAggregateResult(v, what)));
                tuples.add(t);
            });
        }
        else
        {
            //不分组
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            var t = new Tuple(td);
            t.setField(0, new IntField(getAggregateResult(all, what)));
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }
    private int getAggregateResult(List<Tuple> lists, Aggregator.Op op) {

        switch (op)
        {
            case COUNT -> {
                return lists.size();
            }
            case SUM -> {
                return lists.stream().mapToInt(k -> Integer.parseInt(k.getField(afield).toString())).sum();
            }
            case AVG -> {
                return lists.stream().mapToInt(k -> Integer.parseInt(k.getField(afield).toString())).sum() / lists.size();
            }
            case MIN -> {
                return lists.stream().mapToInt(k -> Integer.parseInt(k.getField(afield).toString())).min().getAsInt();
            }
            case MAX -> {
                return lists.stream().mapToInt(k -> Integer.parseInt(k.getField(afield).toString())).max().getAsInt();
            }
            default -> {
                return -1;
            }
        }
    }

}
