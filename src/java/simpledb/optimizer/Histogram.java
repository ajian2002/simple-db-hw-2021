package simpledb.optimizer;

import simpledb.execution.Predicate;

public interface Histogram<T> {
    /**
     * Add a new value to thte histogram
     */
    void addValue(T s);

    /**
     * Estimate the selectivity (as a double between 0 and 1) of the specified
     * predicate over the specified string
     *
     * @param op The operation being applied
     * @param v  添加到直方图中的值
     */
    double estimateSelectivity(Predicate.Op op, T v);

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic join
     * optimization. It may be needed if you want to implement a more
     * efficient optimization
     */
    double avgSelectivity();
}
