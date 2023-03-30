package simpledb.storage;

import java.io.DataOutputStream;
import java.io.IOException;
import simpledb.common.Type;
import simpledb.execution.Predicate;

/**
 * Instance of Field that stores a single integer.
 */
public class DoubleField implements Field {

    private static final long serialVersionUID = 1L;

    private final double value;

    public double getValue() {
        return value;
    }

    /**
     * Constructor.
     *
     * @param i The value of this field.
     */
    public DoubleField(double i) {
        value = i;
    }

    public String toString() {
        return Double.toString(value);
    }

    public int hashCode() {
        return Double.hashCode(value);
    }

    public boolean equals(Object field) {
        if (!(field instanceof DoubleField)) return false;
        return Double.compare(((DoubleField) field).value ,value)==0;
    }

    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeDouble(value);
    }

    /**
     * Compare the specified field to the value of this Field.
     * Return semantics are as specified by Field.compare
     *
     * @throws IllegalCastException if val is not an IntField
     * @see Field#compare
     */
    public boolean compare(Predicate.Op op, Field val) {

        DoubleField iVal = (DoubleField) val;

        switch (op) {
            case EQUALS:
            case LIKE:
                return  Double.compare(value, iVal.value)==0;
            case NOT_EQUALS:
                return Double.compare(value, iVal.value)!=0;
            case GREATER_THAN:
                return Double.compare(value, iVal.value)>0;
            case GREATER_THAN_OR_EQ:
                return Double.compare(value, iVal.value)>=0;
            case LESS_THAN:
                return Double.compare(value, iVal.value)<0;
            case LESS_THAN_OR_EQ:
                return Double.compare(value, iVal.value)<=0;
        }

        return false;
    }

    /**
     * Return the Type of this field.
     *
     * @return Type.INT_TYPE
     */
    public Type getType() {
        return Type.DOUBLE_TYPE;
    }
}
