package edu.caltech.nanodb.expressions;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This class allows us to sort and compare tuples based on an order-by
 * specification.  The specification is simply a list of
 * {@link OrderByExpression} objects, and the order of the expressions
 * themselves matters.  Tuples will be ordered by the first expression; if the
 * tuples' values are the same then the tuples will be ordered by the second
 * expression; etc.
 */
public class TupleComparator implements Comparator<Tuple> {

    /**
     * The schema of the tuples that will be compared by this comparator object.
     */
    private Schema schema;


    /** The specification of how to order the tuples being compared. */
    private ArrayList<OrderByExpression> orderSpec;


    /**
     * The environment to use for evaluating order-by expressions against the
     * first tuple.
     */
    private Environment envTupleA = new Environment();


    /**
     * The environment to use for evaluating order-by expressions against the
     * second tuple.
     */
    private Environment envTupleB = new Environment();


    /**
     * Construct a new tuple-comparator with the given ordering specification.
     *
     * @param schema the schema of the tuples that will be compared by this
     *        comparator object
     *
     * @param orderSpec a series of order-by expressions used to order the
     *        tuples being compared
     */
    public TupleComparator(Schema schema, List<OrderByExpression> orderSpec) {
        if (schema == null)
            throw new IllegalArgumentException("schema cannot be null");

        if (orderSpec == null)
            throw new IllegalArgumentException("orderSpec cannot be null");

        this.schema = schema;
        this.orderSpec = new ArrayList<OrderByExpression>(orderSpec);
    }


    /**
     * Performs the comparison of two tuples based on the configuration of this
     * tuple-comparator object.
     *
     * @design (Donnie) We have to suppress "unchecked operation" warnings on
     *         this code, since {@link Comparable} is a generic (and thus allows
     *         us to specify the type of object being compared), but we want to
     *         use it without specifying any types.
     *
     * @param a the first tuple to compare.
     * @param b the second tuple to compare.
     * @return a negative, zero, or positive value, corresponding to whether
     *         tuple <tt>a</tt> is less than, equal to, or greater than tuple
     *         <tt>b</tt>.
     */
    @Override
    @SuppressWarnings("unchecked")
    public int compare(Tuple a, Tuple b) {

        // Set up the environments for evaluating the order-by specifications.

        envTupleA.clear();
        envTupleA.addTuple(schema, a);

        envTupleB.clear();
        envTupleB.addTuple(schema, b);

        int compareResult = 0;

        // For each order-by spec, evaluate the expression against both tuples,
        // and compare the results.
        for (OrderByExpression entry : orderSpec) {
            Expression expr = entry.getExpression();

            Comparable valueA = (Comparable) expr.evaluate(envTupleA);
            Comparable valueB = (Comparable) expr.evaluate(envTupleB);

            // Although it should be "unknown" when we compare two NULL values
            // for equality, we say they are equal so that they will all appear
            // together in the sorting results.
            if (valueA == null) {
                if (valueB != null)
                    compareResult = -1;
                else
                    compareResult = 0;
            }
            else if (valueB == null) {
                compareResult = 1;
            }
            else {
                compareResult = valueA.compareTo(valueB);
            }

            if (compareResult != 0) {
                if (!entry.isAscending())
                    compareResult = -compareResult;

                break;
            }
        }

        return compareResult;
    }


    /**
     * This helper function returns true if two tuples have the same number of
     * columns and the values compare as equal when coerced with the
     * {@link TypeConverter#coerceComparison} method.  Note that the schemas of
     * the tuples are not considered.
     *
     * @param t1 the first tuple to compare
     * @param t2 the second tuple to compare
     * @return true if the two tuples have the same number of columns, and the
     *         values from <tt>t1</tt> and <tt>t2</tt> compare equal.
     */
    public static boolean areTuplesEqual(Tuple t1, Tuple t2) {
        if (t1 == null)
            throw new IllegalArgumentException("t1 cannot be null");

        if (t2 == null)
            throw new IllegalArgumentException("t2 cannot be null");
        
        if (t1.getColumnCount() != t2.getColumnCount())
            return false;
        
        int size = t1.getColumnCount();
        for (int i = 0; i < size; i++) {
            Object obj1 = t1.getColumnValue(i);
            Object obj2 = t2.getColumnValue(i);

            if (obj1 == null) {
                if (obj2 != null) {
                    // obj1 is null, but obj2 isn't.
                    return false;
                }

                // If we got here, both obj1 and obj2 are null.
            }
            else if (obj2 == null) {
                // obj1 isn't null, but obj2 is.
                return false;
            }
            else {
                // Both objects are non-null.
                TypeConverter.Pair p =
                    TypeConverter.coerceComparison(obj1, obj2);
                if (!p.value1.equals(p.value2))
                    return false;
            }
        }

        return true;
    }
}
