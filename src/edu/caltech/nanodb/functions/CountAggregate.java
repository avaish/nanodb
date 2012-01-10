package edu.caltech.nanodb.functions;

import java.util.HashSet;


/**
 * This aggregate function can be used to compute both <tt>COUNT(...)</tt> and
 * <tt>COUNT(DISTINCT ...)</tt> aggregate functions.  In addition, the
 * <tt>COUNT(DISTINCT ...)</tt> operation can consume either sorted or unsorted
 * values to compute the distinct count.
 */
public class CountAggregate extends AggregateFunction {
    private int count;

    private HashSet<Object> valuesSeen = new HashSet<Object>();

    private Object lastValueSeen;

    private boolean distinct;
    private boolean sortedInputs;


    public CountAggregate(boolean distinct, boolean sortedInputs) {
        this.distinct = distinct;
        this.sortedInputs = sortedInputs;
    }


    public void clearResult() {
        count = -1;

        if (distinct) {
            if (sortedInputs)
                lastValueSeen = null;
            else
                valuesSeen.clear();
        }
    }


    public void addValue(Object value) {
        // NULL values are ignored by aggregate functions.
        if (value == null)
            return;

        if (count == -1)
            count = 0;

        // Counting distinct values requires more checking than just counting
        // any value that comes through.
        if (distinct) {
            if (sortedInputs) {
                // If the inputs are sorted then we increment the count every
                // time we see a new value.
                if (lastValueSeen == null || !lastValueSeen.equals(value)) {
                    lastValueSeen = value;
                    count++;
                }
            }
            else {
                // If the inputs are hashed then we increment the count every
                // time the value isn't already in the hash-set.
                if (valuesSeen.add(value))
                    count++;
            }
        }
        else {
            // Non-distinct count.  Just increment on any non-null value.
            count++;
        }
    }


    public Object getResult() {
        // A value of -1 indicates a NULL result.
        return (count == -1 ? null : Integer.valueOf(count));
    }
}
