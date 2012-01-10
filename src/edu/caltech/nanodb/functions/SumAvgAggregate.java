package edu.caltech.nanodb.functions;


import edu.caltech.nanodb.expressions.ArithmeticOperator;
import edu.caltech.nanodb.expressions.LiteralValue;

/**
 */
public class SumAvgAggregate extends AggregateFunction {

    private boolean computeAverage;




    private Object sum;


    private int count;


    public SumAvgAggregate(boolean computeAverage) {
        this.computeAverage = computeAverage;
    }


    @Override
    public void clearResult() {
        sum = null;
        count = 0;
    }

    @Override
    public void addValue(Object value) {
        if (value == null)
            return;

        if (sum == null) {
            // This is the first value.  Store it.
            sum = value;
        }
        else {
            // Add in the new value.
            sum = ArithmeticOperator.evalObjects(ArithmeticOperator.Type.ADD,
                sum, value);
        }

        if (computeAverage)
            count++;
    }

    @Override
    public Object getResult() {
        if (sum == null) {
            return null;
        }
        else if (computeAverage) {
            // Compute average from the sum and count.
            Object avg = ArithmeticOperator.evalObjects(
                ArithmeticOperator.Type.DIVIDE, sum, Integer.valueOf(count));

            return avg;
        }
        else {
            // Just return the sum.
            return sum;
        }
    }
}
