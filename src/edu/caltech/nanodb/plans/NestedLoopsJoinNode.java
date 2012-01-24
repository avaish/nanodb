package edu.caltech.nanodb.plans;


import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.qeval.ColumnStats;
import edu.caltech.nanodb.qeval.PlanCost;
import edu.caltech.nanodb.qeval.SelectivityEstimator;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * This plan node implements a nested-loops join operation, which can support
 * arbitrary join conditions but is also the slowest join implementation.
 */
public class NestedLoopsJoinNode extends ThetaJoinNode {

    /** Most recently retrieved tuple of the left relation. */
    private Tuple leftTuple;

    /** Most recently retrieved tuple of the right relation. */
    private Tuple rightTuple;


    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;
    
    /** Set to true if tuples match when computing a outer join. */
    private boolean matched;
    
    /** Set to true if we are doing some sort of inner join. */
    private boolean isInnerJoin;
    
    /** Null tuple for right schema. */
    private static Tuple NULL_TUPLE;


    public NestedLoopsJoinNode(PlanNode leftChild, PlanNode rightChild,
                JoinType joinType, Expression predicate) {

        super(leftChild, rightChild, joinType, predicate);
        
        if (joinType != JoinType.CROSS && joinType != JoinType.INNER)
        	isInnerJoin = false;
        else
        	isInnerJoin = true;
        
        if (joinType == JoinType.RIGHT_OUTER)
        	super.swap();
    }


    /**
     * Checks if the argument is a plan node tree with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    public boolean equals(Object obj) {

        if (obj instanceof NestedLoopsJoinNode) {
            NestedLoopsJoinNode other = (NestedLoopsJoinNode) obj;

            return predicate.equals(other.predicate) &&
                   leftChild.equals(other.leftChild) &&
                   rightChild.equals(other.rightChild);
        }

        return false;
    }


    /**
     * Computes the hash-code of the nested-loops plan node.
     */
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /**
     * Returns a string representing this nested-loop join's vital information.
     *
     * @return a string representing this plan-node.
     */
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("NestedLoops[");

        if (predicate != null)
            buf.append("pred:  ").append(predicate);
        else
            buf.append("no pred");

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /**
     * Creates a copy of this plan node and its subtrees.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        NestedLoopsJoinNode node = (NestedLoopsJoinNode) super.clone();

        // Clone the predicate.
        if (predicate != null)
            node.predicate = predicate.duplicate();
        else
            node.predicate = null;

        return node;
    }


    /**
     * Nested-loop joins can conceivably produce sorted results in situations
     * where the outer relation is ordered, but we will keep it simple and just
     * report that the results are not ordered.
     */
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** True if the node supports position marking. **/
    public boolean supportsMarking() {
        return leftChild.supportsMarking() && rightChild.supportsMarking();
    }


    /** True if the node requires that its left child supports marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** True if the node requires that its right child supports marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    @Override
    public void prepare() {
        // Need to prepare the left and right child-nodes before we can do
        // our own work.
        leftChild.prepare();
        rightChild.prepare();

        // Use the parent class' helper-function to prepare the schema.
        prepareSchemaStats();
        
        NULL_TUPLE = new TupleLiteral(rightSchema.numColumns());

        // TODO:  Implement the rest
        cost = null;
    }


    public void initialize() {
        super.initialize();

        done = false;
        leftTuple = null;
        rightTuple = null;
    }


    /**
     * Returns the next joined tuple that satisfies the join condition.
     *
     * @return the next joined tuple that satisfies the join condition.
     *
     * @throws IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IOException {
        if (done)
            return null;

        while (getTuplesToJoin()) {
            if (canJoinTuples() || rightTuple.equals(NULL_TUPLE)) {
            	matched = true || isInnerJoin;
                return joinTuples(leftTuple, rightTuple);
            }
        }

        return null;
    }


    /**
     * This helper function implements the logic that sets {@link #leftTuple}
     * and {@link #rightTuple} based on the nested-loops logic.  
     *
     * @return <tt>true</tt> if another pair of tuples was found to join, or
     *         <tt>false</tt> if no more pairs of tuples are available to join.
     */
    private boolean getTuplesToJoin() throws IOException {
        if (leftTuple == null) {
        	Tuple tempLeft = leftChild.getNextTuple();
        	if (tempLeft == null) {
        		done = true;
        		return false;
        	}
        	leftTuple = tempLeft;
        	matched = false || isInnerJoin;
        }
    	Tuple tempRight = rightChild.getNextTuple();
    	if (tempRight == null) {
    		if ((rightTuple == null) && (isInnerJoin)) {
    			done = true;
    			return false;
    		}
    		if (!matched) {
    			rightTuple = NULL_TUPLE;
    			return true;
    		}
    		Tuple tempLeft = leftChild.getNextTuple();
        	if (tempLeft == null) {
        		done = true;
        		return false;
        	}
        	leftTuple = tempLeft;
        	matched = false || isInnerJoin;
        	rightChild.initialize();
        	return getTuplesToJoin();
    	}
    	else {
    		rightTuple = tempRight;
    		return true;
    	}
    }


    private boolean canJoinTuples() {
        // If the predicate was not set, we can always join them!
        if (predicate == null)
            return true;

        environment.clear();
        environment.addTuple(leftSchema, leftTuple);
        environment.addTuple(rightSchema, rightTuple);

        return predicate.evaluatePredicate(environment);
    }


    public void markCurrentPosition() {
        leftChild.markCurrentPosition();
        rightChild.markCurrentPosition();
    }


    public void resetToLastMark() throws IllegalStateException {
        leftChild.resetToLastMark();
        rightChild.resetToLastMark();

        // TODO:  Prepare to reevaluate the join operation for the tuples.
    }


    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }
}
