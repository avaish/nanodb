package edu.caltech.nanodb.plans;


import java.util.Iterator;
import java.util.LinkedHashSet;


/**
 * Represents the list of plan nodes that the planner generates.  This class
 * uses a list for array access and a set to check for duplicates, ensuring
 * that plan nodes are only added to the list once.
 */
public class PlanArray implements Iterable<PlanNode> {
    /** A set of plans that have been generated so far. */
    private LinkedHashSet<PlanNode> plans = new LinkedHashSet<PlanNode>();


    /** The number of redundant plans generated so far. **/
    private int redundantPlans = 0;


    /**
     * This method adds a new plan to the list, as long as it is not already
     * present.
     *
     * @param plan the plan to add
     *
     * @return true if the plan isn't already in the set
     *
     * @throws java.lang.NullPointerException if the input plan is null
     */
    public boolean addPlan(PlanNode plan) {
        if (plan == null)
            throw new NullPointerException("plan cannot be null");

        boolean added = plans.add(plan);
        if (!added)
            redundantPlans++;

        return added;
    }


    /**
     * Returns the number of unique plans in the set.
     *
     * @return the number of unique plans in the set.
     */
    public int size() {
        return plans.size();
    }


    /**
     * Retrieves the plan at the specified index in the list.
     */
    public Iterator<PlanNode> iterator() {
        return plans.iterator();
    }


    /**
     * Returns the number of redundant plans that the set has rejected.
     *
     * @return the number of redundant plans that the set has rejected.
     */
    public int countRedundantPlans() {
        return redundantPlans;
    }
}
