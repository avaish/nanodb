package edu.caltech.nanodb.qeval;


import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.SelectNode;

import java.io.IOException;


/**
 * <p>
 * This interface specifies the common entry-point for all query
 * planner/optimizer implementations.  The interface is very simple, but a
 * particular implementation might be very complicated depending on what kinds
 * of optimizations are implemented.  Note that a new planner/optimizer is
 * created for each query being planned
 * </p>
 * <p>
 * To support initialization of arbitrary planners, a <tt>Planner</tt>
 * implementation should only have a default constructor.  The
 * {@link PlannerFactory} class is used to instantiate planners.
 * </p>
 */
public interface Planner {
    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.  There is no requirement that tuples produced by the returned plan
     * should support updating or deletion.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an error occurs when the planner is generating a
     *         plan (e.g. because statistics, schema, or indexing information
     *         can't be loaded)
     */
    PlanNode makePlan(SelectClause selClause) throws IOException;


    /**
     * Returns a plan tree for executing a simple select against a single table,
     * whose tuples can also be used for updating and deletion.  This is a
     * strict requirement, as this method is used by the
     * {@link edu.caltech.nanodb.commands.UpdateCommand} and
     * {@link edu.caltech.nanodb.commands.DeleteCommand} classes to create the
     * plans suitable for executing
     *
     * @param tableName the table that the select will operate against
     * @param predicate the selection predicate to apply, or <tt>null</tt> if
     *        all tuples in the table should be returned
     *
     * @return a plan tree for executing the select operation
     *
     * @throws IOException if an error occurs when the planner is generating a
     *         plan (e.g. because statistics, schema, or indexing information
     *         can't be loaded)
     */
    SelectNode makeSimpleSelect(String tableName, Expression predicate)
        throws IOException;
}
