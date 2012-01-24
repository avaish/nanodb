package edu.caltech.nanodb.qeval;


import java.io.IOException;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.commands.SelectValue;

import edu.caltech.nanodb.expressions.Expression;

import edu.caltech.nanodb.plans.FileScanNode;
import edu.caltech.nanodb.plans.NestedLoopsJoinNode;
import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.ProjectNode;
import edu.caltech.nanodb.plans.RenameNode;
import edu.caltech.nanodb.plans.SelectNode;
import edu.caltech.nanodb.plans.SimpleFilterNode;
import edu.caltech.nanodb.plans.SortNode;

import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;

import org.apache.log4j.Logger;


/**
 * This class generates execution plans for performing SQL queries.  The
 * primary responsibility is to generate plans for SQL <tt>SELECT</tt>
 * statements, but <tt>UPDATE</tt> and <tt>DELETE</tt> expressions can also
 */
public class SimplePlanner implements Planner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SimplePlanner.class);


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause) throws IOException {
    	PlanNode top = null;
    	
    	top = processFromClause(selClause.getFromClause(), selClause.getWhereExpr());
    	
    	if ((selClause.getOrderByExprs() != null) && 
    		(!selClause.getOrderByExprs().isEmpty())) {
    		SortNode sn = new SortNode(top, selClause.getOrderByExprs());
    		top = sn;
    	}
    	if (selClause.isTrivialProject()) {
    		top.prepare();
    		return top;
    	}
    	else {
    		ProjectNode pn = new ProjectNode(top, selClause.getSelectValues());
    		top = pn;
    	}
    	top.prepare();
    	return top;
    }
    
    private PlanNode processFromClause(FromClause fromClause, Expression predicate) 
    		throws IOException {
    	PlanNode top = null;
    	if (fromClause.isBaseTable()) {
    		if (fromClause.isRenamed()) {
    			FileScanNode fs =  new FileScanNode(StorageManager.
        	    	getInstance().openTable(fromClause. getTableName()), null);
        	    top = new RenameNode(fs, fromClause.getResultName());
        	    if (predicate != null) {
            		top = new SimpleFilterNode(top, predicate);
            	}
    		}
    		else {
    			FileScanNode fs =  new FileScanNode(StorageManager.
    				getInstance().openTable(fromClause.
        	    	getTableName()), predicate);
        	    top = fs;
    		}
    	}
    	else if (fromClause.isDerivedTable()) {
    		top = new RenameNode((makePlan(fromClause.
        		getSelectClause())), fromClause.getResultName());
        	if (predicate != null) {
        		top = new SimpleFilterNode(top, predicate);
        	}
    	}
    	else {
    		top = new NestedLoopsJoinNode(processFromClause(fromClause.getLeftChild(), 
    			null), processFromClause(fromClause.getRightChild(), null), 
    			fromClause.getJoinType(), fromClause.getPreparedJoinExpr());
    		if (predicate != null) {
        		top = new SimpleFilterNode(top, predicate);
        	}
    	}
		return top;
    }


    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or <tt>null</tt> if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName,
                                       Expression predicate) throws IOException {
        
        // Open the table.
        TableFileInfo tableInfo = StorageManager.getInstance().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode node = new FileScanNode(tableInfo, predicate);

        return node;
    }
}
