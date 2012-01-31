package edu.caltech.nanodb.plans;


import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.qeval.ColumnStats;
import edu.caltech.nanodb.qeval.PlanCost;
import edu.caltech.nanodb.qeval.SelectivityEstimator;
import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;

import edu.caltech.nanodb.storage.TableFileInfo;
import edu.caltech.nanodb.storage.TableManager;


/**
 * A select plan-node that scans a table file, checking the optional predicate
 * against each tuple in the file.
 */
public class FileScanNode extends SelectNode {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(FileScanNode.class);


    /** The table to select from if this node is a leaf. */
    public TableFileInfo tblFileInfo;


    /**
     * This field allows the file-scan node to mark a particular tuple in the
     * tuple-stream and then rewind to that point in the tuple-stream.
     */
    private Tuple markedTuple;


    private boolean jumpToMarkedTuple;


    public FileScanNode(TableFileInfo tblFileInfo, Expression predicate) {
        super(predicate);

        if (tblFileInfo == null)
            throw new NullPointerException("table cannot be null");

        this.tblFileInfo = tblFileInfo;
    }


    /**
     * Returns true if the passed-in object is a <tt>FileScanNode</tt> with
     * the same predicate and table.
     *
     * @param obj the object to check for equality
     *
     * @return true if the passed-in object is equal to this object; false
     *         otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FileScanNode) {
            FileScanNode other = (FileScanNode) obj;
            return tblFileInfo.equals(other.tblFileInfo) &&
                   predicate.equals(other.predicate);
        }

        return false;
    }


    /**
     * Computes the hashcode of a PlanNode.  This method is used to see if two
     * plan nodes CAN be equal.
     **/
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + tblFileInfo.hashCode();
        return hash;
    }


    /**
     * Creates a copy of this simple filter node node and its subtree.  This
     * method is used by {@link PlanNode#duplicate} to copy a plan tree.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        FileScanNode node = (FileScanNode) super.clone();

        // The table-info doesn't need to be copied since it's immutable.
        node.tblFileInfo = tblFileInfo;

        return node;
    }


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("FileScan[");
        buf.append("table:  ").append(tblFileInfo.getTableName());

        if (predicate != null)
            buf.append(", pred:  ").append(predicate.toString());

        buf.append("]");

        return buf.toString();
    }


    /**
     * Currently we will always say that the file-scan node produces unsorted
     * results.  In actuality, a file scan's results will be sorted if the table
     * file uses a sequential format, but currently we don't have any sequential
     * file formats.
     */
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** This node supports marking. */
    public boolean supportsMarking() {
        return true;
    }


    /** This node has no children so of course it doesn't require marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** This node has no children so of course it doesn't require marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    protected void prepareSchema() {
        // Grab the schema from the table.
        schema = tblFileInfo.getSchema();
    }


    // Inherit javadocs from base class.
    public void prepare() {
        // Grab the schema and statistics from the table file.
        // (Technically we should update the statistics based on the predicate.)

        schema = tblFileInfo.getSchema();

        TableStats tableStats = tblFileInfo.getStats();
        stats = tableStats.getAllColumnStats();
        
        // update plan costs
        float numTuples = tableStats.numTuples;
        if (predicate != null)
            numTuples *= SelectivityEstimator.estimateSelectivity
                (predicate, schema, stats);
        float tupleSize = tableStats.avgTupleSize;
        float cpuCost = tableStats.numTuples;
        long numBlockIOs = tableStats.numDataPages;
        
        cost = new PlanCost(numTuples, tupleSize, cpuCost, numBlockIOs);
    }


    public void initialize() {
        super.initialize();

        // Reset our marking state.
        markedTuple = null;
        jumpToMarkedTuple = false;
    }


    public void cleanUp() {
        // Nothing to do!
    }


    /**
     * Advances the current tuple forward for a file scan. Grabs the first tuple
     * if current is null. Otherwise gets the next tuple.
     *
     * @throws java.io.IOException if the TableManager failed to open the table.
     */
    protected void advanceCurrentTuple() throws IOException {
        if (jumpToMarkedTuple) {
            logger.debug("Resuming at previously marked tuple.");
            currentTuple = markedTuple;
            jumpToMarkedTuple = false;

            return;
        }

        TableManager tableManager = tblFileInfo.getTableManager();
        if (currentTuple == null)
            currentTuple = tableManager.getFirstTuple(tblFileInfo);
        else
            currentTuple = tableManager.getNextTuple(tblFileInfo, currentTuple);
    }


    public void markCurrentPosition() {
        if (currentTuple == null)
            throw new IllegalStateException("There is no current tuple!");

        logger.debug("Marking current position in tuple-stream.");
        markedTuple = currentTuple;
    }


    public void resetToLastMark() {
        if (markedTuple == null)
            throw new IllegalStateException("There is no last-marked tuple!");

        logger.debug("Resetting to previously marked position in tuple-stream.");
        jumpToMarkedTuple = true;
    }
}
