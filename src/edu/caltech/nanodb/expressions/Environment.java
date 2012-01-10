package edu.caltech.nanodb.expressions;


import java.util.ArrayList;
import java.util.SortedMap;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This class holds the environment for evaluating expressions that include
 * symbols.  For example, in the SQL command:
 * <pre>  SELECT a, b + 5 FROM t WHERE c < 20;</pre>
 * All of the expressions refer to columns in the current tuple being considered
 * from the table <tt>t</tt>, and thus need to be able to access the current
 * tuple.  This is the role that the environment class serves.
 * <p>
 * An important detail about the environment is that a single tuple's schema can
 * hold values from multiple tables, such as when a tuple is produced as the
 * result of a join operation between two tables.
 *
 * @design (donnie) This class could be applied in several different ways.
 *         Any SELECT clause really could (or should) have its own environment
 *         associated with it, because it will reference tables.  In addition,
 *         a derived table (a named subquery in the FROM clause) can also be
 *         referred to by name.  So, we will have to devise a strategy for
 *         managing environments properly.  Some plan-nodes will have to be
 *         responsible for updating environments, but definitely not all will do
 *         so.
 *         <p>
 *         It probably makes the most sense to give <em>every</em> plan-node its
 *         own environment-reference.  If the reference is null, the node could
 *         get its parent's environment.  Or, we could set all plan-nodes to
 *         have a specific environment, and just manage that assignment process
 *         carefully.
 *         <p>
 *         Environments can refer to a parent environment, for cases where a
 *         query contains subqueries.  The subqueries can refer to the same
 *         table(s) as the outer query, and thus they need their own environment
 *         to track that information.  This becomes especially useful with
 *         correlated subqueries, as the inner query needs to be completely
 *         reevaluated for each value of the outer query.
 *         <p>
 *         Matching a symbol name goes from child to parent.  If a child
 *         environment contains a value for a particular symbol, that value is
 *         returned.  It is only if the child environment <em>doesn't</em>
 *         contain a value that the parent environment is utilized.
 */
public class Environment {

    private ArrayList<Schema> currentSchemas;


    /**
     * A mapping of string table names, to the current tuple for each of those
     * tables.  This class does not allow either table names or "current tuple"
     * values to be <tt>null</tt>.
     */
    private ArrayList<Tuple> currentTuples;


    /**
     * If not <tt>null</tt>, this environment refers to a parent environment
     * that can be used to resolve symbol names into values.
     */
    private Environment parent;


    /**
     * Construct a new, empty environment with a (possibly <tt>null</tt>)
     * parent environment.
     */
    public Environment(Environment parent) {
        currentSchemas = new ArrayList<Schema>();
        currentTuples = new ArrayList<Tuple>();
        this.parent = parent;
    }


    /** Construct a new, empty environment with no parent environment. */
    public Environment() {
        this(null);
    }


    public void clear() {
        currentSchemas.clear();
        currentTuples.clear();
    }


    public void addTuple(Schema schema, Tuple tuple) {
        if (schema == null)
            throw new NullPointerException("schema cannot be null");

        if (tuple == null)
            throw new NullPointerException("tuple cannot be null");

        currentSchemas.add(schema);
        currentTuples.add(tuple);
    }


    /**
     * Given a table name, this method returns the current tuple being
     * considered from that table.  If the table name does not appear in this
     * environment, and the environment has a parent environment,
     * <tt>getCurrentTuple(tableName)</tt> is called on the parent
     * environment.  If the table name is not recognized, <tt>null</tt> is
     * returned.
     *
     * @return the current tuple for the specified table name, or
     *         <tt>null</tt> if the specified table is unrecognized in the
     *         environment.
     *
    public Tuple getCurrentTuple(String tableName) {
        Tuple tup = currentTuples.get(tableName);
        if (tup == null && parentEnv != null)
            tup = parentEnv.getCurrentTuple(tableName);

        return tup;
    }
    */


    public Object getColumnValue(ColumnName colName) {
        Object result = null;
        boolean found = false;

        for (int i = 0; i < currentTuples.size(); i++) {
            Tuple tuple = currentTuples.get(i);
            Schema schema = currentSchemas.get(i);

            SortedMap<Integer, ColumnInfo> cols = schema.findColumns(colName);
            if (cols.isEmpty())
                continue;

            if (found || cols.size() > 1) {
                throw new ExpressionException("Column name " + colName +
                    " is ambiguous");
            }

            int index = cols.keySet().iterator().next();
            result = tuple.getColumnValue(index);
            found = true;
        }

        if (!found && parent != null)
            result = parent.getColumnValue(colName);

        return result;
    }
}
