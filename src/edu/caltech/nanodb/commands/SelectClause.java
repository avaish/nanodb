package edu.caltech.nanodb.commands;


import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.relations.*;
import org.apache.log4j.Logger;


/**
 * This class represents a single <tt>SELECT ...</tt> statement or clause.
 * <tt>SELECT</tt> statements can appear as clauses within other expressions,
 * so the class is written to be used easily within other classes.
 */
public class SelectClause {

    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(SelectClause.class);


    /**
     * This flag indicates whether the SELECT expression should generate
     * duplicate rows, or whether it should simply produce distinct or unique
     * rows.
     */
    private boolean distinct = false;


    /**
     * The specification of values to be produced by the SELECT clause.  These
     * expressions comprise the Generalized Project operation portion of the
     * command.
     */
    private List<SelectValue> selectValues = new ArrayList<SelectValue>();
  
  
    /**
     * This field holds a hierarchy of one or more base and derived relations
     * that produce the rows considered by this <tt>SELECT</tt> clause.  If the
     * <tt>SELECT</tt> expression has no <tt>FROM</tt> clause, this field will
     * be <tt>null</tt>.
     */
    private FromClause fromClause = null;


    /**
     * If a <tt>WHERE</tt> expression is specified, this field will refer to
     * the expression to be evaluated.
     */
    private Expression whereExpr = null;


    /**
     * This collection holds zero or more entries specifying <tt>GROUP BY</tt>
     * values.  If the <tt>SELECT</tt> expression has no <tt>GROUP BY</tt>
     * clause, this collection will be empty.
     */
    private List<Expression> groupByExprs = new ArrayList<Expression>();


    /**
     * If a <tt>HAVING</tt> expression is specified, this field will refer to
     * the expression to be evaluated.
     */
    private Expression havingExpr = null;


    /**
     * This collection holds zero or more entries specifying <tt>ORDER BY</tt>
     * values.  If the <tt>SELECT</tt> expression has no <tt>ORDER BY</tt>
     * clause, this collection will be empty.
     */
    private List<OrderByExpression> orderByExprs = new ArrayList<OrderByExpression>();


    /**
     * When preparing SQL commands for execution, this value is filled in with
     * the schema of the data that this SELECT clause produces.
     */
    private Schema schema = null;


    /**
     * Mark the select clause's results as being distinct or not distinct.  This
     * corresponds to whether the SQL command is "<tt>SELECT [ALL] ...</tt>" or
     * "<tt>SELECT DISTINCT ...</tt>".
     *
     * @param distinct If true, specifies that the results of this select clause
     *        are distinct.  If false, the results of the clause are not distinct.
     */
    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }


    /**
     * Returns true if the select clause's results are to be distinct, or
     * false if the clause's results are not distinct.
     */
    public boolean isDistinct() {
        return distinct;
    }


    /**
     * Adds a specification to the set of values produced by this SELECT
     * clause.  This method is called by the parser as a SELECT command (or
     * subquery) is being parsed.
     */
    public void addSelectValue(SelectValue selectValue) {
        if (selectValue == null)
            throw new NullPointerException();
  
        selectValues.add(selectValue);
    }


    /**
     * Retrieves the select values for this select clause.
     *
     * @return the select values
     */
    public List<SelectValue> getSelectValues() {
        return selectValues;
    }


    public boolean isTrivialProject() {
        if (selectValues.size() == 1) {
            SelectValue selVal = selectValues.get(0);
            if (selVal.isWildcard() && !selVal.getWildcard().isTableSpecified())
                return true;
        }
        return false;
    }


    /**
     * Sets the hierarchy of base and derived relations that produce the rows
     * considered by this <tt>SELECT</tt> clause.
     */
    public void setFromClause(FromClause fromClause) {
        this.fromClause = fromClause;
    }


    /**
     * Retrieves the from clause for this select clause.
     *
     * @return the from clause
     */
    public FromClause getFromClause() {
        return fromClause;
    }


    /**
     * Sets the expression for the <tt>WHERE</tt> clause.  A <code>null</code>
     * value indicates that the SELECT clause has no WHERE condition.
     */
    public void setWhereExpr(Expression whereExpr) {
        this.whereExpr = whereExpr;
    }


    /**
     * Retrieves the where clause from this from clause.
     *
     * @return the where exprssion
     */
    public Expression getWhereExpr() {
        return whereExpr;
    }


    public void addGroupByExpr(Expression groupExpr) {
        groupByExprs.add(groupExpr);
    }


    public List<Expression> getGroupByExprs() {
        return groupByExprs;
    }

    
    public void setHavingExpr(Expression havingExpr) {
        this.havingExpr = havingExpr;
    }


    public Expression getHavingExpr() {
        return havingExpr;
    }


    public void addOrderByExpr(OrderByExpression orderByExpr) {
        orderByExprs.add(orderByExpr);
    }


    public List<OrderByExpression> getOrderByExprs() {
        return orderByExprs;
    }


    /**
     * This method computes the resulting schema from this query, and in the
     * process it performs various semantic checks as well.
     *
     * @return the schema of this select clause's result
     *
     * @throws IOException if the database cannot read schema from the disk
     *         along the way
     *
     * @throws SchemaNameException if the select clause contains some kind of
     *         semantic error involving schemas that are referenced
     */
    public Schema computeSchema() throws IOException, SchemaNameException {

        // This object holds the schema that expressions in the select-clause
        // will be evaluated against.
        Schema selectSchema = new Schema();

        // Compute the schema of the FROM clause first.
        if (fromClause != null) {
            Schema fromSchema = fromClause.prepare();
            logger.debug("From-clause schema:  " + fromSchema);

            selectSchema.append(fromSchema);
        }

        // Make sure that all expressions in this SELECT clause reference
        // known and non-ambiguous names from the FROM clause.

        // SELECT values:  SELECT a, b + c, tbl.* ...
        Schema selectResultsSchema = new Schema();
        for (SelectValue selVal : selectValues) {
            if (selVal.isWildcard()) {
                // Make sure that if a table name is specified, that the table
                // name is in the SELECT clause's schema.
                ColumnName colName = selVal.getWildcard();
                if (colName.isTableSpecified()) {
                    if (!selectSchema.getTableNames().contains(colName.getTableName())) {
                        throw new SchemaNameException("SELECT-value " + colName +
                            " specifies an unrecognized table name.");
                    }
                }
            }
            else {
                // Not a wildcard.  Get the list of column-values, and resolve
                // each one.
                Expression expr = selVal.getExpression();
                resolveExpressionRefs("SELECT-value", expr, selectSchema);
            }

            // Update the result-schema with this select-value's column-info(s).
            selectResultsSchema.append(
                selVal.getColumnInfos(selectSchema, selectResultsSchema));
        }

        logger.debug("Select-results schema:  " + selectResultsSchema);

        // TODO:  Update selectSchema to include columns in selectResultsSchema as well.

        // WHERE clause:
        if (whereExpr != null)
            resolveExpressionRefs("WHERE clause", whereExpr, selectSchema);

        // GROUP BY clauses:
        for (Expression expr : groupByExprs)
            resolveExpressionRefs("GROUP BY clause", expr, selectSchema);

        // HAVING clause:
        if (havingExpr != null)
            resolveExpressionRefs("HAVING clause", havingExpr, selectSchema);

        // ORDER BY clauses:
        for (OrderByExpression expr : orderByExprs)
            resolveExpressionRefs("ORDER BY clause", expr.getExpression(), selectSchema);

        // All done!  Store and return the results.

        schema = selectResultsSchema;
        return schema;
    }


    /**
     * This helper function goes through the expression and verifies that every
     * symbol-reference corresponds to an actual value produced by the
     * <tt>FROM</tt>-clause of the <tt>SELECT</tt> expression.  Any column-names
     * that don't include a table-name are also updated to include the proper
     * table-name.
     *
     * @param desc A short string describing the context of the expression,
     *        since expressions can appear in the <tt>SELECT</tt> clause, the
     *        <tt>WHERE</tt> clause, the <tt>GROUP BY</tt> clause, etc.
     *
     * @param expr The expression that will be evaluated.
     *
     * @param s The schema against which the expression will be evaluated.
     *
     * @throws SchemaNameException if an expression-reference cannot be resolved
     *         against the specified schema, either because the named column
     *         or table doesn't appear in the schema, or if a column name is
     *         ambiguous.
     */
    private void resolveExpressionRefs(String desc, Expression expr, Schema s)
        throws SchemaNameException {

        // Get the list of column-values in the expression, and resolve each one.

        ArrayList<ColumnName> exprColumns = new ArrayList<ColumnName>();
        expr.getAllSymbols(exprColumns);

        for (ColumnName colName : exprColumns) {
            assert !colName.isColumnWildcard();

            SortedMap<Integer, ColumnInfo> found = s.findColumns(colName);

            if (!colName.isTableSpecified()) {
                // Try to resolve the table name using the column name.

                if (found.size() == 1) {
                    ColumnInfo colInfo = found.get(found.firstKey());
                    colName.setTableName(colInfo.getTableName());
                }
                else if (found.size() == 0) {
                    throw new SchemaNameException(desc + " " + expr +
                        " references an unknown column " + colName + ".");
                }
                else {
                    assert found.size() > 1;
                    throw new SchemaNameException(desc + " " + expr +
                        " contains an ambiguous column " + colName +
                        "; found " + found.values() + ".");
                }
            }
            else {
                // Verify that the column name references a real column.

                // Shouldn't be possible to have this match multiple tables,
                // since the table name is specified.
                assert colName.isTableSpecified();
                assert found.size() <= 1;

                if (found.size() == 0) {
                    throw new SchemaNameException(desc + " " + expr +
                        " references an unknown column " + colName + ".");
                }
            }
        }
    }


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("SelectClause[\n");

        if (selectValues != null && selectValues.size() > 0)
            buf.append("\tvalues=").append(selectValues).append('\n');

        if (fromClause != null)
            buf.append("\tfrom=").append(fromClause).append('\n');

        if (whereExpr != null)
            buf.append("\twhere=").append(whereExpr).append('\n');

        if (groupByExprs != null && groupByExprs.size() > 0)
            buf.append("\tgroup_by=").append(groupByExprs).append('\n');

        if (havingExpr != null)
            buf.append("\thaving=").append(havingExpr).append('\n');

        if (orderByExprs != null && orderByExprs.size() > 0)
            buf.append("\torder_by=").append(orderByExprs).append('\n');

        buf.append(']');

        return buf.toString();
    }
}
