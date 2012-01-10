package edu.caltech.nanodb.expressions;


import java.util.Collection;

import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


/**
 * <p>
 * This class implements the <tt>EXISTS (subquery)</tt> operator.  This
 * operation may be optimized out of a query, but if it is not, it can still be
 * evaluated although it will be slow.
 * </p>
 * <p>
 * The <tt>NOT EXISTS (subquery)</tt> clause is translated into
 * <tt>NOT (EXISTS (subquery))</tt> by the parser, as expected.
 * </p>
 */
public class ExistsOperator extends Expression {
    /** This is the subquery. */
    SelectClause subquery;


    public ExistsOperator(SelectClause subquery) {
        if (subquery == null)
            throw new IllegalArgumentException("subquery must be specified");

        this.subquery = subquery;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        // Comparisons always return Boolean values, so just pass a Boolean
        // value in to the TypeConverter to get out the corresponding SQL type.
        ColumnType colType =
            new ColumnType(TypeConverter.getSQLType(Boolean.FALSE));
        return new ColumnInfo(colType);
    }


    public Object evaluate(Environment env) throws ExpressionException {
        // TODO:  IMPLEMENT
        throw new UnsupportedOperationException("Not yet implemented.");
    }


    public boolean hasSymbols() {
        // TODO:  IMPLEMENT
        return false;
    }


    /**
     * Collects all symbols from the subquery.
     */
    public void getAllSymbols(Collection<ColumnName> symbols) {
        // TODO:  IMPLEMENT
    }


    /**
     * Returns a string representation of this <tt>EXISTS</tt> expression.
     */
    @Override
    public String toString() {
        return "EXISTS (" + subquery.toString() + ")";
    }


    /**
     * Returns the subquery used in the <tt>EXISTS</tt> operation.
     *
     * @return the subquery used in the <tt>EXISTS</tt> operation.
     */
    public SelectClause getSubquery() {
        return subquery;
    }


    /**
     * Checks if the argument is an expression with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ExistsOperator) {
            ExistsOperator other = (ExistsOperator) obj;
            return subquery.equals(other.subquery);
        }
        return false;
    }


    @Override
    public int hashCode() {
        return subquery.hashCode();
    }


    /**
     * Creates a copy of expression.  This method is used by the
     * {@link Expression#duplicate} method to make a deep copy of an expression
     * tree.
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        ExistsOperator op = (ExistsOperator) super.clone();

        // Don't clone the subquery since subqueries currently aren't cloneable.

        return op;
    }
}
