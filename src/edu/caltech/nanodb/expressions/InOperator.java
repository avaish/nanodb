package edu.caltech.nanodb.expressions;


import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;

import java.util.ArrayList;
import java.util.Collection;


/**
 * <p>
 * This class implements the <tt>expr IN (values)</tt> or <tt>expr IN
 * (subquery)</tt> operator.  This operation may be optimized out of a query,
 * but if it is not, it can still be evaluated although it will be slow.
 * </p>
 * <p>
 * The <tt>expr NOT IN (...)</tt> operator is translated into <tt>NOT (expr IN
 * (...))</tt> by the parser.
 * </p>
 */
public class InOperator extends Expression {
    /**
     * The expression to check against the set on the righthand side of the
     * <tt>IN</tt> operator.
     */
    Expression expr;


    /**
     * If the righthand side of the <tt>IN</tt> operator is a list of values
     * (expressions, specifically), this is the list of values.
     */
    ArrayList<Expression> values;


    /**
     * If the righthand side of the <tt>IN</tt> operator is a <tt>SELECT</tt>
     * subquery, this is the subquery.
     */
    SelectClause subquery;


    public InOperator(Expression expr, ArrayList<Expression> values) {
        if (expr == null)
            throw new IllegalArgumentException("expr must be specified");

        if (values == null)
            throw new IllegalArgumentException("values must be specified");

        if (values.isEmpty())
            throw new IllegalArgumentException("values must be non-empty");

        this.expr = expr;
        this.values = values;
    }


    public InOperator(Expression expr, SelectClause subquery) {
        if (expr == null)
            throw new IllegalArgumentException("expr must be specified");

        if (subquery == null)
            throw new IllegalArgumentException("subquery must be specified");

        this.expr = expr;
        this.subquery = subquery;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        // Comparisons always return Boolean values, so just pass a Boolean
        // value in to the TypeConverter to get out the corresponding SQL type.
        ColumnType colType =
            new ColumnType(TypeConverter.getSQLType(Boolean.FALSE));
        return new ColumnInfo(colType);
    }


    /**
     * Evaluates this comparison expression and returns either
     * {@link java.lang.Boolean#TRUE} or {@link java.lang.Boolean#FALSE}.  If
     * either the left-hand or right-hand expression evaluates to
     * <code>null</code> (representing the SQL <tt>NULL</tt> value), then the
     * expression's result is always <code>FALSE</code>.
     *
     * @design (Donnie) We have to suppress "unchecked operation" warnings on
     *         this code, since {@link Comparable} is a generic (and thus allows
     *         us to specify the type of object being compared), but we want to
     *         use it without specifying any types.
     */
    @SuppressWarnings("unchecked")
    public Object evaluate(Environment env) throws ExpressionException {
        throw new UnsupportedOperationException("Not yet implemented.");
    }


    /**
     * This method returns true if either the left or right subexpression
     * contains symbols.
     */
    public boolean hasSymbols() {
        if (expr.hasSymbols())
            return true;
        
        if (values != null) {
            for (Expression e : values) {
                if (e.hasSymbols())
                    return true;
            }
        }
        else if (subquery != null) {
            // TODO!
            throw new UnsupportedOperationException("Not yet implemented.");
        }

        return false;
    }


    /**
     * Collects all symbols from the left and right subexpressions of this
     * arithmetic operation and stores them into the specified set.
     */
    public void getAllSymbols(Collection<ColumnName> symbols) {
        expr.getAllSymbols(symbols);

        if (values != null) {
            for (Expression e : values)
                e.getAllSymbols(symbols);
        }
        else if (subquery != null) {
            // TODO!
            throw new UnsupportedOperationException("Not yet implemented.");
        }
        else {
            throw new IllegalStateException(
                "Either values or subquery must be specified");
        }
    }


    /**
     * Returns a string representation of this comparison expression and its
     * subexpressions.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        
        // Convert all of the components into string representations.

        String exprStr = expr.toString();
        buf.append(expr.toString()).append(" IN (");

        if (values != null) {
            boolean first = true;
            for (Expression e : values) {
                if (first)
                    first = false;
                else
                    buf.append(", ");
                
                buf.append(e.toString());
            }
        }
        else if (subquery != null) {
            buf.append(subquery.toString());
        }
        else {
            throw new IllegalStateException(
                "Either values or subquery must be specified");
        }

        buf.append(')');

        return buf.toString();
    }


    /**
     * If the <tt>IN</tt> operation has a list of values on the righthand side,
     * this will be the list of values.  Otherwise, this will be <tt>null</tt>.
     *
     * @return the list of values on the righthand side of the <tt>IN</tt>
     *         operation, or <tt>null</tt>.
     */
    public ArrayList<Expression> getValues() {
        return values;
    }


    /**
     * If the <tt>IN</tt> operation has a subquery on the righthand side, this
     * will be the subquery.  Otherwise, this will be <tt>null</tt>.
     *
     * @return the subquery on the righthand side of the <tt>IN</tt> operation,
     *         or <tt>null</tt>.
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

        if (obj instanceof InOperator) {
            InOperator other = (InOperator) obj;

            if (!expr.equals(other.expr))
                return false;

            if (values != null) {
                return values.equals(other.values);
            }
            else if (subquery != null) {
                return subquery.equals(other.subquery);
            }
            else {
                throw new IllegalStateException(
                    "Either values or subquery must be specified");
            }
        }

        return false;
    }


    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + expr.hashCode();

        if (values != null) {
            hash = 31 * hash + values.hashCode();
        }
        else if (subquery != null) {
            hash = 31 * hash + subquery.hashCode();
        }
        else {
            throw new IllegalStateException(
                "Either values or subquery must be specified");
        }

        return hash;
    }


    /**
     * Creates a copy of expression.  This method is used by the
     * {@link Expression#duplicate} method to make a deep copy of an expression
     * tree.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Object clone() throws CloneNotSupportedException {
        InOperator op = (InOperator) super.clone();

        // Clone the subexpressions.  Don't clone the subquery if there is one,
        // since subqueries currently aren't cloneable.

        op.expr = (Expression) expr.clone();

        if (values != null)
            op.values = (ArrayList<Expression>) values.clone();

        return op;
    }
}
