package edu.caltech.nanodb.expressions;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.functions.FunctionDirectory;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


/**
 * This class implements expressions that are function calls.  The set of
 * available functions to call is stored in the {@link FunctionDirectory}.
 */
public class FunctionCall extends Expression {
    /** The string name of the function as specified in the original SQL. */
    private String funcName;


    /** The list of one or more arguments for the function call. */
    private ArrayList<Expression> args;


    /** The actual function object that implements the function call. */
    private Function function;


    public FunctionCall(String funcName, Expression... args) {
        if (funcName == null)
            throw new IllegalArgumentException("funcName cannot be null");

        if (args == null)
            throw new IllegalArgumentException("args cannot be null");

        this.funcName = funcName;
        this.args = new ArrayList<Expression>(Arrays.asList(args));

        function = FunctionDirectory.getInstance().getFunction(funcName);
    }


    public FunctionCall(String funcName, ArrayList<Expression> args) {
        if (funcName == null)
            throw new IllegalArgumentException("funcName cannot be null");

        if (args == null)
            throw new IllegalArgumentException("args cannot be null");

        this.funcName = funcName;
        this.args = args;

        function = FunctionDirectory.getInstance().getFunction(funcName);
    }


    @Override
    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        return function.getReturnType(args, schema);
    }


    @Override
    public Object evaluate(Environment env) throws ExpressionException {
        return function.evaluate(env, args);
    }


    @Override
    public boolean hasSymbols() {
        for (Expression expr : args) {
            if (expr.hasSymbols())
                return true;
        }

        return false;
    }


    @Override
    public void getAllSymbols(Collection<ColumnName> symbols) {
        for (Expression expr : args)
            expr.getAllSymbols(symbols);
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FunctionCall) {
            FunctionCall other = (FunctionCall) obj;
            return (funcName.equals(other.funcName) && args.equals(other.args));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 7;

        hash = hash * 31 + funcName.hashCode();
        hash = hash * 31 + args.hashCode();

        return hash;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append(funcName).append('(');

        boolean first = true;
        for (Expression arg : args) {
            if (first)
                first = false;
            else
                buf.append(", ");

            buf.append(arg.toString());
        }

        buf.append(')');

        return buf.toString();
    }
}
