package edu.caltech.nanodb.expressions;


import java.util.ArrayList;
import java.util.Collection;

import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.functions.FunctionDirectory;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


/**
 * Created by IntelliJ IDEA.
 * User: donnie
 * Date: 12/30/10
 * Time: 9:39 AM
 * To change this template use File | Settings | File Templates.
 */
public class FunctionCall extends Expression {
    private String funcName;

    /** The list of one or more arguments for the function call. */
    private ArrayList<Expression> args;

    private Function function;


    public FunctionCall(String funcName, ArrayList<Expression> args) {
        if (funcName == null || args == null)
            throw new NullPointerException();

        this.funcName = funcName;
        this.args = args;
        function = FunctionDirectory.getInstance().getFunction(funcName);
    }

    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object evaluate(Environment env) throws ExpressionException {
        return function.evaluate(env, args);
    }

    public boolean hasSymbols() {
        for (Expression expr : args) {
            if (expr.hasSymbols())
                return true;
        }

        return false;
    }

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
