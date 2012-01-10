package edu.caltech.nanodb.functions;


import java.util.List;

import edu.caltech.nanodb.expressions.Environment;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionException;
import edu.caltech.nanodb.expressions.TypeConverter;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;


/**
 * Implements {@code IF (cond, expr1, expr2)}. If the first argument is
 * {@code TRUE}, returns {@code expr1}, else returns {@code expr2}.
 *
 * @author emil
 */
public class If extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        if (args.size() != 3) {
            throw new ExpressionException("Cannot call IF on " + args.size() +
                " arguments");
        }

        // Return the type of the second argument.
        return args.get(1).getColumnInfo(schema);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 3) {
            throw new ExpressionException("Cannot call IF on " + args.size() +
                " arguments");
        }

        Object condVal = args.get(0).evaluate(env);
        
        if (condVal != null && TypeConverter.getBooleanValue(condVal))
            return args.get(1).evaluate(env);
        else
            return args.get(2).evaluate(env);
    }
}
