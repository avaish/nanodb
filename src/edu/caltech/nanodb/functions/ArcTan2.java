package edu.caltech.nanodb.functions;


import java.util.List;

import edu.caltech.nanodb.expressions.Environment;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionException;
import edu.caltech.nanodb.expressions.TypeConverter;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.Schema;


/**
 * Computes the arc-tangent of two arguments.  Returns NULL if arguments are NULL.
 * 
 * @author emil
 */
public class ArcTan2 extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        ColumnType colType = new ColumnType(SQLDataType.DOUBLE);
        return new ColumnInfo(colType);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() != 2) {
            throw new ExpressionException("Cannot call ATAN2 on " +
                args.size() + " arguments");
        }

        Object argVal1 = args.get(0).evaluate(env);
        Object argVal2 = args.get(1).evaluate(env);

        if (argVal1 == null || argVal2 == null)
            return null;

        return Math.atan2(TypeConverter.getDoubleValue(argVal1),
                          TypeConverter.getDoubleValue(argVal2));
    }
}
