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
 * Concatenates arguments as strings.  If any of the arguments is NULL, returns
 * NULL.
 * 
 * @author emil
 */
public class Concat extends Function {
    @Override
    public ColumnInfo getReturnType(List<Expression> args, Schema schema) {
        ColumnType colType = new ColumnType(SQLDataType.VARCHAR);
        return new ColumnInfo(colType);
    }

    @Override
    public Object evaluate(Environment env, List<Expression> args) {
        if (args.size() < 2) {
            throw new ExpressionException("Cannot call CONCAT on " +
                args.size() + " arguments");
        }
        
        StringBuilder buf = new StringBuilder();
        
        for (Expression arg : args) {
            Object val = arg.evaluate(env);

            if (val == null)
                return null;

            buf.append(TypeConverter.getStringValue(val));
        }
        
        return buf.toString();
    }
}
