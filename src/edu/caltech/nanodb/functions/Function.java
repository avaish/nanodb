package edu.caltech.nanodb.functions;


import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.expressions.Environment;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionException;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;


/**
 * Built-in function wrapper.
 * 
 * Built-in functions should extend this class and implement the 
 * {@code evaluate()} method.
 * 
 * Instances of the built-in functions should be created using the factory
 * method {@code fromName}. For example, in order to define the function ABS,
 * you should implement a class {@code Abs} that implements {@code Function}.
 * The factory method will return the {@code Abs} implementation for any
 * string that is case-insensitively equal to "ABS".
 * 
 * @author emil
 */
public abstract class Function {

    public abstract ColumnInfo getReturnType(List<Expression> args,
                                             Schema schema);

    /**
     * Evaluates the function.
     * 
     * Should be called at runtime for every function call.
     * 
     * @param env Environment, in which the arguments are evaluated
     * @param args Arguments for this function
     * 
     * @return The value of the function
     * 
     * @throws ExpressionException when there is a problem with the evaluation
     *                             like wrong number of arguments.
     */
    public abstract Object evaluate(Environment env, List<Expression> args);
}
