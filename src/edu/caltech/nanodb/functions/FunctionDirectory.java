package edu.caltech.nanodb.functions;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: donnie
 * Date: 12/29/11
 * Time: 9:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class FunctionDirectory {


    private static FunctionDirectory instance = new FunctionDirectory();
    
    
    public static FunctionDirectory getInstance() {
        return instance;
    }


    private ConcurrentHashMap<String, Function> functions =
        new ConcurrentHashMap<String, Function>();


    /**
     * @design (Donnie) This constructor is currently private just so that we
     *         can maintain this as a singleton.  There's no real reason why it
     *         <em>has to</em> be a singleton though, so if it makes sense at
     *         some point to make this constructor public, that's fine.
     */
    private FunctionDirectory() {

    }

    
    private void initBuiltinFunctions() {
        // These are all standard (i.e. non-aggregate) functions:
        addFunction("ABS", new Abs());
        addFunction("ACOS", new ArcCos());
        addFunction("ASIN", new ArcSin());
        addFunction("ATAN", new ArcTan());
        addFunction("ATAN2", new ArcTan2());
        addFunction("CEIL", new Ceil());
        addFunction("COALESCE", new Coalesce());
        addFunction("CONCAT", new Concat());
        addFunction("COS", new Cos());
        addFunction("FLOOR", new Floor());
        addFunction("GREATEST", new Greatest());
        addFunction("IF", new If());
        addFunction("IFNULL", new IfNull());
        addFunction("LEAST", new Least());
        addFunction("NULLIF", new NullIf());
        addFunction("POW", new Pow());
        addFunction("ROUND", new Round());
        addFunction("SIN", new Sin());
        addFunction("SQRT", new Sqrt());
        addFunction("TAN", new Tan());

        // These are the aggregate functions:
    }


    /**
     * Add a function to the directory.  The function's name is trimmed and
     * converted to uppercase before updating the directory.  If the directory
     * already contains a function with the specified name, an exception is
     * reported.
     * 
     * @param funcName the name of the function
     * @param impl the {@link Function} object that implements this function
     *
     * @throws IllegalArgumentException if the name or implementation is
     *         <tt>null</tt>, or if the function already appears in the
     *         directory
     */
    public void addFunction(String funcName, Function impl) {
        if (funcName == null)
            throw new IllegalArgumentException("funcName cannot be null");

        if (impl == null)
            throw new IllegalArgumentException("impl cannot be null");

        // Probably, function names will come in cleaned up, but this will make
        // doubly sure!
        funcName = funcName.trim().toUpperCase();
        if (functions.containsKey(funcName)) {
            throw new IllegalArgumentException("Function " + funcName +
                " is already in the directory");
        }
        
        functions.put(funcName, impl);
    }
    


    public Function getFunction(String funcName) {
        funcName = funcName.trim().toUpperCase();
        return functions.get(funcName);
    }
}
