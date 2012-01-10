package edu.caltech.nanodb.server;


import edu.caltech.nanodb.relations.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * 
 * @review (Donnie) Maybe we should include the command that was run.
 */
public class CommandResult implements Serializable {
    /** The system time when command execution started. */
    private long startTimestamp = -1;


    /** The system time when command execution ended. */
    private long endTimestamp = -1;


    /**
     * If a failure occurs while a command is being executed, this will be the
     * exception that indicated the failure.
     */
    private Throwable failure = null;
    
    

    /** The time to the first result being produced. */
    private long firstResultTimestamp = -1;


    /**
     * If the command was a <tt>SELECT</tt> query and the results are to be
     * kept, this will be a collection of the tuples in the order they were
     * produced by the database.
     */
    private ArrayList<Tuple> tuples = null;



    public void startExecution() {
        startTimestamp = System.currentTimeMillis();
    }
    
    
    public void recordFailure(Throwable t) {
        if (t == null)
            throw new IllegalArgumentException("t cannot be null");

        failure = t;
    }
    
    
    public boolean failed() {
        return (failure != null);
    }
    
    
    public Throwable getFailure() {
        return failure;
    }

    
    public void addTuple(Tuple t) {
        if (tuples == null) {
            tuples = new ArrayList<Tuple>();
            firstResultTimestamp = System.currentTimeMillis();
        }

        tuples.add(t);
    }


    public void endExecution() {
        endTimestamp = System.currentTimeMillis();
    }


    /**
     * Returns the total execution time of the command in milliseconds.
     * 
     * @return the total execution time of the command in milliseconds.
     */
    public long getTotalTime() {
        return endTimestamp - startTimestamp;
    }


    /**
     * Returns the time to the first result in milliseconds.
     *
     * @return the time to the first result in milliseconds.
     */
    public long getTimeToFirstResult() {
        if (tuples == null)
            throw new IllegalStateException("The command produced no results.");

        return firstResultTimestamp - startTimestamp;
    }


    public List<Tuple> getTuples() {
        return Collections.unmodifiableList(tuples);
    }
}
