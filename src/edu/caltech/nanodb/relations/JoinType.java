package edu.caltech.nanodb.relations;


/** An enumeration specifying the different types of join operation. */
public enum JoinType {

    /** Inner joins, where only matching rows are included in the result. */
    INNER,

    
    /**
     * Left outer joins, where non-matching rows from the left table are
     * included in the results.
     */
    LEFT_OUTER,


    /**
     * Right outer joins, where non-matching rows from the right table are
     * included in the results.
     */
    RIGHT_OUTER,


    /**
     * Full outer joins, where non-matching rows from either the left or right
     * table are included in the results.
     */
    FULL_OUTER,


    /** Cross joins, which are simply a Cartesian product. */
    CROSS
}
