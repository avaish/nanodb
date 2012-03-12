package edu.caltech.nanodb.transactions;


/**
 * This enumeration specifies the locking modes available on lockable items in
 * the database.
 */
public enum LockMode {
    /** Indicates that shared (read-only) access to an item is requested. */
    SHARED,

    /** Indicates that exclusive (read-write) access to an item is requested. */
    EXCLUSIVE
}
