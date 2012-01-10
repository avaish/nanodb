package edu.caltech.nanodb.storage;


import java.io.IOException;

import java.util.Map;

import edu.caltech.nanodb.relations.Tuple;


/**
 * This interface specifies all operations that must be implemented to support
 * a particular kind of table file.
 */
public interface TableManager {
    /**
     * This method initializes the schema and other details in a newly created
     * table file, using the schema and other details specified in the passed-in
     * <tt>TableFileInfo</tt> object.
     *
     * @param tblFileInfo This object is an in/out parameter.  It is used to
     *        specify the name and schema of the new table being created.  When
     *        the table is successfully created, the object is updated with the
     *        actual file that the table's schema and data are stored in.
     *
     * @throws IOException if the file cannot be created, or if an error occurs
     *         while storing the initial table data.
     */
    void initTableInfo(TableFileInfo tblFileInfo) throws IOException;


    /**
     * This method reads in the schema and other critical information for the
     * specified table.
     *
     * @param tblFileInfo the table information object to populate.  When this
     *        is passed in, it only contains the table's name and the opened
     *        database file to read the data from.
     *
     * @throws IOException if an IO error occurs when attempting to load the
     *         table's schema and other details.
     */
    void loadTableInfo(TableFileInfo tblFileInfo) throws IOException;


    /**
     * This method closes a table file that is currently open, possibly flushing
     * any dirty pages to the table's storage in the process.
     *
     * @param tblFileInfo the table to close
     *
     * @throws IOException if an IO error occurs while attempting to close the
     *         table.  This could occur, for example, if dirty pages are being
     *         flushed to disk and a write error occurs.
     */
    void beforeCloseTable(TableFileInfo tblFileInfo) throws IOException;


    /**
     * Drops the specified table from the database.
     *
     * @param tblFileInfo the table to drop
     *
     * @throws IOException if an IO error occurs while trying to delete the
     *         table's backing storage.
     */
    void beforeDropTable(TableFileInfo tblFileInfo) throws IOException;


    /**
     * Returns the first tuple in this table file, or <tt>null</tt> if there are
     * no tuples in the file.
     *
     * @param tblFileInfo the opened table to get the first tuple from
     *
     * @return the first tuple, or <tt>null</tt> if the table is empty
     *
     * @throws IOException if an IO error occurs while trying to read out the
     *         first tuple
     */
    Tuple getFirstTuple(TableFileInfo tblFileInfo) throws IOException;


    /**
     * Returns the tuple that follows the specified tuple, or <tt>null</tt> if
     * there are no more tuples in the file.
     *
     * @param tblFileInfo the opened table to get the next tuple from
     *
     * @param tup the "previous" tuple in the table
     *
     * @return the tuple following the previous tuple, or <tt>null</tt> if the
     *         previous tuple is the last one in the table
     *
     * @throws IOException if an IO error occurs while trying to retrieve the
     *         next tuple.
     */
    Tuple getNextTuple(TableFileInfo tblFileInfo, Tuple tup) throws IOException;


    /**
     * Returns the tuple corresponding to the specified file pointer.  This
     * method is used by other features in the database, such as indexes.
     *
     * @param tblFileInfo the opened table to get the specified tuple from
     *
     * @param fptr a file-pointer specifying the tuple to retrieve
     * 
     * @return the tuple referenced by <tt>fptr</tt>
     *
     * @throws InvalidFilePointerException if the specified file-pointer doesn't
     *         actually point to a real tuple.
     *
     * @throws IOException if an IO error occurs while trying to retrieve the
     *         specified tuple.
     */
    Tuple getTuple(TableFileInfo tblFileInfo, FilePointer fptr)
        throws InvalidFilePointerException, IOException;


    /**
     * Adds the specified tuple into the table file, returning a new object
     * corresponding to the actual tuple added to the table.
     *
     * @param tblFileInfo the opened table to add the tuple to
     *
     * @param tup a tuple object containing the values to add to the table
     *
     * @return a tuple object actually backed by this table
     *
     * @throws IOException if an IO error occurs while trying to add the new
     *         tuple to the table.
     */
    Tuple addTuple(TableFileInfo tblFileInfo, Tuple tup) throws IOException;


    /**
     * Modifies the values in the specified tuple.
     *
     * @param tblFileInfo the opened table to add the tuple to
     *
     * @param tup the tuple to modify in the table
     *
     * @param newValues a map containing the name/value pairs to use to update
     *        the tuple.  Values in this map will be coerced to the column-type
     *        of the specified columns.  Only the columns being modified need to
     *        be specified in this collection.
     *
     * @throws IOException if an IO error occurs while trying to modify the
     *         tuple's values.
     */
    void updateTuple(TableFileInfo tblFileInfo, Tuple tup,
        Map<String, Object> newValues) throws IOException;


    /**
     * Deletes the specified tuple from the table.
     *
     * @param tblFileInfo the opened table to delete the tuple from
     *
     * @param tup the tuple to delete from the table
     *
     * @throws IOException if an IO error occurs while trying to delete the
     *         tuple.
     */
    void deleteTuple(TableFileInfo tblFileInfo, Tuple tup)
        throws IOException;


    /**
     * This function analyzes the specified table, and updates the table's
     * statistics to be the most up-to-date values.
     *
     * @param tblFileInfo the opened table to analyze.
     *
     * @throws IOException if an IO error occurs while trying to analyze the
     *         table.
     */
    void analyzeTable(TableFileInfo tblFileInfo) throws IOException;


    /**
     * If the table format supports blocked traversal of the table format then
     * this function returns an object implementing the
     * {@link BlockedTableReader} interface.  Otherwise, this method will return
     * <tt>null</tt>.
     * <p>
     * The object returned by this function should be a singleton; just as there
     * should be only one instance of each table-manager implementation, there
     * should only be one instance of each blocked table-reader implementation.
     * (There just isn't any point in allowing more instances than that, since
     * the class doesn't hold state, it just encodes how to manipulate tables.)
     *
     * @return a blocked table-reader for traversing the database in blocks, or
     *         <tt>null</tt> if the table format doesn't support blocked
     *         traversal.
     */
    BlockedTableReader getBlockedReader();
}
