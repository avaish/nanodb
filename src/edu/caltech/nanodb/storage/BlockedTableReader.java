package edu.caltech.nanodb.storage;


import edu.caltech.nanodb.relations.Tuple;

import java.io.IOException;


/**
 * This interface allows a table file to be read in a blocked manner, which
 * allows some operations such as nested-loops join to be implemented more
 * efficiently.  In addition, the interface allows for a file to be read from
 * beginning to end, or vice versa.
 */
public interface BlockedTableReader {
    /**
     * Returns the first page that would contain data in the table being read.
     * Note that there is no gurantee that the page does contain table data, for
     * example if all tuples in the page have been deleted, or if no tuples have
     * yet been added to this page.
     *
     * @param tblFileInfo the opened table to read
     *
     * @return the first data page in the table file, or <tt>null</tt> if there
     *         are no data pages.
     *
     * @throws IOException if an IO error occurs while retrieving the first data
     *         page.
     */
    DBPage getFirstDataPage(TableFileInfo tblFileInfo) throws IOException;


    /**
     * Returns the last page that would contain data in the table being read.
     * Note that there is no gurantee that the page does contain table data, for
     * example if all tuples in the page have been deleted, or if no tuples have
     * yet been added to this page.
     *
     * @param tblFileInfo the opened table to read
     *
     * @return the last data page in the table file, or <tt>null</tt> if there
     *         are no data pages.
     *
     * @throws IOException if an IO error occurs while retrieving the first data
     *         page.
     */
    DBPage getLastDataPage(TableFileInfo tblFileInfo) throws IOException;


    /**
     * Returns the next data page in the table that would follow the current
     * data paqe, or <tt>null</tt> if there are no more data pages.  Note that
     * there is no gurantee that the page does contain table data, for example
     * if all tuples in the page have been deleted, or if no tuples have yet
     * been added to this page.
     *
     * @param tblFileInfo the opened table to read
     *
     * @param dbPage the current data page in the table file
     *
     * @return the next data page in the table file, or <tt>null</tt> if there
     *         are no more data pages.
     *
     * @throws IOException if an IO error occurs while retrieving the next data
     *         page.
     */
    DBPage getNextDataPage(TableFileInfo tblFileInfo, DBPage dbPage)
        throws IOException;


    /**
     * Returns the previous data page in the table that would precede the
     * current data paqe.  Note that there is no gurantee that the page does
     * contain table data, for example if all tuples in the page have been
     * deleted, or if no tuples have yet been added to this page.
     *
     * @param tblFileInfo the opened table to read
     *
     * @param dbPage the current data page in the table file
     *
     * @return the previous data page in the table file, or <tt>null</tt> if
     *         there are no more data pages.
     *
     * @throws IOException if an IO error occurs while retrieving the previous
     *         data page.
     */
    DBPage getPrevDataPage(TableFileInfo tblFileInfo, DBPage dbPage)
        throws IOException;


    /**
     * Returns the first tuple in the current data page, or <tt>null</tt> if
     * there are no tuples in the page.
     *
     * @param tblFileInfo the opened table to read
     *
     * @param dbPage the data page to get the first tuple from
     *
     * @return the first tuple in the data page, or <tt>null</tt> if there are
     *         no tuples in the page
     */
    Tuple getFirstTupleInPage(TableFileInfo tblFileInfo, DBPage dbPage);


    /**
     * Returns the next tuple in the current data page, or <tt>null</tt> if
     * there are no more tuples in the page.
     *
     * @param tblFileInfo the opened table to read
     * @param dbPage the data page to get the next tuple from
     * @param tuple the current tuple in the page
     *
     * @return the next tuple in the data page, or <tt>null</tt> if there are
     *         no more tuples in the page
     */
    Tuple getNextTupleInPage(TableFileInfo tblFileInfo, DBPage dbPage,
                             Tuple tuple);
}
