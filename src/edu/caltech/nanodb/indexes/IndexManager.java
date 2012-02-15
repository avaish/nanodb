package edu.caltech.nanodb.indexes;


import edu.caltech.nanodb.relations.ColumnIndexes;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.PageTuple;

import java.io.IOException;


/**
 * This interface specifies all operations that must be implemented to support
 * a particular kind of index file.
 */
public interface IndexManager {
    /**
     * This method initializes a newly created index file, using the details
     * specified in the passed-in <tt>IndexFileInfo</tt> object.
     *
     * @param idxFileInfo This object is an in/out parameter.  It is used to
     *        specify the name and details of the new index being created.  When
     *        the index is successfully created, the object is updated with the
     *        actual file that the index's data is stored in.
     *
     * @throws IOException if the file cannot be created, or if an error occurs
     *         while storing the initial index data.
     */
    void initIndexInfo(IndexFileInfo idxFileInfo) throws IOException;


    String getUnnamedIndexPrefix(IndexFileInfo idxFileInfo);


    /**
     * This method loads the details for the specified index.
     *
     * @param idxFileInfo the index information object to populate.  When this
     *        is passed in, it only contains the index's name, the name of the
     *        table the index is specified on, and the opened database file to
     *        read the data from.
     *
     * @throws IOException if an IO error occurs when attempting to load the
     *         index's details.
     */
    void loadIndexInfo(IndexFileInfo idxFileInfo) throws IOException;


    /**
     * This method adds a tuple to an index.  The tuple must be a
     * {@link PageTuple} since the file-pointer to the tuple must be stored
     * into the index.
     *
     * @param idxFileInfo the index to add the tuple to
     *
     * @param tup the tuple to add to the index
     *
     * @throws IOException if an IO error occurs when attempting to add the
     *         tuple.
     */
    void addTuple(IndexFileInfo idxFileInfo, PageTuple tup) throws IOException;


    /**
     * This method deletes a tuple from an index.  The tuple must be a
     * {@link PageTuple} since the file-pointer to the tuple must be removed
     * from the index.
     *
     * @param idxFileInfo the index to delete the tuple from
     *
     * @param tup the tuple to delete from the index
     *
     * @throws IOException if an IO error occurs when attempting to delete the
     *         tuple.
     */
    void deleteTuple(IndexFileInfo idxFileInfo, PageTuple tup) throws IOException;
}
