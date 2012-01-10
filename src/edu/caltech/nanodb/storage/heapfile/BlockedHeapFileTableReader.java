package edu.caltech.nanodb.storage.heapfile;


import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.BlockedTableReader;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;

import java.io.EOFException;
import java.io.IOException;


/**
 * An implementation of the <tt>BlockedTableReader</tt> interface, allowing
 * heap table-files to be read/scanned in a block-by-block manner, as opposed to
 * a tuple-by-tuple manner.
 */
public class BlockedHeapFileTableReader implements BlockedTableReader {
    /**
     * The table reader uses the storage manager a lot, so it caches a reference
     * to the singleton instance of the storage manager at initialization.
     */
    private StorageManager storageManager;


    /**
     * Initializes the blocked heap-file table reader.  All the constructor
     * currently does is to cache a reference to the singleton
     * {@link StorageManager}, since it is used so extensively.
     */
    public BlockedHeapFileTableReader() {
        this.storageManager = StorageManager.getInstance();
    }


    // Inherit Javadocs.
    @Override
    public DBPage getFirstDataPage(TableFileInfo tblFileInfo) throws IOException {
        // TODO:  Implement.
        return null;
    }


    // Inherit Javadocs.
    @Override
    public DBPage getLastDataPage(TableFileInfo tblFileInfo) throws IOException {
        // TODO:  Implement.
        return null;
    }


    // Inherit Javadocs.
    @Override
    public DBPage getNextDataPage(TableFileInfo tblFileInfo, DBPage dbPage)
        throws IOException {

        // TODO:  Implement.
        return null;
    }


    // Inherit Javadocs.
    @Override
    public DBPage getPrevDataPage(TableFileInfo tblFileInfo, DBPage dbPage)
        throws IOException {

        // TODO:  Implement.
        return null;
    }


    // Inherit Javadocs.
    @Override
    public Tuple getFirstTupleInPage(TableFileInfo tblFileInfo, DBPage dbPage) {
        // TODO:  Implement.
        return null;
    }


    // Inherit Javadocs.
    @Override
    public Tuple getNextTupleInPage(TableFileInfo tblFileInfo, DBPage dbPage,
        Tuple tup) {

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        // TODO:  Implement.
        return null;
    }
}
