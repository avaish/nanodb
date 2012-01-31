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
        try {
            return storageManager.loadDBPage(tblFileInfo.getDBFile(), 1);
        }
        catch (EOFException e) {
            return null;
        }
    }


    // Inherit Javadocs.
    @Override
    public DBPage getLastDataPage(TableFileInfo tblFileInfo) throws IOException {
        int pages = tblFileInfo.getDBFile().getNumPages();
        if (pages > 1) {
            return storageManager.loadDBPage(tblFileInfo.getDBFile(), pages - 1);
        }
        return null;
    }


    // Inherit Javadocs.
    @Override
    public DBPage getNextDataPage(TableFileInfo tblFileInfo, DBPage dbPage)
        throws IOException {
        try {
            return storageManager.loadDBPage(tblFileInfo.getDBFile(), 
                dbPage.getPageNo() + 1);
        }
        catch (EOFException e) {
            return null;
        }
    }


    // Inherit Javadocs.
    @Override
    public DBPage getPrevDataPage(TableFileInfo tblFileInfo, DBPage dbPage)
        throws IOException {
        int page = dbPage.getPageNo();
        if (page > 2) {
            return storageManager.loadDBPage(tblFileInfo.getDBFile(), 
                dbPage.getPageNo() - 1);
        }
        return null;
    }


    // Inherit Javadocs.
    @Override
    public Tuple getFirstTupleInPage(TableFileInfo tblFileInfo, DBPage dbPage) {
        int numSlots = DataPage.getNumSlots(dbPage);
        for (int iSlot = 0; iSlot < numSlots; iSlot++) {
            int offset = DataPage.getSlotValue(dbPage, iSlot);
            if (offset == DataPage.EMPTY_SLOT)
                continue;
            return new HeapFilePageTuple(tblFileInfo, dbPage, iSlot, offset);
        }
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

        int nextSlot = ptup.getSlot() + 1;
        int numSlots = DataPage.getNumSlots(dbPage);
        
        while (nextSlot < numSlots) {
            int nextOffset = DataPage.getSlotValue(dbPage, nextSlot);
            if (nextOffset != DataPage.EMPTY_SLOT) {
                return new HeapFilePageTuple(tblFileInfo, dbPage, nextSlot, 
                    nextOffset);
            }
            
            nextSlot++;
        }
        
        return null;
    }
}
