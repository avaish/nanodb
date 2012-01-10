package edu.caltech.nanodb.storage.heapfile;


import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.TableFileInfo;

import java.util.List;


/**
 */
public class HeapFilePageTuple extends PageTuple {

    /** General information about the table this tuple is from. */
    private TableFileInfo tblFileInfo;


    /**
     * The slot that this tuple corresponds to.  The tuple doesn't actually
     * manipulate the slot table directly; that is for the
     * {@link HeapFileTableManager} to deal with.
     */
    private int slot;


    /**
     * Construct a new tuple object that is backed by the data in the database
     * page.  This tuple is able to be read from or written to.
     *
     * @param tblFileInfo details of the table that this tuple is stored in
     *
     * @param dbPage the specific database page that holds the tuple
     *
     * @param slot the slot number of the tuple
     *
     * @param pageOffset the offset of the tuple's actual data in the page
     */
    public HeapFilePageTuple(TableFileInfo tblFileInfo, DBPage dbPage, int slot,
                             int pageOffset) {
        super(dbPage, pageOffset, tblFileInfo.getSchema().getColumnInfos());

        if (slot < 0) {
            throw new IllegalArgumentException(
                "slot must be nonnegative; got " + slot);
        }

        this.slot = slot;
    }


    protected void insertTupleDataRange(int off, int len) {
        DataPage.insertTupleDataRange(this.getDBPage(), off, len);
    }


    protected void deleteTupleDataRange(int off, int len) {
        DataPage.deleteTupleDataRange(this.getDBPage(), off, len);
    }


    public int getSlot() {
        return slot;
    }


    public static HeapFilePageTuple storeNewTuple(TableFileInfo tblInfo,
        DBPage dbPage, int slot, int pageOffset, Tuple tuple) {

        List<ColumnInfo> colInfos = tblInfo.getSchema().getColumnInfos();
        PageTuple.storeTuple(dbPage, pageOffset, colInfos, tuple);

        return new HeapFilePageTuple(tblInfo, dbPage, slot, pageOffset);
    }
}
