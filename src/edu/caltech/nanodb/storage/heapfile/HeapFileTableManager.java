package edu.caltech.nanodb.storage.heapfile;


import java.io.EOFException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.qeval.ColumnStats;
import edu.caltech.nanodb.qeval.ColumnStatsCollector;
import edu.caltech.nanodb.qeval.TableStats;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.ForeignKeyColumnIndexes;
import edu.caltech.nanodb.relations.KeyColumnIndexes;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.storage.BlockedTableReader;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.InvalidFilePointerException;
import edu.caltech.nanodb.storage.PageReader;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.PageWriter;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;
import edu.caltech.nanodb.storage.TableManager;


/**
 * This class manages heap files that use the slotted page format for storing
 * variable-size tuples.
 */
public class HeapFileTableManager implements TableManager {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(HeapFileTableManager.class);


    /**
     * Specifying "true" for this system property enables the auto-updating of
     * table statistics that can be updated in an incremental manner.
     */
    public static final String PROP_AUTO_UPDATE_STATS = "nanodb.stats.autoupdate";


    /*
    private static boolean isStatsAutoUpdateEnabled() {
        return Boolean.getBoolean(PROP_AUTO_UPDATE_STATS);
    }
    */


    /*===========================================================================
     * NON-STATIC FIELDS AND METHODS
     */


    /**
     * The table manager uses the storage manager a lot, so it caches a reference
     * to the singleton instance of the storage manager at initialization.
     */
    private StorageManager storageManager;


    /**
     * A singleton instance of the blocked table-reader for heap files.  This
     * is initialized the first time {@link #getBlockedReader} is called.
     */
    private BlockedHeapFileTableReader blockedReader;


    /**
     * Initializes the heap-file table manager.  This class shouldn't be
     * initialized directly, since the storage manager will initialize it when
     * necessary.
     *
     * @param storageManager the storage manager that is using this table manager
     *
     * @throws IllegalArgumentException if <tt>storageManager</tt> is <tt>null</tt>
     */
    public HeapFileTableManager(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.storageManager = storageManager;
    }


    // Copy interface javadocs.
    @Override
    public void initTableInfo(TableFileInfo tblFileInfo) throws IOException {

        String tableName = tblFileInfo.getTableName();
        DBFile dbFile = tblFileInfo.getDBFile();
        TableSchema schema = tblFileInfo.getSchema();

        logger.info(String.format(
            "Initializing new table %s with %d columns, stored at %s",
            tableName, schema.numColumns(), dbFile));

        // Table schema is stored into the header page, so get it and prepare
        // to write out the schema information.
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
        PageWriter hpWriter = new PageWriter(headerPage);
        // Skip past the page-size value.
        hpWriter.setPosition(HeaderPage.OFFSET_NCOLS);

        // Write out the schema details now.
        logger.info("Writing table schema:  " + schema);

        // Column details:
        hpWriter.writeByte(schema.numColumns());
        for (ColumnInfo colInfo : schema.getColumnInfos()) {

            ColumnType colType = colInfo.getType();

            // Each column description consists of a type specification, a set
            // of flags (1 byte), and a string specifying the column's name.

            // Write the SQL data type and any associated details.

            hpWriter.writeByte(colType.getBaseType().getTypeID());

            // If this data type requires additional details, write that as well.
            if (colType.hasLength()) {
                // CHAR and VARCHAR fields have a 2 byte length value after the type.
                hpWriter.writeShort(colType.getLength());
            }

            // Write the column name.
            hpWriter.writeVarString255(colInfo.getName());
        }

        // Write all details of key constraints, foreign keys, and indexes:

        int numConstraints = schema.numCandidateKeys() + schema.numForeignKeys();
        KeyColumnIndexes pk = schema.getPrimaryKey();
        if (pk != null)
            numConstraints++;

        logger.debug("Writing " + numConstraints + " constraints");
        int constraintStartIndex = hpWriter.getPosition();
        hpWriter.writeByte(numConstraints);

        if (pk != null)
            writeKey(hpWriter, TableConstraintType.PRIMARY_KEY, pk);

        for (KeyColumnIndexes ck : schema.getCandidateKeys())
            writeKey(hpWriter, TableConstraintType.UNIQUE, ck);

        for (ForeignKeyColumnIndexes fk : schema.getForeignKeys())
            writeForeignKey(hpWriter, fk);

        logger.debug("Constraints occupy " +
            (hpWriter.getPosition() - constraintStartIndex) +
            " bytes in the schema");
        
        // Compute and store the schema's size.
        int schemaSize = hpWriter.getPosition() - HeaderPage.OFFSET_NCOLS;
        HeaderPage.setSchemaSize(headerPage, schemaSize);

        // Report how much space was used by schema info.  (It's the current
        // position minus 4 bytes, since the first 2 bytes are file-type and
        // encoded page size, and the second 2 bytes are the schema size.)
        logger.debug("Table " + tableName + " schema uses " + schemaSize +
            " bytes of the " + dbFile.getPageSize() + "-byte header page.");

        // Write in empty statistics, so that the values are at least
        // initialized to something.
        TableStats stats = new TableStats(schema.numColumns());
        tblFileInfo.setStats(stats);
        HeaderPage.setTableStats(headerPage, tblFileInfo);
    }


    /**
     * This helper function writes a primary key or candidate key to the table's
     * schema stored in the header page.
     * 
     * @param hpWriter the writer being used to write the table's schema to its
     *        header page
     *
     * @param type the constraint type, either
     *        {@link TableConstraintType#PRIMARY_KEY} or
     *        {@link TableConstraintType#FOREIGN_KEY}.
     *
     * @param key a specification of what columns appear in the key
     *            
     * @throws IllegalArgumentException if the <tt>type</tt> argument is
     *         <tt>null</tt>, or is not one of the accepted values
     */
    private void writeKey(PageWriter hpWriter, TableConstraintType type,
                          KeyColumnIndexes key) {

        if (type == TableConstraintType.PRIMARY_KEY) {
            logger.debug(String.format(" * Primary key %s, enforced with index %s",
                key, key.getIndexName()));
        }
        else if (type == TableConstraintType.UNIQUE) {
            logger.debug(String.format(" * Candidate key %s, enforced with index %s",
                key, key.getIndexName()));
        }
        else {
            throw new IllegalArgumentException(
                "Invalid TableConstraintType value " + type);
        }

        int typeVal = type.getTypeID();
        String cName = key.getConstraintName();
        if (cName != null)
            typeVal |= 0x80;

        hpWriter.writeByte(typeVal);
        if (cName != null)
            hpWriter.writeVarString255(cName);

        hpWriter.writeByte(key.size());
        for (int i = 0; i < key.size(); i++)
            hpWriter.writeByte(key.getCol(i));

        // This should always be specified.
        hpWriter.writeVarString255(key.getIndexName());
    }
    
    
    private void writeForeignKey(PageWriter hpWriter, ForeignKeyColumnIndexes key) {
        logger.debug(" * Foreign key " + key);

        int type = TableConstraintType.FOREIGN_KEY.getTypeID();
        if (key.getConstraintName() != null)
            type |= 0x80;

        hpWriter.writeByte(type);
        if (key.getConstraintName() != null)
            hpWriter.writeVarString255(key.getConstraintName());

        hpWriter.writeVarString255(key.getRefTable());
        hpWriter.writeByte(key.size());
        for (int i = 0; i < key.size(); i++) {
            hpWriter.writeByte(key.getCol(i));
            hpWriter.writeByte(key.getRefCol(i));
        }
    }
        


    /**
     * This method opens the data file corresponding to the specified table
     * name and reads in the table's schema.  If the table is already open
     * then the cached data is simply returned.
     */
    @Override
    public void loadTableInfo(TableFileInfo tblFileInfo) throws IOException {

        // Read in the table file's header page.  Wrap it in a page-reader to make
        // the input operations easier.

        String tableName = tblFileInfo.getTableName();
        DBFile dbFile = tblFileInfo.getDBFile();
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
        PageReader hpReader = new PageReader(headerPage);

        // Pick up from right after the page size.
        hpReader.setPosition(HeaderPage.OFFSET_NCOLS);

        // Read in the column descriptions.

        int numCols = hpReader.readUnsignedByte();
        logger.debug("Table has " + numCols + " columns.");

        if (numCols == 0)
            throw new IOException("Table must have at least one column.");

        TableSchema schema = tblFileInfo.getSchema();
        for (int iCol = 0; iCol < numCols; iCol++) {
            // Each column description consists of a type specification, a set
            // of flags (1 byte), and a string specifying the column's name.

            // Get the SQL data type, and begin to build the column's type
            // with that.

            byte sqlTypeID = hpReader.readByte();

            SQLDataType baseType = SQLDataType.findType(sqlTypeID);
            if (baseType == null) {
                throw new IOException("Unrecognized SQL type " + sqlTypeID +
                    " for column " + iCol);
            }

            ColumnType colType = new ColumnType(baseType);

            // If this data type requires additional details, read that as well.
            if (colType.hasLength()) {
                // CHAR and VARCHAR fields have a 2 byte length value after
                // the type.
                colType.setLength(hpReader.readUnsignedShort());
            }

            // TODO:  Read the column flags. (e.g. not-null, etc.)
            // int colFlags = hpReader.readUnsignedByte();

            // Read and verify the column name.

            String colName = hpReader.readVarString255();

            if (colName.length() == 0) {
                throw new IOException("Name of column " + iCol +
                    " is unspecified.");
            }

            for (int iCh = 0; iCh < colName.length(); iCh++) {
                char ch = colName.charAt(iCh);

                if (iCh == 0 && !(Character.isLetter(ch) || ch == '_') ||
                    iCh > 0 && !(Character.isLetterOrDigit(ch) || ch == '_')) {
                    throw new IOException(String.format("Name of column " +
                        "%d \"%s\" has an invalid character at index %d.",
                        iCol, colName, iCh));
                }
            }

            ColumnInfo colInfo = new ColumnInfo(colName, tableName, colType);

            logger.debug(colInfo);

            schema.addColumnInfo(colInfo);
        }

        // Read all details of key constraints, foreign keys, and indexes:

        int numConstraints = hpReader.readUnsignedByte();
        logger.debug("Reading " + numConstraints + " constraints");

        for (int i = 0; i < numConstraints; i++) {
            int cTypeID = hpReader.readUnsignedByte();
            TableConstraintType cType =
                TableConstraintType.findType((byte) (cTypeID & 0x7F));
            if (cType == null)
                throw new IOException("Unrecognized constraint-type value " + cTypeID);

            switch (cType) {
            case PRIMARY_KEY:
                schema.setPrimaryKey(
                    readKey(hpReader, cTypeID, TableConstraintType.PRIMARY_KEY));
                break;

            case UNIQUE:
                schema.addCandidateKey(
                    readKey(hpReader, cTypeID, TableConstraintType.UNIQUE));
                break;

            case FOREIGN_KEY:
                schema.addForeignKey(readForeignKey(hpReader, cTypeID));
                break;
            
            default:
                throw new IOException(
                    "Encountered unhandled constraint type " + cType);
            }
        }

        // Read in the table's statistics.
        tblFileInfo.setStats(HeaderPage.getTableStats(headerPage, tblFileInfo));
        logger.debug(tblFileInfo.getStats());
    }


    /**
     * This helper function writes a primary key or candidate key to the table's
     * schema stored in the header page.
     *
     * @param hpReader the writer being used to write the table's schema to its
     *        header page
     *
     * @param typeID the unsigned-byte value read from the table's header page,
     *        corresponding to this key's type.  Although this value is already
     *        parsed before calling this method, it also contains flags that
     *        this method handles, so it must be passed in as well.
     *
     * @param type the constraint type, either
     *        {@link TableConstraintType#PRIMARY_KEY} or
     *        {@link TableConstraintType#FOREIGN_KEY}.
     *
     * @return a specification of the key, including its name, what columns
     *         appear in the key, and what index is used to enforce the key
     *
     * @throws IllegalArgumentException if the <tt>type</tt> argument is
     *         <tt>null</tt>, or is not one of the accepted values
     */
    private KeyColumnIndexes readKey(PageReader hpReader, int typeID,
                                     TableConstraintType type) {

        if (type == TableConstraintType.PRIMARY_KEY) {
            logger.debug(" * Reading primary key");
        }
        else if (type == TableConstraintType.UNIQUE) {
            logger.debug(" * Reading candidate key");
        }
        else {
            throw new IllegalArgumentException(
                "Invalid TableConstraintType value " + type);
        }

        String constraintName = null;
        if ((typeID & 0x80) != 0)
            constraintName = hpReader.readVarString255();

        int keySize = hpReader.readUnsignedByte();
        int[] keyCols = new int[keySize];
        for (int i = 0; i < keySize; i++)
            keyCols[i] = hpReader.readUnsignedByte();

        // This should always be specified.
        String indexName = hpReader.readVarString255();
        
        KeyColumnIndexes key = new KeyColumnIndexes(keyCols, indexName);
        key.setConstraintName(constraintName);
        
        return key;
    }    


    private ForeignKeyColumnIndexes readForeignKey(PageReader hpReader, int typeID) {
        logger.debug(" * Reading foreign key");

        String constraintName = null;
        if ((typeID & 0x80) != 0)
            constraintName = hpReader.readVarString255();

        String refTableName = hpReader.readVarString255();
        int keySize = hpReader.readUnsignedByte();

        int[] keyCols = new int[keySize];
        int[] refCols = new int[keySize];
        for (int i = 0; i < keySize; i++) {
            keyCols[i] = hpReader.readUnsignedByte();
            refCols[i] = hpReader.readUnsignedByte();
        }

        ForeignKeyColumnIndexes fk = new ForeignKeyColumnIndexes(
            keyCols, refTableName, refCols);
        fk.setConstraintName(constraintName);

        return fk;
    }


    @Override
    public void beforeCloseTable(TableFileInfo tblFileInfo) throws IOException {
    }


    /**
     * Drops the specified table from the database.  This simply involves
     * deleting the table's data file.
     */
    @Override
    public void beforeDropTable(TableFileInfo tblFileInfo) throws IOException {
        // Do nothing.
    }


    /**
     * Returns the first tuple in this table file, or <tt>null</tt> if
     * there are no tuples in the file.
     */
    @Override
    public Tuple getFirstTuple(TableFileInfo tblFileInfo)
        throws IOException {

        if (tblFileInfo == null)
            throw new IllegalArgumentException("tblFileInfo cannot be null");

        DBFile dbFile = tblFileInfo.getDBFile();

        try {
            // Scan through the data pages until we hit the end of the table
            // file.  It may be that the first run of data pages is empty,
            // so just keep looking until we hit the end of the file.

            // Header page is page 0, so first data page is page 1.

            for (int iPage = 1; /* nothing */ ; iPage++) {
                // Look for data on this page...

                DBPage dbPage = storageManager.loadDBPage(dbFile, iPage);
                int numSlots = DataPage.getNumSlots(dbPage);
                for (int iSlot = 0; iSlot < numSlots; iSlot++) {
                    // Get the offset of the tuple in the page.  If it's 0 then
                    // the slot is empty, and we skip to the next slot.
                    int offset = DataPage.getSlotValue(dbPage, iSlot);
                    if (offset == DataPage.EMPTY_SLOT)
                        continue;

                    // This is the first tuple in the file.  Build up the
                    // HeapFilePageTuple object and return it.
                    return new HeapFilePageTuple(tblFileInfo, dbPage, iSlot,
                        offset);
                }
            }
        }
        catch (EOFException e) {
            // We ran out of pages.  No tuples in the file!
            logger.debug("No tuples in table-file " + dbFile +
                ".  Returning null.");
        }

        return null;
    }


    /**
     * Returns the tuple corresponding to the specified file pointer.  This
     * method is used by many other operations in the database, such as
     * indexes.
     *
     * @throws InvalidFilePointerException if the specified file-pointer
     *         doesn't actually point to a real tuple.
     */
    @Override
    public Tuple getTuple(TableFileInfo tblFileInfo, FilePointer fptr)
        throws InvalidFilePointerException, IOException {

        DBFile dbFile = tblFileInfo.getDBFile();
        DBPage dbPage;

        try {
            // This could throw EOFException if the page doesn't actually exist.
            dbPage = storageManager.loadDBPage(dbFile, fptr.getPageNo());
        }
        catch (EOFException eofe) {
            throw new InvalidFilePointerException("Specified page " +
                fptr.getPageNo() + " doesn't exist in file " +
                dbFile.getDataFile().getName(), eofe);
        }

        // The file-pointer points to the slot for the tuple, not the tuple itself.
        // So, we need to look up that slot's value to get to the tuple data.

        int slot;
        try {
            slot = DataPage.getSlotIndexFromOffset(dbPage, fptr.getOffset());
        }
        catch (IllegalArgumentException iae) {
            throw new InvalidFilePointerException(iae);
        }

        // Pull the tuple's offset from the specified slot, and make sure
        // there is actually a tuple there!

        int offset = DataPage.getSlotValue(dbPage, slot);
        if (offset == DataPage.EMPTY_SLOT) {
            throw new InvalidFilePointerException("Slot " + slot + " on page " +
                fptr.getPageNo() + " is empty.");
        }

        return new HeapFilePageTuple(tblFileInfo, dbPage, slot, offset);
    }


    /**
     * Returns the tuple that follows the specified tuple,
     * or <tt>null</tt> if there are no more tuples in the file.
     **/
    @Override
    public Tuple getNextTuple(TableFileInfo tblFileInfo, Tuple tup)
        throws IOException {

        /* Procedure:
         *   1)  Get slot index of current tuple.
         *   2)  If there are more slots in the current page, find the next
         *       non-empty slot.
         *   3)  If we get to the end of this page, go to the next page
         *       and try again.
         *   4)  If we get to the end of the file, we return null.
         */

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        DBPage dbPage = ptup.getDBPage();
        DBFile dbFile = dbPage.getDBFile();

        int nextSlot = ptup.getSlot() + 1;
        while (true) {
            int numSlots = DataPage.getNumSlots(dbPage);

            while (nextSlot < numSlots) {
                int nextOffset = DataPage.getSlotValue(dbPage, nextSlot);
                if (nextOffset != DataPage.EMPTY_SLOT) {
                    return new HeapFilePageTuple(tblFileInfo, dbPage, nextSlot,
                        nextOffset);
                }

                nextSlot++;
            }

            // If we got here then we reached the end of this page with no
            // tuples.  Go on to the next data-page, and start with the first
            // tuple in that page.

            try {
                dbPage = storageManager.loadDBPage(dbFile, dbPage.getPageNo() + 1);
                nextSlot = 0;
            }
            catch (EOFException e) {
                // Hit the end of the file with no more tuples.  We are done
                // scanning.
                return null;
            }
        }

        // It's pretty gross to have no return statement here, but there's
        // no way to reach this point.
    }


    /**
     * Adds the specified tuple into the table file.  A new
     * <tt>HeapFilePageTuple</tt> object corresponding to the tuple is returned.
     *
     * @review (donnie) This could be made a little more space-efficient.
     *         Right now when computing the required space, we assume that we
     *         will <em>always</em> need a new slot entry, whereas the page may
     *         contain empty slots.  (Note that we don't always create a new
     *         slot when adding a tuple; we will reuse an empty slot.  This
     *         inefficiency is simply in estimating the size required for the
     *         new tuple.)
     */
    @Override
    public Tuple addTuple(TableFileInfo tblFileInfo, Tuple tup)
        throws IOException {

        /*
         * Find out how large the new tuple will be, so we can find a page to
         * store it.
         *
         * Find a page with space for the new tuple.
         *
         * Generate the data necessary for storing the tuple into the file.
         */

        DBFile dbFile = tblFileInfo.getDBFile();

        int tupSize = PageTuple.getTupleStorageSize(
            tblFileInfo.getSchema().getColumnInfos(), tup);

        logger.debug("Adding new tuple of size " + tupSize + " bytes.");

        // Sanity check:  Make sure that the tuple would actually fit in a page
        // in the first place!
        // The "+ 2" is for the case where we need a new slot entry as well.
        if (tupSize + 2 > dbFile.getPageSize()) {
            throw new IOException(
                "Tuple size " + tupSize + " is larger than page size " +
                dbFile.getPageSize() + ".");
        }

        // Search for a page to put the tuple in.  If we hit the end of the
        // data file, create a new page.
        int pageNo = 1;
        DBPage dbPage = null;
        while (true) {
            // Try to load the page without creating a new one.
            try {
                dbPage = storageManager.loadDBPage(dbFile, pageNo);
            }
            catch (EOFException eofe) {
                // Couldn't load the current page, because it doesn't exist.
                // Break out of the loop.
                logger.debug("Reached end of data file without finding " +
                             "space for new tuple.");
                break;
            }

            int freeSpace = DataPage.getFreeSpaceInPage(dbPage);

            logger.debug(String.format("Page %d has %d bytes of free space.",
                pageNo, freeSpace));

            // If this page has enough free space to add a new tuple, break
            // out of the loop.  (The "+ 2" is for the new slot entry we will
            // also need.)
            if (freeSpace >= tupSize + 2) {
                logger.debug("Found space for new tuple in page " + pageNo + ".");
                break;
            }

            // If we reached this point then the page doesn't have enough
            // space, so go on to the next data page.
            dbPage = null;
            pageNo++;
        }

        if (dbPage == null) {
            // Try to create a new page at the end of the file.  In this
            // circumstance, pageNo is *just past* the last page in the data
            // file.
            logger.debug("Creating new page " + pageNo + " to store new tuple.");
            dbPage = storageManager.loadDBPage(dbFile, pageNo, true);
            DataPage.initNewPage(dbPage);
        }

        int slot = DataPage.allocNewTuple(dbPage, tupSize);
        int tupOffset = DataPage.getSlotValue(dbPage, slot);

        logger.debug(String.format(
            "New tuple will reside on page %d, slot %d.", pageNo, slot));

        HeapFilePageTuple pageTup = HeapFilePageTuple.storeNewTuple(tblFileInfo,
            dbPage, slot, tupOffset, tup);

        DataPage.sanityCheck(dbPage);

        return pageTup;
    }


    // Inherit interface-method documentation.
    /**
     * @review (donnie) This method will fail if a tuple is modified in a way
     *         that requires more space than is currently available in the data
     *         page.  One solution would be to move the tuple to a different
     *         page and then perform the update, but that would cause all kinds
     *         of additional issue.  So, if the page runs out of data, oh well.
     */
    @Override
    public void updateTuple(TableFileInfo tblFileInfo, Tuple tup,
                            Map<String, Object> newValues) throws IOException {

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        Schema schema = tblFileInfo.getSchema();

        for (Map.Entry<String, Object> entry : newValues.entrySet()) {
            String colName = entry.getKey();
            Object value = entry.getValue();

            int colIndex = schema.getColumnIndex(colName);
            ptup.setColumnValue(colIndex, value);
        }

        DataPage.sanityCheck(ptup.getDBPage());
    }


    // Inherit interface-method documentation.
    @Override
    public void deleteTuple(TableFileInfo tblFileInfo, Tuple tup)
        throws IOException {

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        DBPage dbPage = ptup.getDBPage();
        DataPage.deleteTuple(dbPage, ptup.getSlot());

        DataPage.sanityCheck(dbPage);
    }


    // Copy interface javadocs.
    @Override
    public void analyzeTable(TableFileInfo tblFileInfo) throws IOException {
        logger.warn("TODO:  Implement analyzeTable().");

        /* Your implementation will end with lines like these.  You must be
         * sure to update the TableFileInfo object, and then store the results
         * back to the header page.
         *
        TableStats stats = new TableStats(...);
        tblFileInfo.setStats(stats);

        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
        HeaderPage.setTableStats(headerPage, tblFileInfo);
        */
    }


    public BlockedTableReader getBlockedReader() {
        if (blockedReader == null)
            blockedReader = new BlockedHeapFileTableReader();

        return blockedReader;
    }
}
