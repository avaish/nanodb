package edu.caltech.nanodb.storage.btreeindex;


import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;


/**
 * <p>
 * This class wraps a {@link DBPage} object that is a leaf page in the
 * B<sup>+</sup> tree implementation, to provide some of the basic
 * leaf-management operations necessary for the indexing structure.
 * </p>
 * <p>
 * Operations involving individual inner-pages are provided by the
 * {@link InnerPage} wrapper-class.  Higher-level operations involving multiple
 * leaves and/or inner pages of the B<sup>+</sup> tree structure, are provided
 * by the {@link LeafPageOperations} and {@link InnerPageOperations} classes.
 * </p>
 */
public class LeafPage {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(LeafPage.class);


    /** The page type always occupies the first byte of the page. */
    public static final int OFFSET_PAGE_TYPE = 0;


    /**
     * The offset where the next-sibling page number is stored in this page.
     * The only leaf page that doesn't have a next sibling is the last leaf
     * in the index; its "next page" value will be set to 0.
     */
    public static final int OFFSET_NEXT_PAGE_NO = 1;


    /**
     * The offset where the number of key+pointer entries is stored in the page.
     */
    public static final int OFFSET_NUM_ENTRIES = 3;


    /** The offset of the first key in the leaf page. */
    public static final int OFFSET_FIRST_KEY = 5;


    /** The actual data page that holds the B<sup>+</sup> tree leaf node. */
    private DBPage dbPage;


    /**
     * Information about the index itself, such as what file it is stored in,
     * its name, the columns in the index, and so forth.
     */
    private IndexFileInfo idxFileInfo;


    /**
     * Since we require the schema of the index rather frequently, this is a
     * cached copy of the index's schema.
     */
    private List<ColumnInfo> colInfos;
    
    
    /** The number of entries (pointers + keys) stored within this leaf page. */
    private int numEntries;


    /**
     * A list of the keys stored in this leaf page.  Each key also stores the
     * file-pointer for the associated tuple, as the last value in the key.
     */
    private ArrayList<BTreeIndexPageTuple> keys;


    /**
     * The total size of all data (pointers + keys + initial values) stored
     * within this leaf page.  This is also the offset at which we can start
     * writing more data without overwriting anything.
     */
    private int endOffset;


    /**
     * Initialize the leaf-page wrapper class for the specified index page.
     * The contents of the leaf-page are cached in the fields of the wrapper
     * object.
     *
     * @param dbPage the data page from the index file to wrap
     * @param idxFileInfo the general descriptive information about the index
     */
    public LeafPage(DBPage dbPage, IndexFileInfo idxFileInfo) {
        if (dbPage.readUnsignedByte(0) != BTreeIndexManager.BTREE_LEAF_PAGE) {
            throw new IllegalArgumentException("Specified DBPage " +
                dbPage.getPageNo() + " is not marked as a leaf-page.");
        }

        this.dbPage = dbPage;
        this.idxFileInfo = idxFileInfo;
        this.colInfos = idxFileInfo.getIndexSchema();

        loadPageContents();
    }


    /**
     * This static helper function initializes a {@link DBPage} object's
     * contents with the type and detail values that will allow a new
     * {@code LeafPage} wrapper to be instantiated for the page, and then it
     * returns a wrapper object for the page.
     *
     * @param dbPage the page to initialize as a leaf-page.
     *
     * @param idxFileInfo details about the index that the leaf-page is for
     *
     * @return a newly initialized {@code LeafPage} object wrapping the page
     */
    public static LeafPage init(DBPage dbPage, IndexFileInfo idxFileInfo) {
        dbPage.writeByte(OFFSET_PAGE_TYPE, BTreeIndexManager.BTREE_LEAF_PAGE);
        dbPage.writeShort(OFFSET_NUM_ENTRIES, 0);
        dbPage.writeShort(OFFSET_NEXT_PAGE_NO, 0);

        return new LeafPage(dbPage, idxFileInfo);
    }


    /**
     * This private helper scans through the leaf page's contents and caches
     * the contents of the leaf page in a way that makes it easy to use and
     * manipulate.
     */
    private void loadPageContents() {
        numEntries = dbPage.readUnsignedShort(OFFSET_NUM_ENTRIES);
        keys = new ArrayList<BTreeIndexPageTuple>(numEntries);

        if (numEntries > 0) {
            // Handle first key separately since we know its offset.

            BTreeIndexPageTuple key =
                new BTreeIndexPageTuple(dbPage, OFFSET_FIRST_KEY, colInfos);

            keys.add(key);

            // Handle remaining keys.
            for (int i = 1; i < numEntries; i++) {
                int keyEndOffset = key.getEndOffset();
                key = new BTreeIndexPageTuple(dbPage, keyEndOffset, colInfos);
                keys.add(key);
            }

            endOffset = key.getEndOffset();
        }
        else {
            // There are no entries (pointers + keys).
            endOffset = OFFSET_FIRST_KEY;
        }
    }


    /**
     * Returns the high-level details for the index that this page is a part of.
     *
     * @return the high-level details for the index
     */
    public IndexFileInfo getIndexFileInfo() {
        return idxFileInfo;
    }


    /**
     * Returns the page-number of this leaf page.
     *
     * @return the page-number of this leaf page.
     */
    public int getPageNo() {
        return dbPage.getPageNo();
    }


    /**
     * Returns the page-number of the next leaf page in the sequence of leaf
     * pages, or 0 if this is the last leaf-page in the index.
     *
     * @return the page-number of the next leaf page in the sequence of leaf
     *         pages, or 0 if this is the last leaf-page in the index.
     */
    public int getNextPageNo() {
        return dbPage.readUnsignedShort(OFFSET_NEXT_PAGE_NO);
    }


    /**
     * Sets the page-number of the next leaf page in the sequence of leaf pages.
     *
     * @param pageNo the page-number of the next leaf-page in the index, or 0
     *        if this is the last leaf-page in the index.
     */
    public void setNextPageNo(int pageNo) {
        if (pageNo < 0) {
            throw new IllegalArgumentException(
                "pageNo must be in range [0, 65535]; got " + pageNo);
        }
        
        dbPage.writeShort(OFFSET_NEXT_PAGE_NO, pageNo);
    }


    /**
     * Returns the number of entries in this leaf-page.  Note that this count
     * does not include the pointer to the next leaf; it only includes the keys
     * and associated pointers to tuples in the table-file.
     *
     * @return the number of entries in this leaf-page.
     */
    public int getNumEntries() {
        return numEntries;
    }


    /**
     * Returns the amount of space available in this leaf page, in bytes.
     *
     * @return the amount of space available in this leaf page, in bytes.
     */
    public int getFreeSpace() {
        return dbPage.getPageSize() - endOffset;
    }


    /**
     * Returns the key at the specified index.
     *
     * @param index the index of the key to retrieve
     *
     * @return the key at that index
     */
    public BTreeIndexPageTuple getKey(int index) {
        return keys.get(index);
    }


    /**
     * Returns the size of the key at the specified index, in bytes.
     *
     * @param index the index of the key to get the size of
     *
     * @return the size of the specified key, in bytes
     */
    public int getKeySize(int index) {
        BTreeIndexPageTuple key = getKey(index);
        return key.getEndOffset() - key.getOffset();
    }


    /**
     * This method inserts a key into the leaf page, making sure to keep keys
     * in monotonically increasing order.  This method will throw an exception
     * if the leaf page already contains the specified key; this is acceptable
     * because keys include a "uniquifier" value, the file-pointer to the actual
     * tuple that the key is associated with.  Thus, if a key appears multiple
     * times in a leaf-page, the index would actually be invalid.
     *
     * @param newKey the new key to add to the leaf page
     *               
     * @throws IllegalStateException if the specified key already appears in
     *         the leaf page.
     */
    public void addEntry(TupleLiteral newKey) {
        if (newKey.getStorageSize() == -1) {
            throw new IllegalArgumentException("New key's storage size must " +
                "be computed before this method is called.");
        }

        if (getFreeSpace() < newKey.getStorageSize()) {
            throw new IllegalArgumentException(String.format(
                "Not enough space in this node to store the new key " +
                "(%d bytes free; %d bytes required)", getFreeSpace(),
                newKey.getStorageSize()));
        }

        if (numEntries == 0) {
            logger.debug("Leaf page is empty; storing new entry at start.");
            addEntryAtIndex(newKey, 0);
        }
        else {
            int i;
            for (i = 0; i < numEntries; i++) {
                BTreeIndexPageTuple key = keys.get(i);

                /* This gets REALLY verbose... */
                logger.trace(i + ":  comparing " + newKey + " to " + key);

                // Compare the tuple to the current key.  Once we find where the
                // new key/tuple should go, copy the key/pointer into the page.
                int cmp = TupleComparator.compareTuples(newKey, key);
                if (cmp < 0) {
                    logger.debug("Storing new entry at index " + i +
                        " in the leaf page.");
                    addEntryAtIndex(newKey, i);
                    break;
                }
                else if (cmp == 0) {
                    // This should NEVER happen!  Remember that every key has
                    // a uniquifier associated with it - the actual location of
                    // the associated tuple - so this should be an error.
                    throw new IllegalStateException("Key " + newKey +
                        " already appears in the index!");
                }
            }

            if (i == numEntries) {
                // The new tuple will go at the end of this page's entries.
                logger.debug("Storing new entry at end of leaf page.");
                addEntryAtIndex(newKey, numEntries);
            }
        }

        // The addEntryAtIndex() method updates the internal fields that cache
        // where keys live, etc.  So, we don't need to do that here.
    }


    /**
     * This private helper takes care of inserting an entry at a specific index
     * in the leaf page.  This method should be called with care, so as to
     * ensure that keys always remain in monotonically increasing order.
     *
     * @param newKey the new key to insert into the leaf page
     * @param index the index to insert the key at.  Any existing keys at or
     *        after this index will be shifted over to make room for the new key.
     */
    private void addEntryAtIndex(TupleLiteral newKey, int index) {
        logger.debug("Leaf-page is starting with data ending at index " +
            endOffset + ", and has " + numEntries + " entries.");

        // Get the length of the new tuple, and add in the size of the
        // file-pointer as well.
        int len = newKey.getStorageSize();
        if (len == -1) {
            throw new IllegalArgumentException("New key's storage size must " +
                "be computed before this method is called.");
        }

        logger.debug("New key's storage size is " + len + " bytes");

        int keyOffset;
        if (index < numEntries) {
            // Need to slide keys after this index over, in order to make space.

            BTreeIndexPageTuple key = getKey(index);

            // Make space for the new key/pointer to be stored, then copy in
            // the new values.

            keyOffset = key.getOffset();

            logger.debug("Moving leaf-page data in range [" + keyOffset + ", " +
                endOffset + ") over by " + len + " bytes");

            dbPage.moveDataRange(keyOffset, keyOffset + len, endOffset - keyOffset);
        }
        else {
            // The new key falls at the end of the data in the leaf index page.
            keyOffset = endOffset;
            logger.debug("New key is at end of leaf-page data; not moving anything.");
        }

        // Write the key and its associated file-pointer value into the page.
        PageTuple.storeTuple(dbPage, keyOffset, colInfos, newKey);

        // Increment the total number of entries.
        dbPage.writeShort(OFFSET_NUM_ENTRIES, numEntries + 1);

        // Reload the page contents now that we have a new key in the mix.
        // TODO:  We could do this more efficiently, but this should be
        //        sufficient for now.
        loadPageContents();

        logger.debug("Wrote new key to leaf-page at offset " + keyOffset + ".");
        logger.debug("Leaf-page is ending with data ending at index " +
            endOffset + ", and has " + numEntries + " entries.");
    }


    /**
     * This helper function moves the specified number of entries to the left
     * sibling of this leaf node.  The data is copied in one shot so that the
     * transfer will be fast, and the various associated bookkeeping values in
     * both leaves are updated.
     *
     * @param leftSibling the left sibling of this leaf-node in the index file
     *
     * @param count the number of entries (i.e. keys) to move to the left
     *        sibling
     */
    public void moveEntriesLeft(LeafPage leftSibling, int count) {
        if (leftSibling == null)
            throw new IllegalArgumentException("leftSibling cannot be null");

        if (leftSibling.getNextPageNo() != getPageNo()) {
            logger.error(String.format("Left sibling leaf %d says that page " +
                "%d is its right sibling, not this page %d",
                leftSibling.getPageNo(), leftSibling.getNextPageNo(), getPageNo()));
            
            throw new IllegalArgumentException("leftSibling " +
                leftSibling.getPageNo() + " isn't actually the left " +
                "sibling of this leaf-node " + getPageNo());
        }

        if (count < 0 || count > numEntries) {
            throw new IllegalArgumentException("count must be in range [0, " +
                numEntries + "), got " + count);
        }

        int moveEndOffset = getKey(count).getOffset();
        int len = moveEndOffset - OFFSET_FIRST_KEY;

        // Copy the range of key-data to the destination page.  Then update the
        // count of entries in the destination page.
        // Don't need to move any data in the left sibling; we are appending!
        leftSibling.dbPage.write(leftSibling.endOffset, dbPage.getPageData(),
            OFFSET_FIRST_KEY, len);             // Copy the key-data across
        leftSibling.dbPage.writeShort(OFFSET_NUM_ENTRIES,
            leftSibling.numEntries + count);    // Update the entry-count

        // Remove that range of key-data from this page.
        dbPage.moveDataRange(moveEndOffset, OFFSET_FIRST_KEY,
            endOffset - moveEndOffset);
        dbPage.writeShort(OFFSET_NUM_ENTRIES, numEntries - count);

        // Only erase the old data in the leaf page if we are trying to make
        // sure everything works properly.
        if (BTreeIndexManager.CLEAR_OLD_DATA)
            dbPage.setDataRange(endOffset - len, len, (byte) 0);

        // Update the cached info for both leaves.
        loadPageContents();
        leftSibling.loadPageContents();
    }


    /**
     * This helper function moves the specified number of entries to the right
     * sibling of this leaf node.  The data is copied in one shot so that the
     * transfer will be fast, and the various associated bookkeeping values in
     * both leaves are updated.
     *
     * @param rightSibling the right sibling of this leaf-node in the index file
     *
     * @param count the number of entries (i.e. keys) to move to the right
     *        sibling
     */
    public void moveEntriesRight(LeafPage rightSibling, int count) {
        if (rightSibling == null)
            throw new IllegalArgumentException("rightSibling cannot be null");

        if (getNextPageNo() != rightSibling.getPageNo()) {
            throw new IllegalArgumentException("rightSibling " +
                rightSibling.getPageNo() + " isn't actually the right " +
                "sibling of this leaf-node " + getPageNo());
        }
        
        if (count < 0 || count > numEntries) {
            throw new IllegalArgumentException("count must be in range [0, " +
                numEntries + "), got " + count);
        }

        int startOffset = getKey(numEntries - count).getOffset();
        int len = endOffset - startOffset;

        // Copy the range of key-data to the destination page.  Then update the
        // count of entries in the destination page.

        // Make room for the data
        rightSibling.dbPage.moveDataRange(OFFSET_FIRST_KEY,
            OFFSET_FIRST_KEY + len, rightSibling.endOffset - OFFSET_FIRST_KEY);

        // Copy the key-data across
        rightSibling.dbPage.write(OFFSET_FIRST_KEY, dbPage.getPageData(),
            startOffset, len);

        // Update the entry-count
        rightSibling.dbPage.writeShort(OFFSET_NUM_ENTRIES,
            rightSibling.numEntries + count);

        // Remove that range of key-data from this page.
        dbPage.writeShort(OFFSET_NUM_ENTRIES, numEntries - count);

        // Only erase the old data in the leaf page if we are trying to make
        // sure everything works properly.
        if (BTreeIndexManager.CLEAR_OLD_DATA)
            dbPage.setDataRange(startOffset, len, (byte) 0);
        
        // Update the cached info for both leaves.
        loadPageContents();
        rightSibling.loadPageContents();
    }
}
