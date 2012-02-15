package edu.caltech.nanodb.storage.btreeindex;


import java.util.List;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;
import org.apache.log4j.Logger;


/**
 * <p>
 * This class wraps a {@link DBPage} object that is an inner page in the
 * B<sup>+</sup> tree implementation, to provide some of the basic
 * inner-page-management operations necessary for the indexing structure.
 * </p>
 * <p>
 * Operations involving individual leaf-pages are provided by the
 * {@link LeafPage} wrapper-class.  Higher-level operations involving multiple
 * leaves and/or inner pages of the B<sup>+</sup> tree structure, are provided
 * by the {@link LeafPageOperations} and {@link InnerPageOperations} classes.
 * </p>
 */
public class InnerPage {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(InnerPage.class);


    /** The page type always occupies the first byte of the page. */
    public static final int OFFSET_PAGE_TYPE = 0;


    /**
     * The offset where the number of pointer entries is stored in the page.
     * The page will hold one less keys than pointers, since each key must be
     * sandwiched between two pointers.
     */
    public static final int OFFSET_NUM_POINTERS = 3;


    /** The offset of the first pointer in the non-leaf page. */
    public static final int OFFSET_FIRST_POINTER = 5;


    /** The actual data page that holds the B<sup>+</sup> tree leaf node. */
    private DBPage dbPage;


    /**
     * Information about the index itself, such as what file it is stored in,
     * its name, the columns in the index, and so forth.
     */
    private IndexFileInfo idxFileInfo;


    /** The number of pointers stored within this non-leaf page. */
    private int numPointers;


    /**
     * An array of the offsets where the pointers are stored in this non-leaf
     * page.  Each pointer points to another page within the index file.  There
     * is one more pointer than keys, since each key must be sandwiched between
     * two pointers.
     */
    private int[] pointerOffsets;


    /**
     * An array of the keys stored in this non-leaf page.  Each key also stores
     * the file-pointer for the associated tuple, as the last value in the key.
     */
    private BTreeIndexPageTuple[] keys;


    /**
     * The total size of all data (pointers + keys + initial values) stored
     * within this leaf page.  This is also the offset at which we can start
     * writing more data without overwriting anything.
     */
    private int endOffset;


    /**
     * Initialize the inner-page wrapper class for the specified index page.
     * The contents of the inner-page are cached in the fields of the wrapper
     * object.
     *
     * @param dbPage the data page from the index file to wrap
     * @param idxFileInfo the general descriptive information about the index
     */
    public InnerPage(DBPage dbPage, IndexFileInfo idxFileInfo) {
        this.dbPage = dbPage;
        this.idxFileInfo = idxFileInfo;

        loadPageContents();
    }


    /**
     * This static helper function initializes a {@link DBPage} object's
     * contents with the type and detail values that will allow a new
     * {@code InnerPage} wrapper to be instantiated for the page, and then it
     * returns a wrapper object for the page.  This version of the {@code init}
     * function creates an inner page that is initially empty.
     *
     * @param dbPage the page to initialize as an inner-page.
     *
     * @param idxFileInfo details about the index that the inner-page is for
     *
     * @return a newly initialized {@code InnerPage} object wrapping the page
     */
    public static InnerPage init(DBPage dbPage, IndexFileInfo idxFileInfo) {
        dbPage.writeByte(OFFSET_PAGE_TYPE, BTreeIndexManager.BTREE_INNER_PAGE);
        dbPage.writeShort(OFFSET_NUM_POINTERS, 0);
        return new InnerPage(dbPage, idxFileInfo);
    }


    /**
     * This static helper function initializes a {@link DBPage} object's
     * contents with the type and detail values that will allow a new
     * {@code InnerPage} wrapper to be instantiated for the page, and then it
     * returns a wrapper object for the page.  This version of the {@code init}
     * function creates an inner page that initially contains the specified
     * page-pointers and key value.
     *
     * @param dbPage the page to initialize as an inner-page.
     *
     * @param idxFileInfo details about the index that the inner-page is for
     *
     * @param pagePtr1 the first page-pointer to store in the inner page, to the
     *        left of {@code key1}
     *
     * @param key1 the first key to store in the inner page
     *
     * @param pagePtr2 the second page-pointer to store in the inner page, to
     *        the right of {@code key1}
     *
     * @return a newly initialized {@code InnerPage} object wrapping the page
     */
    public static InnerPage init(DBPage dbPage, IndexFileInfo idxFileInfo,
                                   int pagePtr1, Tuple key1, int pagePtr2) {

        dbPage.writeByte(OFFSET_PAGE_TYPE, BTreeIndexManager.BTREE_INNER_PAGE);

        // Write the first contents of the non-leaf page:  [ptr0, key0, ptr1]
        // Since key0 will usually be a BTreeIndexPageTuple, we have to rely on
        // the storeTuple() method to tell us where the new tuple's data ends.

        int offset = OFFSET_FIRST_POINTER;

        dbPage.writeShort(offset, pagePtr1);
        offset += 2;

        offset = PageTuple.storeTuple(dbPage, offset,
            idxFileInfo.getIndexSchema(), key1);

        dbPage.writeShort(offset, pagePtr2);

        dbPage.writeShort(OFFSET_NUM_POINTERS, 2);

        return new InnerPage(dbPage, idxFileInfo);
    }


    /**
     * This private helper scans through the inner page's contents and caches
     * the contents of the inner page in a way that makes it easy to use and
     * manipulate.
     */
    private void loadPageContents() {
        numPointers = dbPage.readUnsignedShort(OFFSET_NUM_POINTERS);
        if (numPointers > 0) {
            pointerOffsets = new int[numPointers];
            keys = new BTreeIndexPageTuple[numPointers - 1];

            List<ColumnInfo> colInfos = idxFileInfo.getIndexSchema();

            // Handle first pointer + key separately since we know their offsets

            pointerOffsets[0] = OFFSET_FIRST_POINTER;
            BTreeIndexPageTuple key = new BTreeIndexPageTuple(dbPage,
                OFFSET_FIRST_POINTER + 2, colInfos);
            keys[0] = key;

            // Handle all the pointer/key pairs.  This excludes the last
            // pointer.

            int keyEndOffset;
            for (int i = 1; i < numPointers - 1; i++) {
                // Next pointer starts where the previous key ends.
                keyEndOffset = key.getEndOffset();
                pointerOffsets[i] = keyEndOffset;
                
                // Next key starts after the next pointer.
                key = new BTreeIndexPageTuple(dbPage, keyEndOffset + 2, colInfos);
                keys[i] = key;
            }

            keyEndOffset = key.getEndOffset();
            pointerOffsets[numPointers - 1] = keyEndOffset;
            endOffset = keyEndOffset + 2;
        }
        else {
            // There are no entries (pointers + keys).
            endOffset = OFFSET_FIRST_POINTER;
            pointerOffsets = null;
            keys = null;
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
     * Returns the number of pointers currently stored in this inner page.  The
     * number of keys is always one less than the number of pointers, since
     * each key must have a pointer on both sides.
     *
     * @return the number of pointers in this inner page.
     */
    public int getNumPointers() {
        return numPointers;
    }


    /**
     * Returns the number of keys currently stored in this inner page.  The
     * number of keys is always one less than the number of pointers, since
     * each key must have a pointer on both sides.
     *
     * @return the number of keys in this inner page.
     *
     * @throws IllegalStateException if the inner page contains 0 pointers
     */
    public int getNumKeys() {
        if (numPointers < 1) {
            throw new IllegalStateException("Inner page contains no " +
                "pointers.  Number of keys is meaningless.");
        }
        
        return numPointers - 1;
    }


    /**
     * Returns the amount of space available in this inner page, in bytes.
     *
     * @return the amount of space available in this inner page, in bytes.
     */
    public int getFreeSpace() {
        return dbPage.getPageSize() - endOffset;
    }


    /**
     * Returns the pointer at the specified index.
     *
     * @param index the index of the pointer to retrieve
     *
     * @return the pointer at that index
     */
    public int getPointer(int index) {
        return dbPage.readUnsignedShort(pointerOffsets[index]);
    }


    /**
     * Returns the key at the specified index.
     *
     * @param index the index of the key to retrieve
     *
     * @return the key at that index
     */
    public BTreeIndexPageTuple getKey(int index) {
        return keys[index];
    }


    /**
     * This helper method scans the inner page for the specified page-pointer,
     * returning the index of the pointer if it is found, or -1 if the pointer
     * is not found.
     *
     * @param pointer the page-pointer to find in this inner page
     *
     * @return the index of the page-pointer if found, or -1 if not found
     */
    public int getIndexOfPointer(int pointer) {
        for (int i = 0; i < getNumPointers(); i++) {
            if (getPointer(i) == pointer)
                return i;
        }

        return -1;
    }


    public void replaceKey(int index, Tuple key) {
        int oldStart = keys[index].getOffset();
        int oldLen = keys[index].getEndOffset() - oldStart;
        
        List<ColumnInfo> colInfos = idxFileInfo.getIndexSchema();
        int newLen = PageTuple.getTupleStorageSize(colInfos, key);
        
        if (newLen != oldLen) {
            // Need to adjust the amount of space the key takes.
            
            if (endOffset + newLen - oldLen > dbPage.getPageSize()) {
                throw new IllegalArgumentException(
                    "New key-value is too large to fit in non-leaf page.");
            }

            dbPage.moveDataRange(oldStart + oldLen, oldStart + newLen,
                endOffset - oldStart - oldLen);
        }

        PageTuple.storeTuple(dbPage, oldStart, colInfos, key);

        // Reload the page contents.
        // TODO:  This is slow, but it should be fine for now.
        loadPageContents();
    }


    /**
     * This method inserts a new key and page-pointer into the inner page,
     * immediately following the page-pointer {@code pagePtr1}, which must
     * already appear within the page.  The caller is expected to have already
     * verified that the new key and page-pointer are able to fit in the page.
     *
     * @param pagePtr1 the page-pointer which should appear before the new key
     *        in the inner page.  <b>This is required to already appear within
     *        the inner page.</b>
     *
     * @param key1 the new key to add to the inner page, immediately after the
     *        {@code pagePtr1} value.
     *
     * @param pagePtr2 the new page-pointer to add to the inner page,
     *        immediately after the {@code key1} value.
     *
     * @throws IllegalArgumentException if the specified {@code pagePtr1} value
     *         cannot be found in the inner page, or if the new key and
     *         page-pointer won't fit within the space available in the page.
     */
    public void addEntry(int pagePtr1, Tuple key1, int pagePtr2) {

        if (logger.isTraceEnabled()) {
            logger.trace("Non-leaf page " + getPageNo() +
                " contents before adding entry:\n" + toFormattedString());
        }

        int i;
        for (i = 0; i < numPointers; i++) {
            if (getPointer(i) == pagePtr1)
                break;
        }
        
        logger.debug(String.format("Found page-pointer %d in index %d",
            pagePtr1, i));

        if (i == numPointers) {
            throw new IllegalArgumentException(
                "Can't find initial page-pointer " + pagePtr1 +
                " in non-leaf page " + getPageNo());
        }
        
        // Figure out where to insert the new key and value.

        int oldKeyStart;
        if (i < numPointers - 1) {
            // There's a key i associated with pointer i.  Use the key's offset,
            // since it's after the pointer.
            oldKeyStart = keys[i].getOffset();
        }
        else {
            // The pageNo1 pointer is the last pointer in the sequence.  Use
            // the end-offset of the data in the page.
            oldKeyStart = endOffset;
        }
        int len = endOffset - oldKeyStart;

        // Compute the size of the new key and pointer, and make sure they fit
        // into the page.

        List<ColumnInfo> colInfos = idxFileInfo.getIndexSchema();
        int newKeySize = PageTuple.getTupleStorageSize(colInfos, key1);
        int newEntrySize = newKeySize + 2;
        if (endOffset + newEntrySize > dbPage.getPageSize()) {
            throw new IllegalArgumentException("New key-value and " +
                "page-pointer are too large to fit in non-leaf page.");
        }

        if (len > 0) {
            // Move the data after the pageNo1 pointer to make room for
            // the new key and pointer.
            dbPage.moveDataRange(oldKeyStart, oldKeyStart + newEntrySize, len);
        }

        // Write in the new key/pointer values.
        PageTuple.storeTuple(dbPage, oldKeyStart, colInfos, key1);
        dbPage.writeShort(oldKeyStart + newKeySize, pagePtr2);

        // Finally, increment the number of pointers in the page, then reload
        // the cached data.

        dbPage.writeShort(OFFSET_NUM_POINTERS, numPointers + 1);

        loadPageContents();

        if (logger.isTraceEnabled()) {
            logger.trace("Non-leaf page " + getPageNo() +
                " contents after adding entry:\n" + toFormattedString());
        }
    }


    /**
     * <p>
     * This helper function moves the specified number of page-pointers to the
     * left sibling of this inner node.  The data is copied in one shot so that
     * the transfer will be fast, and the various associated bookkeeping values
     * in both inner pages are updated.
     * </p>
     *
     * @param leftSibling the left sibling of this inner node in the index file
     *
     * @param count the number of pointers to move to the left sibling
     *
     * @param parentKey If this inner node and the sibling already have a parent
     *        node, this is the key between the two nodes' page-pointers in the
     *        parent node.  If the two nodes don't have a parent (i.e. because
     *        an inner node is being split into two nodes and the depth of the
     *        tree is being increased) then this value will be {@code null}.
     *
     * @return the key that should go into the parent node, between the
     *         page-pointers for this node and its sibling
     *
     * @todo (Donnie) When support for deletion is added to the index
     *       implementation, we will need to support the case when the incoming
     *       {@code parentKey} is non-{@code null}, but the returned key is
     *       {@code null} because one of the two siblings' pointers will be
     *       removed.
     */
    public TupleLiteral movePointersLeft(InnerPage leftSibling, int count,
                                         Tuple parentKey) {

        if (count < 0 || count > numPointers) {
            throw new IllegalArgumentException("count must be in range [0, " +
                numPointers + "), got " + count);
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Non-leaf page " + getPageNo() +
                " contents before moving pointers left:\n" + toFormattedString());
        }

        // The parent-key can be null if we are splitting a page into two pages.
        // However, this situation is only valid if the right sibling is EMPTY.
        int parentKeyLen = 0;
        if (parentKey != null) {
            parentKeyLen = PageTuple.getTupleStorageSize(
                idxFileInfo.getIndexSchema(), parentKey);
        }
        else {
            if (leftSibling.getNumPointers() != 0) {
                throw new IllegalStateException("Cannot move pointers to " +
                    "non-empty sibling if no parent-key is specified!");
            }
        }

        /* TODO:  IMPLEMENT THE REST OF THIS METHOD.
         *
         * You can use PageTuple.storeTuple() to write a key into a DBPage.
         *
         * The DBPage.write() method is useful for copying a large chunk of
         * data from one DBPage to another.
         *
         * Your implementation also needs to properly handle the incoming
         * parent-key, and produce a new parent-key as well.
         */
        logger.error("NOT YET IMPLEMENTED:  movePointersLeft()");

        // Update the cached info for both non-leaf pages.
        loadPageContents();
        leftSibling.loadPageContents();

        if (logger.isTraceEnabled()) {
            logger.trace("Non-leaf page " + getPageNo() +
                " contents after moving pointers left:\n" + toFormattedString());

            logger.trace("Left-sibling page " + leftSibling.getPageNo() +
                " contents after moving pointers left:\n" +
                leftSibling.toFormattedString());
        }

        return null;
    }


    /**
     * <p>
     * This helper function moves the specified number of page-pointers to the
     * right sibling of this inner node.  The data is copied in one shot so that
     * the transfer will be fast, and the various associated bookkeeping values
     * in both inner pages are updated.
     * </p>
     *
     * @param rightSibling the right sibling of this inner node in the index file
     *
     * @param count the number of pointers to move to the right sibling
     *
     * @param parentKey If this inner node and the sibling already have a parent
     *        node, this is the key between the two nodes' page-pointers in the
     *        parent node.  If the two nodes don't have a parent (i.e. because
     *        an inner node is being split into two nodes and the depth of the
     *        tree is being increased) then this value will be {@code null}.
     *
     * @return the key that should go into the parent node, between the
     *         page-pointers for this node and its sibling
     *
     * @todo (Donnie) When support for deletion is added to the index
     *       implementation, we will need to support the case when the incoming
     *       {@code parentKey} is non-{@code null}, but the returned key is
     *       {@code null} because one of the two siblings' pointers will be
     *       removed.
     */
    public TupleLiteral movePointersRight(InnerPage rightSibling, int count,
                                          Tuple parentKey) {

        if (count < 0 || count > numPointers) {
            throw new IllegalArgumentException("count must be in range [0, " +
                numPointers + "), got " + count);
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Non-leaf page " + getPageNo() +
                " contents before moving pointers right:\n" + toFormattedString());
        }

        // The parent-key can be null if we are splitting a page into two pages.
        // However, this situation is only valid if the right sibling is EMPTY.
        int parentKeyLen = 0;
        if (parentKey != null) {
            parentKeyLen = PageTuple.getTupleStorageSize(
                idxFileInfo.getIndexSchema(), parentKey);
        }
        else {
            if (rightSibling.getNumPointers() != 0) {
                throw new IllegalStateException("Cannot move pointers to " +
                    "non-empty sibling if no parent-key is specified!");
            }
        }

        /* TODO:  IMPLEMENT THE REST OF THIS METHOD.
         *
         * You can use PageTuple.storeTuple() to write a key into a DBPage.
         *
         * The DBPage.write() method is useful for copying a large chunk of
         * data from one DBPage to another.
         *
         * Your implementation also needs to properly handle the incoming
         * parent-key, and produce a new parent-key as well.
         */
        logger.error("NOT YET IMPLEMENTED:  movePointersRight()");

        // Update the cached info for both non-leaf pages.
        loadPageContents();
        rightSibling.loadPageContents();

        if (logger.isTraceEnabled()) {
            logger.trace("Non-leaf page " + getPageNo() +
                " contents after moving pointers right:\n" + toFormattedString());

            logger.trace("Right-sibling page " + rightSibling.getPageNo() +
                " contents after moving pointers right:\n" +
                rightSibling.toFormattedString());
        }

        return null;
    }


    /**
     * <p>
     * This helper method creates a formatted string containing the contents of
     * the inner page, including the pointers and the intervening keys.
     * </p>
     * <p>
     * It is strongly suggested that this method should only be used for
     * trace-level output, since otherwise the output will become overwhelming.
     * </p>
     *
     * @return a formatted string containing the contents of the inner page
     */
    public String toFormattedString() {
        StringBuilder buf = new StringBuilder();

        buf.append(String.format("Inner page %d contains %d pointers%n",
            getPageNo(), numPointers));

        if (numPointers > 0) {
            for (int i = 0; i < numPointers - 1; i++) {
                buf.append(String.format("    Pointer %d = page %d%n", i,
                    getPointer(i)));
                buf.append(String.format("    Key %d = %s%n", i, getKey(i)));
            }
            buf.append(String.format("    Pointer %d = page %d%n", numPointers - 1,
                getPointer(numPointers - 1)));
        }

        return buf.toString();
    }
}
