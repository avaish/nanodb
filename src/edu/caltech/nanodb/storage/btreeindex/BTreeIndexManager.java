package edu.caltech.nanodb.storage.btreeindex;


import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.indexes.OrderedIndexManager;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleLiteral;

import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.indexes.IndexInfo;
import edu.caltech.nanodb.indexes.IndexManager;

import edu.caltech.nanodb.relations.ColumnIndexes;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.TableConstraintType;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * <p>
 * This is the class that manages B<sup>+</sup> tree indexes.  These indexes are
 * used for enforcing primary, candidate and foreign keys, and also for
 * providing optimized access to tuples with specific values.
 * </p>
 * <p>
 * Here is a brief overview of the NanoDB B<sup>+</sup> tree file format:
 * </p>
 * <ul>
 * <li>Page 0 is always a header page, and specifies the entry-points in the
 *     hierarchy:  the root page of the tree, and the first and last leaves of
 *     the tree.  Page 0 also maintains a list of empty pages in the tree, so
 *     that adding new nodes to the tree is fast.  (See the {@link HeaderPage}
 *     class for details.)</li>
 * <li>The remaining pages are either leaf nodes, inner nodes, or empty nodes.
 *     The first byte of the page always indicates the kind of node.  For
 *     details about the internal structure of leaf and inner nodes, see the
 *     {@link InnerPage} and {@link LeafPage} classes.</li>
 * <li>Empty nodes are formed into a simple singly linked list.  Each empty
 *     node holds a page-pointer to the next empty node in the sequence, using
 *     an unsigned short stored at index 1 (after the page-type value in index
 *     0).  The final empty page stores 0 as its next-page pointer value.</li>
 * </ul>
 * <p>
 * This index implementation always adds a uniquifier to tuples being stored in
 * the index; specifically, the file-pointer of the tuple being stored into the
 * index.  This file-pointer also allows the referenced tuple to be retrieved
 * from the table via the index when needed.  The tuple's file-pointer is always
 * appended to the key-value being stored, so the last column is always the
 * file-pointer to the tuple.
 * </p>
 */
public class BTreeIndexManager implements IndexManager {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BTreeIndexManager.class);


    /**
     * This value is stored in a B-tree page's byte 0, to indicate that the page
     * is an inner (i.e. non-leaf) page.
     */
    public static final int BTREE_INNER_PAGE = 1;


    /**
     * This value is stored in a B-tree page's byte 0, to indicate that the page
     * is a leaf page.
     */
    public static final int BTREE_LEAF_PAGE = 2;


    /**
     * This value is stored in a B-tree page's byte 0, to indicate that the page
     * is empty.
     */
    public static final int BTREE_EMPTY_PAGE = 2;


    /**
     * If this flag is set to true, all data in data-pages that is no longer
     * necessary is cleared.  This will increase the cost of write-ahead
     * logging, but it also exposes bugs more quickly because old data won't be
     * around.
     */
    public static final boolean CLEAR_OLD_DATA = true;


    /**
     * The table manager uses the storage manager a lot, so it caches a reference
     * to the singleton instance of the storage manager at initialization.
     */
    private StorageManager storageManager;


    /**
     * A helper class that manages the larger-scale operations involving leaf
     * nodes of the B+ tree.
     */
    private LeafPageOperations leafPageOps;


    /**
     * A helper class that manages the larger-scale operations involving inner
     * nodes of the B+ tree.
     */
    private InnerPageOperations innerPageOps;


    /**
     * Initializes the heap-file table manager.  This class shouldn't be
     * initialized directly, since the storage manager will initialize it when
     * necessary.
     *
     * @param storageManager the storage manager that is using this table manager
     *
     * @throws IllegalArgumentException if <tt>storageManager</tt> is <tt>null</tt>
     */
    public BTreeIndexManager(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.storageManager = storageManager;

        innerPageOps = new InnerPageOperations(this);
        leafPageOps = new LeafPageOperations(this, innerPageOps);
    }


    /**
     * This helper function generates the prefix of a name for an index with no
     * actual name specified.  Since indexes and other constraints don't
     * necessarily require names to be specified, we need some way to generate
     * these names.
     *
     * @param idxFileInfo the information describing the index to be named
     *
     * @return a string containing a prefix to use for naming the index.
     */
    public String getUnnamedIndexPrefix(IndexFileInfo idxFileInfo) {
        // Generate a prefix based on the contents of the IndexFileInfo object.
        IndexInfo info = idxFileInfo.getIndexInfo();
        TableConstraintType constraintType = info.getConstraintType();

        if (constraintType == null)
            return "IDX_" + idxFileInfo.getTableName();

        switch (info.getConstraintType()) {
            case PRIMARY_KEY:
                return "PK_" + idxFileInfo.getTableName();

            case UNIQUE:
                return "CK_" + idxFileInfo.getTableName();

            default:
                throw new IllegalArgumentException("Unrecognized constraint type " +
                    constraintType);
        }
    }


    // Copy interface javadocs.
    @Override
    public void initIndexInfo(IndexFileInfo idxFileInfo) throws IOException {
        String indexName = idxFileInfo.getIndexName();
        String tableName = idxFileInfo.getTableName();
        DBFile dbFile = idxFileInfo.getDBFile();

        //Schema schema = idxFileInfo.getSchema();

        logger.info(String.format(
            "Initializing new index %s on table %s, stored at %s", indexName,
            tableName, dbFile));

        // The index's header page just stores details of the indexing structure
        // itself, since the the actual schema information and other index
        // details are stored in the referenced table.

        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
        HeaderPage.setRootPageNo(headerPage, 0);
        HeaderPage.setFirstLeafPageNo(headerPage, 0);
        HeaderPage.setFirstEmptyPageNo(headerPage, 0);
    }


    /**
     * This method reads in the schema and other critical information for the
     * specified table.
     *
     * @throws IOException if an IO error occurs when attempting to load the
     *         table's schema and other details.
     */
    public void loadIndexInfo(IndexFileInfo idxFileInfo) throws IOException {
        // For now, we don't need to do anything in this method.
    }


    @Override
    public void addTuple(IndexFileInfo idxFileInfo, PageTuple tup)
        throws IOException {

        // These are the values we store into the index for the tuple:  the key,
        // and a file-pointer to the tuple that the key is for.
        TupleLiteral newTupleKey = makeStoredKeyValue(idxFileInfo, tup);

        logger.debug("Adding search-key value " + newTupleKey + " to index " +
            idxFileInfo.getIndexName());

        // Navigate to the leaf-page, creating one if the index is currently
        // empty.
        ArrayList<Integer> pagePath = new ArrayList<Integer>();
        LeafPage leaf =
            navigateToLeafPage(idxFileInfo, newTupleKey, true, pagePath);

        leafPageOps.addEntry(leaf, newTupleKey, pagePath);
    }


    @Override
    public void deleteTuple(IndexFileInfo idxFileInfo, PageTuple tup)
        throws IOException {
        // TODO:  IMPLEMENT
    }


    @Override
    public List<String> verifyIndex(IndexFileInfo idxFileInfo) throws IOException {
        BTreeIndexVerifier verifier = new BTreeIndexVerifier(idxFileInfo);
        List<String> errors = verifier.verify();

        return errors;
    }


    /**
     * This helper method performs the common task of navigating from the root
     * of the B<sup>+</sup> tree down to the appropriate leaf node, based on
     * the search-key provided by the caller.  Note that this method does not
     * determine whether the search-key actually exists.  Rather, it simply
     * navigates to the leaf in the index where the search-key would appear, if
     * it indeed appears in the index.
     *
     * @param idxFileInfo details of the index that is being navigated
     *
     * @param searchKey the search-key being used to navigate the B<sup>+</sup>
     *        tree structure
     *
     * @param createIfNeeded If the B<sup>+</sup> tree is currently empty (i.e.
     *        not even containing leaf pages) then this argument can be used to
     *        create a new leaf page where the search-key can be stored.  This
     *        allows the method to be used for adding tuples to the index.
     *
     * @param pagePath If this optional argument is specified, then the method
     *        stores the sequence of page-numbers it visits as it navigates
     *        from root to leaf.  If {@code null} is passed then nothing is
     *        stored as the method traverses the index structure.
     *
     * @return the leaf-page where the search-key would appear, or {@code null}
     *         if the index is currently empty and {@code createIfNeeded} is
     *         {@code false}.
     *
     * @throws IOException if an IO error occurs while navigating the index
     *         structure
     */
    private LeafPage navigateToLeafPage(IndexFileInfo idxFileInfo,
        TupleLiteral searchKey, boolean createIfNeeded,
        List<Integer> pagePath) throws IOException {

        String indexName = idxFileInfo.getIndexName();

        // The header page tells us where the root page starts.
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);

        // Get the root page of the index.
        int rootPageNo = HeaderPage.getRootPageNo(dbpHeader);
        DBPage dbpRoot;
        if (rootPageNo == 0) {
            // The index doesn't have any data-pages at all yet.  Create one if
            // the caller wants it.

            if (!createIfNeeded)
                return null;

            // We need to create a brand new leaf page and make it the root.

            logger.debug("Index " + indexName + " currently has no data " +
                "pages; finding/creating one to use as the root!");

            dbpRoot = getNewDataPage(dbFile);
            rootPageNo = dbpRoot.getPageNo();

            HeaderPage.setRootPageNo(dbpHeader, rootPageNo);
            HeaderPage.setFirstLeafPageNo(dbpHeader, rootPageNo);

            dbpRoot.writeByte(0, BTREE_LEAF_PAGE);
            LeafPage.init(dbpRoot, idxFileInfo);

            logger.debug("New root pageNo is " + rootPageNo);
        }
        else {
            // The index has a root page; load it.
            dbpRoot = storageManager.loadDBPage(dbFile, rootPageNo);

            logger.debug("Index " + idxFileInfo.getIndexName() +
                " root pageNo is " + rootPageNo);
        }

        // Next, descend down the index's structure until we find the proper
        // leaf-page based on the key value(s).

        DBPage dbPage = dbpRoot;
        int pageType = dbPage.readByte(0);
        if (pageType != BTREE_INNER_PAGE && pageType != BTREE_LEAF_PAGE)
            throw new IOException("Invalid page type encountered:  " + pageType);

        while (pageType != BTREE_LEAF_PAGE)
        {
            if (pagePath != null) {
                pagePath.add(dbPage.getPageNo());
            }
            
            // Insert new leaf node into 'linked list'.
            InnerPage innerPage = new InnerPage(dbPage, idxFileInfo);
            int next_pointer = innerPage.getNumPointers() - 1;
            
            // Determine which pointer to follow.
            for (int i = 0; i < innerPage.getNumKeys(); i++) {
                int compare = TupleComparator.comparePartialTuples(searchKey, 
                    innerPage.getKey(i));
                
                if (compare == 0) {
                    next_pointer = i + 1;
                    break;
                }
                else if (compare < 0) {
                    next_pointer = i;
                    break;
                }
            }
            
            // Load the next page.
            dbPage = storageManager.loadDBPage(dbFile, 
                innerPage.getPointer(next_pointer));
            pageType = dbPage.readByte(0);
            if (pageType != BTREE_INNER_PAGE && pageType != BTREE_LEAF_PAGE)
                throw new IOException("Invalid page type encountered:  " + pageType);
        }
        
        if (pagePath != null) {
            pagePath.add(dbPage.getPageNo());
        }
        
        // Return the leaf page (first check if it's a leaf!)
        assert(pageType == BTREE_LEAF_PAGE);
        return new LeafPage(dbPage, idxFileInfo);
    }


    /**
     * This helper function finds and returns a new data page, either by taking
     * it from the empty-pages list in the index file, or if this list is empty,
     * creating a brand new page at the end of the file.
     *
     * @param dbFile the index file to get a new empty data page from
     *
     * @return an empty {@code DBPage} that can be used as a new index page.
     *
     * @throws IOException if an error occurs while loading a data page, or
     *         while extending the size of the index file.
     */
    public DBPage getNewDataPage(DBFile dbFile) throws IOException {

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);

        DBPage newPage;
        int pageNo = HeaderPage.getFirstEmptyPageNo(dbpHeader);

        if (pageNo == 0) {
            // There are no empty pages.  Create a new page to use.

            logger.debug("No empty pages.  Extending index file " + dbFile +
                " by one page.");

            int numPages = dbFile.getNumPages();
            newPage = storageManager.loadDBPage(dbFile, numPages, true);
        }
        else {
            // Load the empty page, and remove it from the chain of empty pages.

            logger.debug("First empty page number is " + pageNo);

            newPage = storageManager.loadDBPage(dbFile, pageNo);
            int nextEmptyPage = newPage.readUnsignedShort(1);
            HeaderPage.setFirstEmptyPageNo(dbpHeader, nextEmptyPage);
        }

        logger.debug("Found new data page for the index:  page " +
            newPage.getPageNo());

        return newPage;
    }


    /**
     * This helper function marks a data page in the index as "empty", and adds
     * it to the list of empty pages in the index file.
     *
     * @param dbPage the data-page that is no longer used.
     *
     * @throws IOException if an IO error occurs while releasing the data page,
     *         such as not being able to load the header page.
     */
    public void releaseDataPage(DBPage dbPage) throws IOException {
        // TODO:  If this page is the last page of the index file, we could
        //        truncate pages off the end until we hit a non-empty page.
        //        Instead, we'll leave all the pages around forever...

        DBFile dbFile = dbPage.getDBFile();
        
        // Record in the page that it is empty.
        dbPage.writeByte(0, BTREE_EMPTY_PAGE);

        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);

        // Retrieve the old "first empty page" value, and store it in this page.
        int prevEmptyPageNo = HeaderPage.getFirstEmptyPageNo(dbpHeader);
        dbPage.writeShort(1, prevEmptyPageNo);

        if (CLEAR_OLD_DATA) {
            // Clear out the remainder of the data-page since it's now unused.
            dbPage.setDataRange(3, dbPage.getPageSize() - 3, (byte) 0);
        }

        // Store the new "first empty page" value into the header.
        HeaderPage.setFirstEmptyPageNo(dbpHeader, dbPage.getPageNo());
    }


    /**
     * This helper function creates a {@link TupleLiteral} that holds the
     * key-values necessary for storing or deleting the specified table-tuple
     * in the index.  Specifically, this method stores the tuple's file-pointer
     * in the key as the last value.
     *
     * @param idxFileInfo the details of the index to create the key for
     *
     * @param ptup the tuple from the original table, that the key will be
     *        created from.
     *
     * @return a tuple-literal that can be used for storing, looking up, or
     *         deleting the specific tuple {@code ptup}.
     */
    private TupleLiteral makeStoredKeyValue(IndexFileInfo idxFileInfo,
                                                 PageTuple ptup) {

        // Figure out what columns from the table we use for the index keys.
        ColumnIndexes colIndexes = idxFileInfo.getTableColumnIndexes();
        
        // Build up a new tuple-literal containing the new key to be inserted.
        TupleLiteral newKeyVal = new TupleLiteral();
        for (int i = 0; i < colIndexes.size(); i++)
            newKeyVal.addValue(ptup.getColumnValue(colIndexes.getCol(i)));

        // Include the file-pointer as the last value in the tuple, so that all
        // key-values are unique in the index.
        newKeyVal.addValue(ptup.getExternalReference());

        List<ColumnInfo> colInfos = idxFileInfo.getIndexSchema();
        int storageSize = PageTuple.getTupleStorageSize(colInfos, newKeyVal);
        newKeyVal.setStorageSize(storageSize);

        return newKeyVal;
    }
}
