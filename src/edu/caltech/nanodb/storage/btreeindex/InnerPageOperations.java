package edu.caltech.nanodb.storage.btreeindex;


import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class provides high-level B<sup>+</sup> tree management operations
 * performed on inner nodes.  These operations are provided here and not on the
 * {@link InnerPage} class since they sometimes involve splitting or merging
 * inner nodes, updating parent nodes, and so forth.
 */
public class InnerPageOperations {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(InnerPageOperations.class);

    private StorageManager storageManager;
    
    private BTreeIndexManager bTreeManager;
    
    
    public InnerPageOperations(BTreeIndexManager bTreeManager) {
        this.bTreeManager = bTreeManager;
        storageManager = StorageManager.getInstance();
    }
    

    public InnerPage loadPage(IndexFileInfo idxFileInfo, int pageNo)
        throws IOException {

        if (pageNo == 0)
            return null;

        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);
        return new InnerPage(dbPage, idxFileInfo);
    }


    /**
     * This helper function is used to update a key between two existing
     * pointers in an inner B<sup>+</sup> tree node.  It is an error if the
     * specified pair of pointers cannot be found in the node.
     *
     * @param page the inner page to update the key in
     * @param pagePath the path to the page, from the root node
     * @param pagePtr1 the pointer P<sub>i</sub> before the key to update
     * @param key1 the new value of the key K<sub>i</sub> to store
     * @param pagePtr2 the pointer P<sub>i+1</sub> after the key to update
     *
     * @todo (Donnie) This implementation has a major failing that will occur
     *       infrequently - if the inner page doesn't have room for the new key
     *       (e.g. if the page was already almost full, and then the new key is
     *       larger than the old key) then the inner page needs to be split,
     *       per usual.  Right now it will just throw an exception in this case.
     *       This is why the {@code pagePath} argument is provided, so that when
     *       this bug is fixed, the page-path will be available.
     */
    public void replaceKey(InnerPage page, List<Integer> pagePath,
                           int pagePtr1, Tuple key1, int pagePtr2) {

        for (int i = 0; i < page.getNumPointers() - 1; i++) {
            if (page.getPointer(i) == pagePtr1 &&
                page.getPointer(i + 1) == pagePtr2) {

                // Found the pair of pointers!  Replace the key-value.

                BTreeIndexPageTuple oldKey = page.getKey(i);
                int oldKeySize = oldKey.getSize();

                int newKeySize = PageTuple.getTupleStorageSize(
                    page.getIndexFileInfo().getIndexSchema(), key1);

                if (page.getFreeSpace() - oldKeySize + newKeySize >= 0) {
                    // We have room - go ahead and do this.
                    page.replaceKey(i, key1);
                }
                else {
                    // We need to split the inner page in this situation.
                    throw new UnsupportedOperationException(
                        "Can't replace key on inner page at index " + i +
                        ": out of space, and NanoDB doesn't know how to " +
                        " split inner pages in this situation yet.");
                }
                
                // Make sure we didn't cause any brain damage...
                assert page.getPointer(i) == pagePtr1;
                assert page.getPointer(i + 1) == pagePtr2;
                
                return;
            }
        }

        // If we got here, we have a big problem.  Couldn't find the expected
        // pair of pointers we were handed.

        // Dump the page contents because presumably we want to figure out
        // what is going on...
        logger.error(String.format(
            "Couldn't find pair of pointers %d and %d in inner page %d!",
            pagePtr1, pagePtr2, page.getPageNo()));
        logger.error("Page contents:\n" + page.toFormattedString());

        throw new IllegalStateException(
            "Couldn't find sequence of page-pointers [" + pagePtr1 + ", " +
                pagePtr2 + "] in non-leaf page " + page.getPageNo());
    }


    /**
     * This helper function determines how many pointers must be relocated from
     * one inner page to another, in order to free up the specified number of
     * bytes.  If it is possible, the number of pointers that must be relocated
     * is returned.  If it is not possible, the method returns 0.
     *
     * @param page the inner page to relocate entries from
     *
     * @param adjPage the adjacent page (predecessor or successor) to relocate
     *        entries to
     *
     * @param movingRight pass {@code true} if the sibling is to the right of
     *        {@code page} (and therefore we are moving entries right), or
     *        {@code false} if the sibling is to the left of {@code page} (and
     *        therefore we are moving entries left).
     *
     * @param bytesRequired the number of bytes that must be freed up in
     *        {@code page} by the operation
     *
     * @param parentKeySize the size of the parent key that must also be
     *        relocated into the adjacent page, and therefore affects how many
     *        pointers can be transferred
     *
     * @return the number of pointers that must be relocated to free up the
     *         required space, or 0 if it is not possible.
     */
    private int tryNonLeafRelocateForSpace(InnerPage page, InnerPage adjPage,
        boolean movingRight, int bytesRequired, int parentKeySize) {

        int numKeys = page.getNumKeys();
        int pageBytesFree = page.getFreeSpace();
        int adjBytesFree = adjPage.getFreeSpace();

        // The parent key always has to move to the adjacent page, so if that
        // won't fit, don't even try.
        if (adjBytesFree < parentKeySize)
            return 0;

        adjBytesFree -= parentKeySize;

        int keyBytesMoved = 0;
        int lastKeySize = parentKeySize;

        int numRelocated = 0;
        while (true) {
            if (adjBytesFree < keyBytesMoved + 2 * numRelocated) {
                numRelocated--;
                break;
            }

            // Figure out the index of the key we need the size of, based on the
            // direction we are moving values.  If we are moving values right,
            // we need to look at the keys starting at the rightmost one.  If we
            // are moving values left, we need to start with the leftmost key.
            int index;
            if (movingRight)
                index = numKeys - numRelocated - 1;
            else
                index = numRelocated;

            keyBytesMoved += lastKeySize;

            lastKeySize = page.getKey(index).getSize();
            logger.debug("Key " + index + " is " + lastKeySize + " bytes");

            numRelocated++;

            // Since we don't yet know which page the new pointer will go into,
            // stop when we can put the pointer in either page.
            if (pageBytesFree >= bytesRequired &&
                (adjBytesFree + keyBytesMoved + 2 * numRelocated) >= bytesRequired) {
                break;
            }
        }

        assert numRelocated >= 0;
        return numRelocated;
    }


    /**
     * This helper function adds an entry (a key and associated pointer) to
     * this inner page, after the page-pointer {@code pagePtr1}.
     *
     * @param page the inner page to add the entry to
     *
     * @param pagePath the path of page-numbers to this inner page
     *
     * @param pagePtr1 the <u>existing</u> page that the new key and next-page
     *        number will be inserted after
     *
     * @param key1 the new key-value to insert after the {@code pagePtr1} value
     *
     * @param pagePtr2 the new page-pointer value to follow the {@code key1}
     *        value
     *
     * @throws IOException if an IO error occurs while updating the index
     */
    public void addEntry(InnerPage page, List<Integer> pagePath,
        int pagePtr1, Tuple key1, int pagePtr2) throws IOException {

        // The new entry will be the key, plus 2 bytes for the page-pointer.
        List<ColumnInfo> colInfos = page.getIndexFileInfo().getIndexSchema();
        int newEntrySize = PageTuple.getTupleStorageSize(colInfos, key1) + 2;

        if (page.getFreeSpace() < newEntrySize) {
            // Try to relocate entries from this inner page to either sibling,
            // or if that can't happen, split the inner page into two.
            if (!relocatePointersAndAddKey(page, pagePath,
                pagePtr1, key1, pagePtr2, newEntrySize)) {
                splitAndAddKey(page, pagePath, pagePtr1, key1, pagePtr2);
            }
        }
        else {
            // There is room in the leaf for the new key.  Add it there.
            page.addEntry(pagePtr1, key1, pagePtr2);
        }

    }


    private boolean relocatePointersAndAddKey(InnerPage page,
        List<Integer> pagePath, int pagePtr1, Tuple key1, int pagePtr2,
        int newEntrySize) throws IOException {

        int pathSize = pagePath.size();
        if (pagePath.get(pathSize - 1) != page.getPageNo()) {
            throw new IllegalArgumentException(
                "Inner page number doesn't match last page-number in page path");
        }
        
        // See if we are able to relocate records either direction to free up
        // space for the new key.

        IndexFileInfo idxFileInfo = page.getIndexFileInfo();

        if (pathSize == 1)  // This node is also the root - no parent.
            return false;   // There aren't any siblings to relocate to.

        int parentPageNo = pagePath.get(pathSize - 2);

        InnerPage parentPage = loadPage(idxFileInfo, parentPageNo);
        int numPointers = parentPage.getNumPointers();
        int pagePtrIndex = parentPage.getIndexOfPointer(page.getPageNo());

        // Check each sibling in its own code block so that we can constrain
        // the scopes of the variables a bit.  This keeps us from accidentally
        // reusing the "prev" variables in the "next" section.

        {
            InnerPage prevPage = null;
            if (pagePtrIndex - 1 >= 0) {
                prevPage = loadPage(idxFileInfo,
                    parentPage.getPointer(pagePtrIndex - 1));
            }
            if (prevPage != null) {
                // See if we can move some of this inner node's entries to the
                // previous node, to free up space.

                BTreeIndexPageTuple parentKey = parentPage.getKey(pagePtrIndex - 1);
                int parentKeySize = parentKey.getSize();

                int count = tryNonLeafRelocateForSpace(page, prevPage, false,
                    newEntrySize, parentKeySize);

                if (count > 0) {
                    // Yes, we can do it!

                    logger.debug(String.format("Relocating %d entries from " +
                        "inner-page %d to left-sibling inner-page %d", count,
                        page.getPageNo(), pagePtr1));

                    logger.debug("Space before relocation:  Inner = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        prevPage.getFreeSpace() + " bytes");

                    TupleLiteral newParentKey =
                        page.movePointersLeft(prevPage, count, parentKey);

                    addEntryToInnerPair(prevPage, page, pagePtr1, key1, pagePtr2);

                    logger.debug("New parent-key is " + newParentKey);

                    pagePath.remove(pathSize - 1);
                    replaceKey(parentPage, pagePath, prevPage.getPageNo(),
                        newParentKey, page.getPageNo());

                    logger.debug("Space after relocation:  Inner = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        prevPage.getFreeSpace() + " bytes");

                    return true;
                }
            }
        }

        {
            InnerPage nextPage = null;
            if (pagePtrIndex + 1 < numPointers) {
                nextPage = loadPage(idxFileInfo,
                    parentPage.getPointer(pagePtrIndex + 1));
            }
            if (nextPage != null) {
                // See if we can move some of this inner node's entries to the
                // previous node, to free up space.

                BTreeIndexPageTuple parentKey = parentPage.getKey(pagePtrIndex);
                int parentKeySize = parentKey.getSize();

                int count = tryNonLeafRelocateForSpace(page, nextPage, true,
                    newEntrySize, parentKeySize);

                if (count > 0) {
                    // Yes, we can do it!

                    logger.debug(String.format("Relocating %d entries from " +
                        "inner-page %d to right-sibling inner-page %d", count,
                        page.getPageNo(), pagePtr2));

                    logger.debug("Space before relocation:  Inner = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        nextPage.getFreeSpace() + " bytes");

                    TupleLiteral newParentKey =
                        page.movePointersRight(nextPage, count, parentKey);

                    addEntryToInnerPair(page, nextPage, pagePtr1, key1, pagePtr2);

                    logger.debug("New parent-key is " + newParentKey);

                    pagePath.remove(pathSize - 1);
                    replaceKey(parentPage, pagePath, page.getPageNo(),
                        newParentKey, nextPage.getPageNo());

                    logger.debug("Space after relocation:  Inner = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        nextPage.getFreeSpace() + " bytes");

                    return true;
                }
            }
        }

        // Couldn't relocate entries to either the previous or next page.  We
        // must split the leaf into two.
        return false;
    }


    /**
     * <p>
     * This helper function splits the specified inner page into two pages,
     * also updating the parent page in the process, and then inserts the
     * specified key and page-pointer into the appropriate inner page.  This
     * method is used to add a key/pointer to an inner page that doesn't have
     * enough space, when it isn't possible to relocate pointers to the left
     * or right sibling of the page.
     * </p>
     * <p>
     * When the inner node is split, half of the pointers are put into the new
     * sibling, regardless of the size of the keys involved.  In other words,
     * this method doesn't try to keep the pages half-full based on bytes used.
     * </p>
     *
     * @param page the inner node to split and then add the key/pointer to
     *
     * @param pagePath the sequence of page-numbers traversed to reach this
     *        inner node.
     *
     * @param pagePtr1 the existing page-pointer after which the new key and
     *        pointer should be inserted
     *
     * @param key1 the new key to insert into the inner page, immediately after
     *        the page-pointer value {@code pagePtr1}.
     *
     * @param pagePtr2 the new page-pointer value to insert after the new key
     *        value
     *
     * @throws IOException if an IO error occurs during the operation.
     */
    private void splitAndAddKey(InnerPage page, List<Integer> pagePath,
        int pagePtr1, Tuple key1, int pagePtr2) throws IOException {

        int pathSize = pagePath.size();
        if (pagePath.get(pathSize - 1) != page.getPageNo()) {
            throw new IllegalArgumentException(
                "Inner page number doesn't match last page-number in page path");
        }

        logger.debug("Splitting inner-page " + page.getPageNo() +
            " into two inner pages.");

        // Get a new blank page in the index, with the same parent as the
        // inner-page we were handed.

        IndexFileInfo idxFileInfo = page.getIndexFileInfo();
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage newDBPage = bTreeManager.getNewDataPage(dbFile);
        InnerPage newPage = InnerPage.init(newDBPage, idxFileInfo);

        // Figure out how many values we want to move from the old page to the
        // new page.

        int numPointers = page.getNumPointers();

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Relocating %d pointers from left-page %d" +
                " to right-page %d", numPointers, page.getPageNo(), newPage.getPageNo()));
            logger.debug("    Old left # of pointers:  " + page.getNumPointers());
            logger.debug("    Old right # of pointers:  " + newPage.getNumPointers());
        }

        Tuple parentKey = null;
        InnerPage parentPage = null;
        
        int parentPageNo = 0;
        if (pathSize > 1)
            parentPageNo = pagePath.get(pathSize - 2);
        
        if (parentPageNo != 0) {
            parentPage = loadPage(idxFileInfo, parentPageNo);
            int parentPtrIndex = parentPage.getIndexOfPointer(page.getPageNo());
            if (parentPtrIndex < parentPage.getNumPointers() - 1)
                parentKey = parentPage.getKey(parentPtrIndex);
        }
        Tuple newParentKey =
            page.movePointersRight(newPage, numPointers / 2, parentKey);

        if (logger.isDebugEnabled()) {
            logger.debug("    New parent key:  " + newParentKey);
            logger.debug("    New left # of pointers:  " + page.getNumPointers());
            logger.debug("    New right # of pointers:  " + newPage.getNumPointers());
        }

        addEntryToInnerPair(page, newPage, pagePtr1, key1, pagePtr2);

        // If the current node doesn't have a parent, it's because it's
        // currently the root.
        if (parentPageNo == 0) {
            // Create a new root node and set both leaves to have it as their
            // parent.
            DBPage dbpParent = bTreeManager.getNewDataPage(dbFile);
            parentPage = InnerPage.init(dbpParent, idxFileInfo,
                page.getPageNo(), newParentKey, newPage.getPageNo());

            parentPageNo = parentPage.getPageNo();

            // We have a new root-page in the index!
            DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);
            HeaderPage.setRootPageNo(dbpHeader, parentPageNo);

            logger.debug("Set index root-page to inner-page " + parentPageNo);
        }
        else {
            // Add the new page into the parent non-leaf node.  (This may cause
            // the parent node's contents to be moved or split, if the parent
            // is full.)

            // (We already set the new node's parent-page-number earlier.)

            pagePath.remove(pathSize - 1);
            addEntry(parentPage, pagePath, page.getPageNo(), newParentKey,
                newPage.getPageNo());

            logger.debug("Parent page " + parentPageNo + " now has " +
                parentPage.getNumPointers() + " page-pointers.");
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Parent page contents:\n" +
                parentPage.toFormattedString());
        }
    }


    /**
     * This helper method takes a pair of inner nodes that are siblings to each
     * other, and adds the specified key to whichever node the key should go
     * into.
     *
     * @param prevPage the first page in the pair, left sibling of
     *        {@code nextPage}
     *
     * @param nextPage the second page in the pair, right sibling of
     *        {@code prevPage}
     *
     * @param pageNo1 the pointer to the left of the new key/pointer values that
     *        will be added to one of the pages
     *
     * @param key1 the new key-value to insert immediately after the existing
     *        {@code pageNo1} value
     *
     * @param pageNo2 the new pointer-value to insert immediately after the new
     *        {@code key1} value
     */
    private void addEntryToInnerPair(InnerPage prevPage, InnerPage nextPage,
                                     int pageNo1, Tuple key1, int pageNo2) {

        InnerPage page;
        
        // See if pageNo1 appears in the left page.
        int ptrIndex1 = prevPage.getIndexOfPointer(pageNo1);
        if (ptrIndex1 != -1) {
            page = prevPage;
        }
        else {
            // The pointer *should be* in the next page.  Verify this...
            page = nextPage;
            
            if (nextPage.getIndexOfPointer(pageNo1) == -1) {
                throw new IllegalStateException(String.format(
                    "Somehow lost page-pointer %d from inner pages %d and %d",
                    pageNo1, prevPage.getPageNo(), nextPage.getPageNo()));
            }
        }
        
        page.addEntry(pageNo1, key1, pageNo2);
    }
}
