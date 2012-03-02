package edu.caltech.nanodb.storage;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import edu.caltech.nanodb.transactions.TransactionManager;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.client.SessionState;
import edu.caltech.nanodb.storage.writeahead.LogSequenceNumber;


/**
 * The buffer manager reduces the number of disk IO operations by managing an
 * in-memory cache of data pages.
 *
 * @todo Add integrity checks, e.g. to make sure every cached page's file
 *       appears in the collection of cached files.
 */
public class BufferManager {

    /**
     * The system property that can be used to specify the size of the page
     * cache in the buffer manager.
     */
    public static final String PROP_PAGECACHE_SIZE = "nanodb.pagecache.size";

    /** The default page-cache size is defined to be 1MB. */
    public static final long DEFAULT_PAGECACHE_SIZE = 1024 * 1024;


    /**
     * The system property that can be used to specify the page replacement
     * policy in the buffer manager.
     */
    public static final String PROP_PAGECACHE_POLICY = "nanodb.pagecache.policy";


    /**
     * This helper class keeps track of a data page that is currently cached.
     */
    private static class CachedPageInfo {
        public DBFile dbFile;

        public int pageNo;

        public CachedPageInfo(DBFile dbFile, int pageNo) {
            if (dbFile == null)
                throw new IllegalArgumentException("dbFile cannot be null");

            this.dbFile = dbFile;
            this.pageNo = pageNo;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CachedPageInfo) {
                CachedPageInfo other = (CachedPageInfo) obj;
                return dbFile.equals(other.dbFile) && pageNo == other.pageNo;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 31 * hash + dbFile.hashCode();
            hash = 31 * hash + pageNo;
            return hash;
        }
    }


    /**
     * This helper class keeps track of a data page that is currently "pinned"
     * or in use by a client.  This prevents the page from being flushed out
     * of the cache while the client is using it.
     */
    private static class PinnedPageInfo {
        /** The session ID of the session that has this page pinned. */
        public int sessionID;

        /** The page that is pinned. */
        public DBPage dbPage;


        public PinnedPageInfo(int sessionID, DBPage dbPage) {
            this.sessionID = sessionID;
            this.dbPage = dbPage;
        }


        @Override
        public boolean equals(Object obj) {
            if (obj instanceof PinnedPageInfo) {
                PinnedPageInfo other = (PinnedPageInfo) obj;
                return sessionID == other.sessionID &&
                    dbPage.equals(other.dbPage);
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 31 * hash + sessionID;
            hash = 31 * hash + dbPage.hashCode();
            return hash;
        }
    }


    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BufferManager.class);


    private FileManager fileManager;


    /**
     * This collection holds the {@link DBFile} objects corresponding to various
     * opened files the database is currently using.
     */
    private LinkedHashMap<String, DBFile> cachedFiles;


    /**
     * This collection holds database pages (not WAL pages) that the database
     * is currently working with, so that they don't continually need to be
     * reloaded.
     */
    private LinkedHashMap<CachedPageInfo, DBPage> cachedPages;


    /**
     * This collection holds all pages that are pinned by various sessions
     * that are currently accessing the database.
     */
    private HashSet<PinnedPageInfo> pinnedPages;


    /**
     * This collection maps session IDs to the pages that each session has
     * pinned.
     */
    private HashMap<Integer, HashSet<PinnedPageInfo>> pinnedPagesBySessionID;


    /**
     * This field records how many bytes are currently cached, in total.  Note
     * that this does not currently keep track of clean copies of dirty pages.
     */
    private long totalBytesCached;


    /**
     * This field records the maximum allowed cache size.
     */
    private long maxCacheSize;


    public BufferManager(FileManager fileManager) {
        this.fileManager = fileManager;

        configureMaxCacheSize();

        cachedFiles = new LinkedHashMap<String, DBFile>();

        String replacementPolicy = configureReplacementPolicy();
        cachedPages = new LinkedHashMap<CachedPageInfo, DBPage>(16, 0.75f,
            "lru".equals(replacementPolicy));

        totalBytesCached = 0;
        
        pinnedPages = new HashSet<PinnedPageInfo>();
        pinnedPagesBySessionID = new HashMap<Integer, HashSet<PinnedPageInfo>>();
    }


    private void configureMaxCacheSize() {
        // Set the default up-front; it's just easier that way.
        maxCacheSize = DEFAULT_PAGECACHE_SIZE;

        String str = System.getProperty(PROP_PAGECACHE_SIZE);
        if (str != null) {
            str = str.trim().toLowerCase();

            long scale = 1;
            if (str.length() > 1) {
                char modifierChar = str.charAt(str.length() - 1);
                boolean removeModifier = true;
                if (modifierChar == 'k')
                    scale = 1024;
                else if (modifierChar == 'm')
                    scale = 1024 * 1024;
                else if (modifierChar == 'g')
                    scale = 1024 * 1024 * 1024;
                else
                    removeModifier = false;

                if (removeModifier)
                    str = str.substring(0, str.length() - 1);
            }

            try {
                maxCacheSize = Long.parseLong(str);
                maxCacheSize *= scale;
            }
            catch (NumberFormatException e) {
                logger.error(String.format(
                    "Could not parse page-cache size value \"%s\"; " +
                    "using default value of %d bytes",
                    System.getProperty(PROP_PAGECACHE_SIZE),
                    DEFAULT_PAGECACHE_SIZE));

                maxCacheSize = DEFAULT_PAGECACHE_SIZE;
            }
        }
    }


    private String configureReplacementPolicy() {
        String str = System.getProperty(PROP_PAGECACHE_POLICY);
        if (str != null) {
            str = str.trim().toLowerCase();

            if (!("lru".equals(str) || "fifo".equals(str))) {
                logger.error(String.format(
                    "Unrecognized value \"%s\" for page-cache replacement " +
                    "policy; using default value of LRU.",
                    System.getProperty(PROP_PAGECACHE_POLICY)));
            }
        }

        return str;
    }


    /**
     * Retrieves the specified {@link DBFile} from the buffer manager, if it has
     * already been opened.
     *
     * @param filename The filename of the database file to retrieve.  This
     *        should be ONLY the database filename, no path.  The path is
     *        expected to be relative to the database's base directory.
     *
     * @return the {@link DBFile} corresponding to the filename, if it has
     *         already been opened, or <tt>null</tt> if the file isn't currently
     *         open.
     */
    public DBFile getFile(String filename) {
        DBFile dbFile = cachedFiles.get(filename);

        logger.debug(String.format(
            "Requested file %s is%s in file-cache.",
            filename, (dbFile != null ? "" : " NOT")));

        return dbFile;
    }
    
    
    public void addFile(DBFile dbFile) {
        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        String filename = dbFile.getDataFile().getName();
        if (cachedFiles.containsKey(filename)) {
            throw new IllegalStateException(
                "File cache already contains file " + filename);
        }
        
        // NOTE:  If we want to keep a cap on how many files are opened, we
        //        would do that here.

        logger.debug(String.format( "Adding file %s to file-cache.", filename));
        
        cachedFiles.put(filename, dbFile);
    }
    
    
    public void pinPage(DBPage dbPage) {
        // Make sure this page is pinned by the session so that we don't
        // flush it until the session is done with it.
        
        int sessionID = SessionState.get().getSessionID();
        PinnedPageInfo pp = new PinnedPageInfo(sessionID, dbPage);
        
        // First, add it to the overall set of pinned pages.
        
        if (pinnedPages.add(pp)) {
            dbPage.incPinCount();
            logger.debug(String.format("Session %d is pinning page [%s,%d].  " +
                "New pin-count is %d.", sessionID, dbPage.getDBFile(),
                dbPage.getPageNo(), dbPage.getPinCount()));
        }
        
        // Next, add it to the set of pages pinned by this particular session.
        // (This makes it easier to unpin all pages used by this session.)
        
        HashSet<PinnedPageInfo> pinnedBySession =
            pinnedPagesBySessionID.get(sessionID);
        
        if (pinnedBySession == null) {
            pinnedBySession = new HashSet<PinnedPageInfo>();
            pinnedPagesBySessionID.put(sessionID, pinnedBySession);
        }

        pinnedBySession.add(pp);
    }


    public void unpinPage(DBPage dbPage) {
        // If the page is pinned by the session then unpin it.
        int sessionID = SessionState.get().getSessionID();
        PinnedPageInfo pp = new PinnedPageInfo(sessionID, dbPage);

        // First, remove it from the overall set of pinned pages.
        
        if (pinnedPages.remove(pp)) {
            dbPage.decPinCount();
            logger.debug(String.format("Session %d is unpinning page " +
                "[%s,%d].  New pin-count is %d.", sessionID, dbPage.getDBFile(),
                dbPage.getPageNo(), dbPage.getPinCount()));
        }

        // Next, remove it from the set of pages pinned by this particular
        // session.

        HashSet<PinnedPageInfo> pinnedBySession =
            pinnedPagesBySessionID.get(sessionID);
        
        if (pinnedBySession != null) {
            pinnedBySession.remove(pp);

            // If the set becomes empty, remove the hash-set for the session.
            if (pinnedBySession.isEmpty())
                pinnedPagesBySessionID.remove(sessionID);
        }
    }


    /**
     * This method unpins all pages pinned by the current session.  This is
     * generally done at the end of each transaction so that pages aren't
     * pinned forever, and can actually be evicted from the buffer manager.
     */
    public void unpinAllPages() {
        // Unpin all pages pinned by this session.
        int sessionID = SessionState.get().getSessionID();

        // Remove the set of pages pinned by this session, and save the
        // return-value so we can iterate through it and unpin each page.
        HashSet<PinnedPageInfo> pinnedBySession =
            pinnedPagesBySessionID.remove(sessionID);

        // If no pages pinned, we're done.
        if (pinnedBySession == null)
            return;

        for (PinnedPageInfo pp : pinnedBySession) {
            DBPage dbPage = pp.dbPage;

            pinnedPages.remove(pp);
            dbPage.decPinCount();
            logger.debug(String.format("Session %d is unpinning page " +
                "[%s,%d].  New pin-count is %d.", sessionID, dbPage.getDBFile(),
                dbPage.getPageNo(), dbPage.getPinCount()));
        }
    }


    public DBPage getPage(DBFile dbFile, int pageNo) {
        DBPage dbPage = cachedPages.get(new CachedPageInfo(dbFile, pageNo));

        logger.debug(String.format(
            "Requested page [%s,%d] is%s in page-cache.",
            dbFile, pageNo, (dbPage != null ? "" : " NOT")));

        if (dbPage != null) {
            // Make sure this page is pinned by the session so that we don't
            // flush it until the session is done with it.
            pinPage(dbPage);
        }

        return dbPage;
    }


    public void addPage(DBPage dbPage) throws IOException {
        if (dbPage == null)
            throw new IllegalArgumentException("dbPage cannot be null");

        DBFile dbFile = dbPage.getDBFile();
        int pageNo = dbPage.getPageNo();

        CachedPageInfo cpi = new CachedPageInfo(dbFile, pageNo);
        if (cachedPages.containsKey(cpi)) {
            throw new IllegalStateException(String.format(
                "Page cache already contains page [%s,%d]", dbFile, pageNo));
        }

        logger.debug(String.format("Adding page [%s,%d] to page-cache.",
            dbFile, pageNo));

        int pageSize = dbPage.getPageSize();
        ensureSpaceAvailable(pageSize);

        cachedPages.put(cpi, dbPage);

        // Make sure this page is pinned by the session so that we don't flush
        // it until the session is done with it.
        pinPage(dbPage);
    }


    /**
     * This helper function ensures that the buffer manager has the specified
     * amount of space available.  This is done by removing pages out of the
     * buffer manager's cache
     *
     * @param bytesRequired the amount of space that should be made available
     *        in the cache, in bytes
     *
     * @throws IOException if an IO error occurs when flushing dirty pages out
     *         to disk
     */
    private void ensureSpaceAvailable(int bytesRequired) throws IOException {
        // If we already have enough space, return without doing anything.
        if (bytesRequired + totalBytesCached <= maxCacheSize)
            return;

        // We don't currently have enough space in the cache.  Try to solve
        // this problem by evicting pages.  We collect together the pages to
        // evict, so that we can update the write-ahead log before flushing
        // the pages.

        ArrayList<DBPage> dirtyPages = new ArrayList<DBPage>();

        if (!cachedPages.isEmpty()) {
            // The cache will be too large after adding this page.

            Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
                cachedPages.entrySet().iterator();

            while (entries.hasNext() &&
                bytesRequired + totalBytesCached > maxCacheSize) {
                Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

                DBPage oldPage = entry.getValue();

                if (oldPage.isPinned())  // Can't flush pages that are in use.
                    continue;

                logger.debug(String.format(
                    "    Evicting page [%s,%d] from page-cache to make room.",
                    oldPage.getDBFile(), oldPage.getPageNo()));

                entries.remove();
                totalBytesCached -= oldPage.getPageSize();

                // If the page is dirty, we need to write its data to disk before
                // invalidating it.  Otherwise, just invalidate it.
                if (oldPage.isDirty()) {
                    logger.debug("    Evicted page is dirty; must save to disk.");
                    dirtyPages.add(oldPage);
                }
                else {
                    oldPage.invalidate();
                }
            }
        }

        // If we have any dirty data pages, they need to be flushed to disk.
        writeDirtyPages(dirtyPages, /* invalidate */ true);

        if (bytesRequired + totalBytesCached > maxCacheSize)
            logger.warn("Buffer manager is currently using too much space.");
    }


    private void writeDirtyPages(List<DBPage> dirtyPages, boolean invalidate)
        throws IOException {

        // Make sure that we follow the Write Ahead Logging Rule, by forcing
        // the WAL out to disk for the maximum page-LSN of any of these pages.
        if (!dirtyPages.isEmpty()) {
            // First, find out how much of the WAL must be forced to allow
            // these pages to be written back to disk.
            LogSequenceNumber maxLSN = null;
            for (DBPage dbPage : dirtyPages) {
                DBFileType type = dbPage.getDBFile().getType();
                if (type == DBFileType.WRITE_AHEAD_LOG_FILE ||
                    type == DBFileType.TXNSTATE_FILE) {
                    // We don't log changes to these files.
                    continue;
                }
                
                LogSequenceNumber pageLSN = dbPage.getPageLSN();
                if (maxLSN == null || pageLSN.compareTo(maxLSN) > 0)
                    maxLSN = pageLSN;
            }

            if (maxLSN != null) {
                // Force the WAL out to the specified point.
                TransactionManager txnMgr =
                    StorageManager.getInstance().getTransactionManager();
                if (txnMgr != null)
                    txnMgr.forceWAL(maxLSN);
            }

            // Finally, we can write out each dirty page.
            for (DBPage dbPage : dirtyPages) {
                fileManager.saveDBPage(dbPage);

                if (invalidate)
                    dbPage.invalidate();
            }
        }
    }


    /**
     * This method writes all dirty pages in the specified file, optionally
     * syncing the file after performing the write.  The pages are not removed
     * from the buffer manager after writing them; their dirty state is simply
     * cleared.
     *
     * @param dbFile the file whose dirty pages should be written to disk
     *
     * @param minPageNo dirty pages with a page-number less than this value
     *        will not be written to disk
     *
     * @param maxPageNo dirty pages with a page-number greater than this value
     *        will not be written to disk
     *
     * @param sync If true then the database file will be sync'd to disk;
     *        if false then no sync will occur.  The sync will always occur,
     *        in case dirty pages had previously been flushed to disk without
     *        syncing.
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or while writing the file's contents.
     */
    public void writeDBFile(DBFile dbFile, int minPageNo, int maxPageNo,
                            boolean sync) throws IOException {

        logger.info(String.format("Writing all dirty pages for file %s to disk%s.",
            dbFile, (sync ? " (with sync)" : "")));

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
            cachedPages.entrySet().iterator();

        ArrayList<DBPage> dirtyPages = new ArrayList<DBPage>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            CachedPageInfo info = entry.getKey();
            if (dbFile.equals(info.dbFile)) {
                DBPage oldPage = entry.getValue();
                if (!oldPage.isDirty())
                    continue;

                int pageNo = oldPage.getPageNo();
                if (pageNo < minPageNo || pageNo > maxPageNo)
                    continue;

                logger.debug(String.format("    Saving page [%s,%d] to disk.",
                    oldPage.getDBFile(), oldPage.getPageNo()));

                dirtyPages.add(oldPage);
            }
        }

        writeDirtyPages(dirtyPages, /* invalidate */ false);

        if (sync) {
            logger.debug("Syncing file " + dbFile);
            fileManager.syncDBFile(dbFile);
        }
    }


    /**
     * This method writes all dirty pages in the specified file, optionally
     * syncing the file after performing the write.  The pages are not removed
     * from the buffer manager after writing them; their dirty state is simply
     * cleared.
     *
     * @param dbFile the file whose dirty pages should be written to disk
     *
     * @param sync If true then the database file will be sync'd to disk;
     *        if false then no sync will occur.  The sync will always occur,
     *        in case dirty pages had previously been flushed to disk without
     *        syncing.
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or while writing the file's contents.
     */
    public void writeDBFile(DBFile dbFile, boolean sync) throws IOException {
        writeDBFile(dbFile, 0, Integer.MAX_VALUE, sync);
    }


    /**
     * This method writes all dirty pages in the buffer manager to disk.  The
     * pages are not removed from the buffer manager after writing them; their
     * dirty state is simply cleared.
     * 
     * @param sync if true, this method will sync all files in which dirty pages
     *        were found, with the exception of WAL files and the
     *        transaction-state file.  If false, no file syncing will be
     *        performed.
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or while writing the file's contents.
     */
    public void writeAll(boolean sync) throws IOException {
        logger.info("Writing ALL dirty pages in the Buffer Manager to disk.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
            cachedPages.entrySet().iterator();

        ArrayList<DBPage> dirtyPages = new ArrayList<DBPage>();
        HashSet<DBFile> dirtyFiles = new HashSet<DBFile>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            DBPage oldPage = entry.getValue();
            if (!oldPage.isDirty())
                continue;

            DBFile dbFile = oldPage.getDBFile();
            DBFileType type = dbFile.getType();
            if (type != DBFileType.WRITE_AHEAD_LOG_FILE &&
                type != DBFileType.TXNSTATE_FILE) {
                dirtyFiles.add(oldPage.getDBFile());
            }

            logger.debug(String.format("    Saving page [%s,%d] to disk.",
                dbFile, oldPage.getPageNo()));

            dirtyPages.add(oldPage);
        }

        writeDirtyPages(dirtyPages, /* invalidate */ false);
        
        if (sync) {
            logger.debug("Synchronizing all files containing dirty pages to disk.");
            for (DBFile dbFile : dirtyFiles)
                fileManager.syncDBFile(dbFile);
        }
    }

    /**
     * This method removes all cached pages in the specified file from the
     * buffer manager, writing out any dirty pages in the process.  This method
     * is not generally recommended to be used, as it basically defeats the
     * purpose of the buffer manager in the first place; rather, the
     * {@link #writeDBFile} method should be used instead.  There is a specific
     * situation in which it is used, when a file is being removed from the
     * Buffer Manager by the Storage Manager.
     *
     * @param dbFile the file whose pages should be flushed from the cache
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or the file's contents
     */
    public void flushDBFile(DBFile dbFile) throws IOException {
        logger.info("Flushing all pages for file " + dbFile +
            " from the Buffer Manager.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
            cachedPages.entrySet().iterator();

        ArrayList<DBPage> dirtyPages = new ArrayList<DBPage>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            CachedPageInfo info = entry.getKey();
            if (dbFile.equals(info.dbFile)) {
                DBPage oldPage = entry.getValue();

                logger.debug(String.format(
                    "    Evicting page [%s,%d] from page-cache.",
                    oldPage.getDBFile(), oldPage.getPageNo()));

                // Remove the page from the cache.
                entries.remove();
                totalBytesCached -= oldPage.getPageSize();

                // If the page is dirty, we need to write its data to disk before
                // invalidating it.  Otherwise, just invalidate it.
                if (oldPage.isDirty()) {
                    logger.debug("    Evicted page is dirty; must save to disk.");
                    dirtyPages.add(oldPage);
                }
                else {
                    oldPage.invalidate();
                }
            }
        }

        writeDirtyPages(dirtyPages, /* invalidate */ true);
    }


    /**
     * This method removes all cached pages from the buffer manager, writing
     * out any dirty pages in the process.  This method is not generally
     * recommended to be used, as it basically defeats the purpose of the
     * buffer manager in the first place; rather, the {@link #writeAll} method
     * should be used instead.  However, this method is useful to cause certain
     * performance issues to manifest with individual commands, and the Storage
     * Manager also uses it during shutdown processing to ensure all data is
     * saved to disk.
     *
     * @throws IOException if an IO error occurs while updating the write-ahead
     *         log, or the file's contents
     */
    public void flushAll() throws IOException {
        logger.info("Flushing ALL database pages from the Buffer Manager.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
            cachedPages.entrySet().iterator();

        ArrayList<DBPage> dirtyPages = new ArrayList<DBPage>();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            DBPage oldPage = entry.getValue();

            logger.debug(String.format(
                "    Evicting page [%s,%d] from page-cache.",
                oldPage.getDBFile(), oldPage.getPageNo()));

            // Remove the page from the cache.
            entries.remove();
            totalBytesCached -= oldPage.getPageSize();

            // If the page is dirty, we need to write its data to disk before
            // invalidating it.  Otherwise, just invalidate it.
            if (oldPage.isDirty()) {
                logger.debug("    Evicted page is dirty; must save to disk.");
                dirtyPages.add(oldPage);
            }
            else {
                oldPage.invalidate();
            }
        }

        writeDirtyPages(dirtyPages, /* invalidate */ true);
    }


    /**
     * This method removes a file from the cache, first flushing all pages from
     * the file out of the cache.  This operation is used by the Storage Manager
     * to close a data file.
     *
     * @param dbFile the file to remove from the cache.
     *
     * @throws IOException if an IO error occurs while writing out dirty pages
     */
    public void removeDBFile(DBFile dbFile) throws IOException {
        logger.info("Removing DBFile " + dbFile + " from buffer manager");
        flushDBFile(dbFile);
        cachedFiles.remove(dbFile.getDataFile().getName());
    }


    /**
     * This method removes ALL files from the cache, first flushing all pages
     * from the cache so that any dirty pages will be saved to disk (possibly
     * updating the write-ahead log in the process).  This operation is used by
     * the Storage Manager during shutdown.
     *
     * @return a list of the files that were in the cache, so that they can be
     *         used by the caller if necessary (e.g. to sync and close each one)
     *
     * @throws IOException if an IO error occurs while writing out dirty pages
     */
    public List<DBFile> removeAll() throws IOException {
        logger.info("Removing ALL DBFiles from buffer manager");

        // Flush all pages, ensuring that dirty pages will be written too.
        flushAll();

        // Get the list of DBFiles we had in the cache, then clear the cache.
        ArrayList<DBFile> dbFiles = new ArrayList<DBFile>(cachedFiles.values());
        cachedFiles.clear();

        return dbFiles;
    }
}
