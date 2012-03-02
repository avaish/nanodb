package edu.caltech.nanodb.storage;


import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.indexes.IndexManager;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.server.EventDispatcher;
import edu.caltech.nanodb.storage.btreeindex.BTreeIndexManager;
import edu.caltech.nanodb.storage.heapfile.HeapFileTableManager;
import edu.caltech.nanodb.transactions.TransactionManager;


/**
 *
 *
 * @todo This class requires synchronization, once we support multiple clients.
 */
public class StorageManager {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(StorageManager.class);


    /*========================================================================
     * STATIC FIELDS AND METHODS
     */


    /**
     * The system property that can be used to specify the base-directory path
     * for the storage manager to use.
     */
    public static final String PROP_BASEDIR = "nanodb.basedir";


    /**
     * The system property that can be used to specify the default page-size
     * to use when creating new database files.
     */
    public static final String PROP_PAGESIZE = "nanodb.pagesize";


    /**
     * The default base-directory path used by the storage manager.  This
     * value is set to "<tt>./datafiles</tt>" (or "<tt>.\datafiles</tt>"
     * if on Windows).
     */
    public static final String DEFAULT_BASEDIR_PATH =
      "." + File.separator + "datafiles";


    /** This is the singleton instance of the storage manager. */
    private static StorageManager storageMgr = null;


    /**
     * This method initializes the singleton instance of the storage manager.
     * It should only be called once, at database startup.
     *
     * @throws IllegalStateException if <tt>init()</tt> has already been called
     * @throws IOException if the storage manager cannot access the data
     *         directory for some reason
     */
    public static void init() throws IOException {
        if (storageMgr != null) {
            throw new IllegalStateException(
                "Storage manager is already initialized.");
        }

        String baseDirPath = System.getProperty(PROP_BASEDIR, DEFAULT_BASEDIR_PATH);
        File baseDir = new File(baseDirPath);

        storageMgr = new StorageManager(baseDir);
        storageMgr.finishInit();

        // Register the component that manages indexes when tables are modified.
        EventDispatcher.getInstance().addRowEventListener(
            new IndexUpdater(storageMgr));
    }


    /**
     * Returns the singleton instance of the storage manager.
     *
     * @return the singleton instance of the storage manager
     */
    public static StorageManager getInstance() {
        if (storageMgr == null) {
            throw new IllegalStateException(
                "StorageManager has not been initialized");
        }

        return storageMgr;
    }


    /**
     * This method shuts down the singleton instance of the storage manager.
     * It should only be called once, at database shutdown.
     *
     * @throws IllegalStateException if <tt>init()</tt> has not been called
     * @throws IOException if the storage manager cannot save all data for some
     *         reason
     */
    public static void shutdown() throws IOException {
        if (storageMgr == null) {
            throw new IllegalStateException(
                "Storage manager is not initialized.");
        }

        storageMgr.shutdownStorage();
        storageMgr = null;
    }


    /**
     * Returns the current page size to use for new database files.  If the
     * <tt>nanodb.pagesize</tt> system property is a valid page size then this
     * value is used.  Otherwise, the {@link DBFile#DEFAULT_PAGESIZE} value is
     * used.
     *
     * @return the current page size to use for new database files
     */
    public static int getCurrentPageSize() {
        // Use the default page size if no property value is specified.
        int pageSize = DBFile.DEFAULT_PAGESIZE;

        String pageSizeStr = System.getProperty(PROP_PAGESIZE);
        if (pageSizeStr != null) {
            try {
                pageSize = Integer.parseInt(pageSizeStr);
            }
            catch (NumberFormatException nfe) {
                logger.warn("Current value of " + PROP_PAGESIZE +
                    " property is not an integer:  \"" + pageSizeStr + "\"");
            }

            if (!DBFile.isValidPageSize(pageSize)) {
                logger.warn("Current value of " + PROP_PAGESIZE +
                    " property is not a valid page size:  " + pageSize);

                pageSize = DBFile.DEFAULT_PAGESIZE;
            }
        }

        return pageSize;
    }


    /*========================================================================
     * NON-STATIC FIELDS AND METHODS
     */


    /** The base directory, in which all database files are stored. */
    private File baseDir;


    /** The buffer manager stores data pages in memory, to avoid disk IOs. */
    private BufferManager bufferManager;


    /**
     * The file manager performs basic operations against the filesystem,
     * without performing any buffering whatsoever.
     */
    private FileManager fileManager;


    /**
     * If transactions are enabled, this will be the singleton transaction
     * manager instance; otherwise, it will be {@code null}.
     */
    private TransactionManager transactionManager;


    /**
     * This mapping is used to keep track of the manager objects used for each
     * file-type we need to operate on.
     */
    private HashMap<DBFileType, Object> fileTypeManagers =
        new HashMap<DBFileType, Object>();


    /**
     * An internal cache of what tables are currently open in the database.
     * This keeps us from having to reload table schemas every time someone
     * wants to access a table, and it also allows us to know what tables need
     * to be closed.
     */
    private HashMap<String, TableFileInfo> openTables =
        new HashMap<String, TableFileInfo>();


    /**
     * An internal cache of what indexes are currently open in the database.
     * This keeps us from having to reload index details every time someone
     * wants to access an index, and it also allows us to know what indexes
     * need to be closed.
     */
    private HashMap<String, IndexFileInfo> openIndexes =
        new HashMap<String, IndexFileInfo>();


    /**
     * The constructor initalizes the storage manager based on the passed-in
     * arguments.  It is private because we only want a singleton instance of
     * the storage manager, and this constructor is invoked from the static
     * {@link #init} method.
     *
     * @param baseDir the directory containing the database's files
     *
     * @throws IOException if the directory cannot be accessed
     */
    private StorageManager(File baseDir) throws IOException {
        // Make sure the base directory exists and is valid and all that.

        if (!baseDir.exists()) {
            logger.info("Base directory " + baseDir + " doesn't exist; creating.");
            if (!baseDir.mkdirs()) {
                throw new IOException("Couldn't create base directory " + baseDir);
            }
        }

        if (!baseDir.isDirectory()) {
            throw new IOException("Base-directory path " + baseDir +
                " doesn't refer to a directory.");
        }

        logger.info("Using base directory " + baseDir);
        this.baseDir = baseDir;

        fileManager = new FileManager(baseDir);
        bufferManager = new BufferManager(fileManager);
    }


    public void finishInit() throws IOException {
        if (TransactionManager.isEnabled()) {
            logger.info("Initializing transaction manager.");
            transactionManager = new TransactionManager(this, bufferManager);

            // This method opens the transaction-state file, performs any
            // necessary recovery operations, and so forth.
            transactionManager.initialize();
        }

        fileTypeManagers.put(DBFileType.HEAP_DATA_FILE,
            new HeapFileTableManager(this));

        fileTypeManagers.put(DBFileType.BTREE_INDEX_FILE,
            new BTreeIndexManager(this));
    }


    public void addFileTypeManager(DBFileType type, Object manager) {
        fileTypeManagers.put(type, manager);
    }


    private void shutdownStorage() throws IOException {
        transactionManager.forceWAL();

        List<DBFile> dbFiles = bufferManager.removeAll();
        for (DBFile dbFile : dbFiles)
            fileManager.closeDBFile(dbFile);
    }


    /**
     * Returns the base directory where all database files are stored.
     *
     * @return the base directory where all database files are stored
     */
    public File getBaseDir() {
        return baseDir;
    }


    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    
    public DBFile createDBFile(String filename, DBFileType type)
        throws IOException {

        if (bufferManager.getFile(filename) != null) {
            throw new IllegalStateException("A file " + filename +
                " is already cached in the Buffer Manager!  Does it already exist?");
        }

        DBFile dbFile = fileManager.createDBFile(filename, type,
            getCurrentPageSize());

        bufferManager.addFile(dbFile);

        return dbFile;
    }


    public DBFile openDBFile(String filename) throws IOException {
        DBFile dbFile = bufferManager.getFile(filename);
        if (dbFile == null) {
            dbFile = fileManager.openDBFile(filename);
            bufferManager.addFile(dbFile);
        }

        return dbFile;
    }
    
    
    private void closeDBFile(DBFile dbFile) throws IOException {
        bufferManager.removeDBFile(dbFile);
        fileManager.closeDBFile(dbFile);
    }


    /**
     * Returns the table-manager for the specified file type, initializing a new
     * manager if one has not already been created.
     *
     * @param type the database file type to get the table manager for.
     *
     * @return the table-manager instance for the specified file type
     *
     * @throws IllegalArgumentException if the file-type is <tt>null</tt>, or if
     *         the file-type is currently unsupported.
     */
    private TableManager getTableManager(DBFileType type) {
        if (type == null)
            throw new IllegalArgumentException("type cannot be null");

        Object manager = fileTypeManagers.get(type);
        if (manager == null) {
            throw new IllegalArgumentException(
                "Unsupported table-file type:  " + type);
        }
        else if (!(manager instanceof TableManager)) {
            throw new IllegalArgumentException("File type " + type +
                " isn't a table-file type.");
        }

        return (TableManager) manager;
    }


    /**
     * Returns the index-manager for the specified file type, initializing a new
     * manager if one has not already been created.
     *
     * @param type the database file type to get the index manager for.
     *
     * @return the index-manager instance for the specified file type
     *
     * @throws IllegalArgumentException if the file-type is <tt>null</tt>, or if
     *         the file-type is currently unsupported.
     */
    private IndexManager getIndexManager(DBFileType type) {
        if (type == null)
            throw new IllegalArgumentException("type cannot be null");

        Object manager = fileTypeManagers.get(type);
        if (manager == null) {
            throw new IllegalArgumentException(
                "Unsupported index-file type:  " + type);
        }
        else if (!(manager instanceof IndexManager)) {
            throw new IllegalArgumentException("File type " + type +
                " isn't an index-file type.");
        }

        return (IndexManager) manager;
    }


    /**
     * This method returns a database page to use, retrieving it from the buffer
     * manager if it is already loaded, or reading it from the specified data
     * file if it is not already loaded.  If the page must be loaded from the
     * file, it will be added to the buffer manager.  This operation may cause
     * other database pages to be evicted from the buffer manager, and written
     * back to disk if the evicted pages are dirty.
     * <p>
     * The <tt>create</tt> flag controls whether an error is propagated, if the
     * requested page is past the current end of the data file.  (Note that if a
     * new page is created, the file's size will not reflect the new page until
     * it is actually written to the file.)
     *
     * @param dbFile the database file to load the page from
     * @param pageNo the number of the page to load
     * @param create a flag specifying whether the page should be created if it
     *        doesn't already exist
     *
     * @return the database page, either from cache or from the data file
     *
     * @throws IllegalArgumentException if the page number is negative
     *
     * @throws java.io.EOFException if the requested page is not in the data file,
     *         and the <tt>create</tt> flag is set to <tt>false</tt>.
     */
    public DBPage loadDBPage(DBFile dbFile, int pageNo, boolean create)
        throws IOException {

        // Try to retrieve from the buffer manager.
        DBPage dbPage = bufferManager.getPage(dbFile, pageNo);
        if (dbPage == null) {
            // Buffer manager didn't have it.  Read direct from the file, then
            // add it to the buffer manager.
            dbPage = fileManager.loadDBPage(dbFile, pageNo, create);
            bufferManager.addPage(dbPage);
        }

        return dbPage;
    }


    /**
     * This method returns a database page to use, retrieving it from the buffer
     * manager if it is already loaded, or reading it from the specified data
     * file if it is not already loaded.  If the page must be loaded from the
     * file, it will be added to the buffer manager.  This operation may cause
     * other database pages to be evicted from the buffer manager, and written
     * back to disk if the evicted pages are dirty.
     * <p>
     * (This method is simply a wrapper of
     * {@link #loadDBPage(DBFile, int, boolean)}, passing <tt>false</tt> for
     * <tt>create</tt>.)
     *
     * @param dbFile the database file to load the page from
     * @param pageNo the number of the page to load
     *
     * @return the database page, either from cache or from the data file
     *
     * @throws IllegalArgumentException if the page number is negative
     *
     * @throws java.io.EOFException if the requested page is not in the data file,
     *         and the <tt>create</tt> flag is set to <tt>false</tt>.
     */
    public DBPage loadDBPage(DBFile dbFile, int pageNo) throws IOException {
        return loadDBPage(dbFile, pageNo, false);
    }


    public void releaseDBPage(DBPage dbPage) throws IOException {
        // If the page is dirty, record its changes to the write-ahead log.
        if (transactionManager != null)
            transactionManager.recordPageUpdate(dbPage);

        // Finally, unpin the page so that it may be evicted.
        bufferManager.unpinPage(dbPage);
    }


    /*========================================================================
     * CODE RELATED TO TABLE FILES
     */


    /**
     * This method takes a table name and returns a filename string that
     * specifies where the table's data is stored.
     *
     * @param tableName the name of the table to get the filename of
     *
     * @return the name of the file that holds the table's data
     */
    private String getTableFileName(String tableName) {
        return tableName + ".tbl";
    }


    /**
     * Creates a new table file with the table-name and schema specified in the
     * passed-in <tt>TableFileInfo</tt> object.  Additional details such as the
     * data file and the table manager are stored into the passed-in
     * <tt>TableFileInfo</tt> object upon successful creation of the new table.
     *
     * @param tblFileInfo This object is an in/out parameter.  It is used to
     *        specify the name and schema of the new table being created.  When
     *        the table is successfully created, the object is updated with the
     *        actual file that the table's schema and data are stored in.
     *
     * @throws IOException if the file cannot be created, or if an error occurs
     *         while storing the initial table data.
     */
    public void createTable(TableFileInfo tblFileInfo) throws IOException {

        int pageSize = StorageManager.getCurrentPageSize();

        String tableName = tblFileInfo.getTableName();
        String tblFileName = getTableFileName(tableName);

        // TODO:  the file-type should be specified in the TableFileInfo object
        DBFileType type = DBFileType.HEAP_DATA_FILE;
        TableManager tblManager = getTableManager(type);

        DBFile dbFile = fileManager.createDBFile(tblFileName, type, pageSize);
        logger.debug("Created new DBFile for table " + tableName +
            " at path " + dbFile.getDataFile());

        // Cache this table since it's now considered "open".
        openTables.put(tblFileInfo.getTableName(), tblFileInfo);

        tblFileInfo.setDBFile(dbFile);
        tblFileInfo.setTableManager(tblManager);

        tblManager.initTableInfo(tblFileInfo);
    }


    /**
     * This method opens the data file corresponding to the specified table
     * name and reads in the table's schema.  If the table is already open
     * then the cached data is simply returned.
     *
     * @param tableName the name of the table to open.  This is generally
     *        whatever was specified in a SQL statement that references the
     *        table.
     *
     * @return an object representing the schema and other details of the open
     *         table
     *
     * @throws java.io.FileNotFoundException if no table-file exists for the
     *         table; in other words, it doesn't yet exist.
     *
     * @throws IOException if an IO error occurs when attempting to open the
     *         table.
     */
    public TableFileInfo openTable(String tableName) throws IOException {
        TableFileInfo tblFileInfo;

        // If the table is already open, just return the cached information.
        tblFileInfo = openTables.get(tableName);
        if (tblFileInfo != null)
            return tblFileInfo;

        // Open the data file for the table; read out its type and page-size.

        String tblFileName = getTableFileName(tableName);
        DBFile dbFile = openDBFile(tblFileName);
        DBFileType type = dbFile.getType();
        TableManager tblManager = getTableManager(type);

        logger.debug(String.format("Opened DBFile for table %s at path %s.",
            tableName, dbFile.getDataFile()));
        logger.debug(String.format("Type is %s, page size is %d bytes.",
            type, dbFile.getPageSize()));

        tblFileInfo = new TableFileInfo(tableName, dbFile);
        tblFileInfo.setTableManager(tblManager);

        // Cache this table since it's now considered "open".
        openTables.put(tableName, tblFileInfo);

        // Defer to the appropriate table-manager to read in the remainder of
        // the details.
        tblManager.loadTableInfo(tblFileInfo);

        return tblFileInfo;
    }


    /**
     * This method closes a table file that is currently open, possibly flushing
     * any dirty pages to the table's storage in the process.
     *
     * @param tblFileInfo the table to close
     *
     * @throws IOException if an IO error occurs while attempting to close the
     *         table.  This could occur, for example, if dirty pages are being
     *         flushed to disk and a write error occurs.
     */
    public void closeTable(TableFileInfo tblFileInfo) throws IOException {
        DBFile dbFile = tblFileInfo.getDBFile();

        // Flush all open pages for the table.
        bufferManager.flushDBFile(dbFile);

        // Remove this table from the cache since it's about to be closed.
        openTables.remove(tblFileInfo.getTableName());

        // Let the table manager do any final cleanup necessary before closing
        // the table.
        DBFileType type = dbFile.getType();
        getTableManager(type).beforeCloseTable(tblFileInfo);
        closeDBFile(dbFile);
    }


    /**
     * Drops the specified table from the database.
     *
     * @param tableName the name of the table to drop
     *
     * @throws IOException if an IO error occurs while trying to delete the
     *         table's backing storage.
     */
    public void dropTable(String tableName) throws IOException {
        // As odd as it might seem, we need to open the table so that we can
        // drop all related objects, such as indexes on the table, etc.
        TableFileInfo tblFileInfo = openTable(tableName);
        
        TableSchema schema = tblFileInfo.getSchema();
        for (String indexName : schema.getIndexes().keySet())
            dropIndex(tblFileInfo, indexName);

        // Close the table.  This will purge out all dirty pages for the table
        // as well.
        closeTable(tblFileInfo);

        String tblFileName = getTableFileName(tableName);
        fileManager.deleteDBFile(tblFileName);
    }

    
    public void dropIndex(TableFileInfo tblFileInfo, String indexName)
        throws IOException {


    }
    
    

    /*========================================================================
     * CODE RELATED TO INDEX FILES
     */


    /**
     * This method takes an index name and returns a filename string that
     * specifies where the index's data is stored.
     *
     * @param indexName the name of the index to get the filename of
     *
     * @return the name of the file that holds the index's data
     */
    private String getIndexFileName(String indexName) {
        return indexName + ".idx";
    }


    /**
     * Creates a new index file with the index name, table name, and column list
     * specified in the passed-in <tt>IndexFileInfo</tt> object.  Additional
     * details such as the data file and the index manager are stored into the
     * passed-in <tt>IndexFileInfo</tt> object upon successful creation of the
     * new index.
     *
     * @param idxFileInfo This object is an in/out parameter.  It is used to
     *        specify the name and details of the new index being created.  When
     *        the index is successfully created, the object is updated with the
     *        actual file that the index is stored in.
     *
     * @throws IOException if the file cannot be created, or if an error occurs
     *         while storing the initial index data.
     */
    public void createIndex(IndexFileInfo idxFileInfo) throws IOException {
        String indexName = idxFileInfo.getTableName();
        String idxFileName = getIndexFileName(indexName);

        DBFileType type = idxFileInfo.getIndexType();
        IndexManager idxManager = getIndexManager(type);

        int pageSize = StorageManager.getCurrentPageSize();

        DBFile dbFile = fileManager.createDBFile(idxFileName, type, pageSize);
        logger.debug("Created new DBFile for index " + indexName +
            " at path " + dbFile.getDataFile());

        // Cache this index since it's now considered "open".
        openIndexes.put(idxFileInfo.getTableName(), idxFileInfo);

        idxFileInfo.setDBFile(dbFile);
        idxFileInfo.setIndexManager(idxManager);

        idxManager.initIndexInfo(idxFileInfo);
    }


    public void createUnnamedIndex(IndexFileInfo idxFileInfo) throws IOException {
        DBFileType type = idxFileInfo.getIndexType();
        IndexManager idxManager = getIndexManager(type);

        // Figure out an index name and filename for the unnamed index.  Primary
        // keys are handled separately, since each table only has one primary
        // key.  All other indexes are named by concatenating a prefix with a
        // numeric value that makes the index's name unique.
        File f;
        String indexName, indexFilename;
        String prefix = idxManager.getUnnamedIndexPrefix(idxFileInfo);
        if (idxFileInfo.getIndexInfo().getConstraintType() ==
            TableConstraintType.PRIMARY_KEY) {
            
            indexName = prefix;
            indexFilename = getIndexFileName(indexName);
            f = new File(baseDir, indexFilename);
            if (!f.createNewFile()) {
                throw new IOException("Couldn't create file " + f +
                    " for primary-key index " + indexName);
            }
        }
        else {
            String pattern = prefix + "_%03d";
            int i = 1;
            do {
                indexName = String.format(pattern, i);
                indexFilename = getIndexFileName(indexName);
                f = new File(baseDir, indexFilename);
                i++;
            }
            while (!f.createNewFile());
        }
        idxFileInfo.setIndexName(indexName);

        int pageSize = StorageManager.getCurrentPageSize();

        DBFile dbFile = fileManager.initDBFile(f, type, pageSize);
        logger.debug("Created new DBFile for unnamed index " + indexName +
            " at path " + dbFile.getDataFile());

        // Cache this index since it's now considered "open".
        openIndexes.put(idxFileInfo.getTableName(), idxFileInfo);

        idxFileInfo.setDBFile(dbFile);
        idxFileInfo.setIndexManager(idxManager);

        idxManager.initIndexInfo(idxFileInfo);
    }


    /**
     * This method opens the data file corresponding to the specified index
     * name and reads in the index's details.  If the index is already open
     * then the cached data is simply returned.
     *
     * @param tblFileInfo the table that the index is defined on
     *
     * @param indexName the name of the index to open.  Indexes are not
     *        referenced directly except by CREATE/ALTER/DROP INDEX statements,
     *        so these index names are stored in the table schema files, and are
     *        generally opened when the optimizer needs to know what indexes are
     *        available.
     *
     * @return an object representing the details of the open index
     *
     * @throws java.io.FileNotFoundException if no index-file exists for the
     *         index; in other words, it doesn't yet exist.
     *
     * @throws IOException if an IO error occurs when attempting to open the
     *         index.
     */
    public IndexFileInfo openIndex(TableFileInfo tblFileInfo, String indexName)
        throws IOException {

        IndexFileInfo idxFileInfo;

        // If the index is already open, just return the cached information.
        idxFileInfo = openIndexes.get(indexName);
        if (idxFileInfo != null)
            return idxFileInfo;

        // Open the data file for the index; read out its type and page-size.

        String idxFileName = getIndexFileName(indexName);
        DBFile dbFile = openDBFile(idxFileName);
        DBFileType type = dbFile.getType();
        IndexManager idxManager = getIndexManager(type);

        logger.debug(String.format("Opened DBFile for index %s at path %s.",
            indexName, dbFile.getDataFile()));
        logger.debug(String.format("Type is %s, page size is %d bytes.",
            type, dbFile.getPageSize()));

        idxFileInfo = new IndexFileInfo(indexName, tblFileInfo, dbFile);
        idxFileInfo.setIndexManager(idxManager);

        // Cache this index since it's now considered "open".
        openIndexes.put(indexName, idxFileInfo);

        // Defer to the appropriate index-manager to read in the remainder of
        // the details.
        idxManager.loadIndexInfo(idxFileInfo);

        return idxFileInfo;
    }
}
