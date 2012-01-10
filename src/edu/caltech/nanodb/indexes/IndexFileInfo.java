package edu.caltech.nanodb.indexes;


import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBFileType;


/**
 * This class is used to hold information about a single index in the database.
 * It stores the table's name, the schema details of the table, and the
 * {@link DBFile} object where the table's data is actually stored.
 */
public class IndexFileInfo {

    /** The name of the index. */
    private String indexName;


    /** The name of the table the index is against. */
    private String tableName;


    /** The actual details about what columns are in the index, etc. */
    private IndexInfo indexInfo;


    /**
     * The type of index to create.  The default type is
     * {@link DBFileType#BTREE_INDEX_FILE}.
     */
    private DBFileType indexType = DBFileType.BTREE_INDEX_FILE;


    /** The index manager used to access this index file. */
    private IndexManager indexManager;


    /**
     * If the index file has been opened, this is the actual data file that
     * the index is stored in.  Otherwise, this will be <tt>null</tt>.
     */
    private DBFile dbFile;


    public IndexFileInfo(String indexName, String tableName, DBFile dbFile) {
        // if (indexName == null)
        //     throw new IllegalArgumentException("indexName must be specified");

        if (tableName == null)
            throw new IllegalArgumentException("tableName must be specified");

        this.indexName = indexName;
        this.tableName = tableName;
        this.dbFile = dbFile;
    }


    /**
     * Construct an index file information object for the specified index name.
     * This constructor is used by the <tt>CREATE TABLE</tt> command to hold the
     * table's schema, before the table has actually been created.  After the
     * table is created, the {@link #setDBFile} method is used to store the
     * database-file object onto this object.
     *
     * @param indexName the name of the index that this object represents
     * @param tableName the name of the table that the index is built against
     */
    public IndexFileInfo(String indexName, String tableName, IndexInfo indexInfo) {
        // if (indexName == null)
        //     throw new IllegalArgumentException("indexName must be specified");

        if (tableName == null)
            throw new IllegalArgumentException("tableName must be specified");

        this.indexName = indexName;
        this.tableName = tableName;
        this.indexInfo = indexInfo;
    }
    
    
    public DBFileType getIndexType() {
        return indexType;
    }

    
    public IndexInfo getIndexInfo() {
        return indexInfo;
    }
    

    /**
     * Returns the actual database file that holds this index's data.
     *
     * @return the actual database file that holds this index's data, or
     *         <tt>null</tt> if it hasn't yet been set.
     */
    public DBFile getDBFile() {
        return dbFile;
    }


    /**
     * Method for storing the database-file object onto this index-file
     * information object, for example after successful completion of a
     * <tt>CREATE INDEX</tt> command.
     *
     * @param dbFile the database file that the index's data is stored in.
     */
    public void setDBFile(DBFile dbFile) {

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile must not be null!");

        if (this.dbFile != null)
            throw new IllegalStateException("This object already has a dbFile!");

        this.dbFile = dbFile;
    }


    /**
     * Returns the index name.
     *
     * @return the index name
     */
    public String getIndexName() {
        return indexName;
    }

    
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
    

    /**
     * Returns the associated table name.
     *
     * @return the associated table name
     */
    public String getTableName() {
        return tableName;
    }
    
    
    public void setIndexManager(IndexManager indexManager) {
        this.indexManager = indexManager;
    }
}


