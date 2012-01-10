package edu.caltech.nanodb.storage;


import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;


/**
 * This class is used to hold information about a single table in the database.
 * It stores the table's name, the schema details of the table, and the
 * {@link DBFile} object where the table's data is actually stored.
 */
public class TableFileInfo {
    /** If a table name isn't specified, this value is used instead. */
    public static final String UNNAMED_TABLE = "(unnamed)";


    /** The name of this table. */
    private String tableName;


    /** The schema of this table file. */
    private TableSchema schema;


    /** The statistics stored in this table file. */
    private TableStats stats;


    /** The table manager used to access this table file. */
    private TableManager tableManager;


    /**
     * If the table file has been opened, this is the actual data file that
     * the table is stored in.  Otherwise, this will be <tt>null</tt>.
     */
    private DBFile dbFile;


    /**
     * Construct a table file information object that represents the specified
     * table name and on-disk database file object.
     *
     * @param tableName the name of the table that this object represents
     *
     * @param dbFile the database file that holds the table's data
     *
     * @review (donnie) Shouldn't this just load the column info from the
     *         specified DBFile instance?
     */
    public TableFileInfo(String tableName, DBFile dbFile) {
        if (tableName == null)
            tableName = UNNAMED_TABLE;

        this.tableName = tableName;
        this.dbFile = dbFile;

        schema = new TableSchema();
        stats = new TableStats(schema.numColumns());
    }


    /**
     * Construct a table file information object for the specified table name.
     * This constructor is used by the <tt>CREATE TABLE</tt> command to hold the
     * table's schema, before the table has actually been created.  After the
     * table is created, the {@link #setDBFile} method is used to store the
     * database-file object onto this object.
     *
     * @param tableName the name of the table that this object represents
     */
    public TableFileInfo(String tableName) {
        this(tableName, null);
    }


    /**
     * Returns the actual database file that holds this table's data.
     *
     * @return the actual database file that holds this table's data, or
     *         <tt>null</tt> if it hasn't yet been set.
     */
    public DBFile getDBFile() {
        return dbFile;
    }


    /**
     * Method for storing the database-file object onto this table-file
     * information object, for example after successful completion of a
     * <tt>CREATE TABLE</tt> command.
     *
     * @param dbFile the database file that the table's data is stored in.
     */
    public void setDBFile(DBFile dbFile) {

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile must not be null!");

        if (this.dbFile != null)
            throw new IllegalStateException("This object already has a dbFile!");

        this.dbFile = dbFile;
    }


    /**
     * Returns the associated table name.
     *
     * @return the associated table name
     */
    public String getTableName() {
        return tableName;
    }


    public TableManager getTableManager() {
        return tableManager;
    }


    public void setTableManager(TableManager tableManager) {
        this.tableManager = tableManager;
    }


    /**
     * Returns the schema object associated with this table.  Note that this is
     * not a copy of the schema; it can be modified if so desired.  This is
     * necessary for table creation and modification.
     *
     * @return the schema object describing this table's schema
     */
    public TableSchema getSchema() {
        return schema;
    }


    public TableStats getStats() {
        return stats;
    }


    public void setStats(TableStats stats) {
        if (stats == null)
            throw new NullPointerException("stats cannot be null");

        this.stats = stats;
    }
}
