package edu.caltech.nanodb.commands;

import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.indexes.IndexInfo;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

/**
 * This command-class represents the <tt>CREATE INDEX</tt> DDL command.
 */
public class CreateIndexCommand extends Command {
    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(CreateIndexCommand.class);


    private String indexName;


    private String indexType;

    /**
     * This flag specifies whether the index is a unique index or not.  If the
     * value is true then no key-value may appear multiple times; if the value
     * is false then a key-value may appear multiple times.
     */
    private boolean unique;


    /** The name of the table that the index is built against. */
    private String tableName;


    /**
     * The list of column-names that the index is built against.  The order of
     * these values is important; for ordered indexes, the index records must be
     * kept in the order specified by the sequence of column names.
     */
    private ArrayList<String> columnNames = new ArrayList<String>();



    public CreateIndexCommand(String indexName, String tableName,
                              boolean unique) {
        super(Type.DDL);

        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        this.indexName = indexName;
        this.tableName = tableName;
        this.unique = unique;
    }


    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }


    public boolean isUnique() {
        return unique;
    }


    public void addColumn(String columnName) {
        this.columnNames.add(columnName);
    }


    public void execute() throws ExecutionException {
        // Set up the index-file info based on the command details.
        StorageManager storageManager = StorageManager.getInstance();

        // Open the table and get the schema for the table.
        logger.debug(String.format("Opening table %s to retrieve schema",
            tableName));
        TableFileInfo tblFileInfo;
        try {
            tblFileInfo = storageManager.openTable(tableName);
        }
        catch (FileNotFoundException e) {
            throw new ExecutionException(String.format(
                "Specified table %s doesn't exist!", tableName), e);
        }
        catch (IOException e) {
            throw new ExecutionException(String.format(
                "Error occurred while opening table %s", tableName), e);
        }

        // TODO:  Look up each column mentioned in the index, and build up the
        //        details of the index.
        TableSchema schema = tblFileInfo.getSchema();

        logger.debug(String.format("Creating an IndexFileInfo object " +
            "describing the new index %s on table %s.", indexName, tableName));
        IndexFileInfo idxFileInfo =
            new IndexFileInfo(indexName, tblFileInfo, (IndexInfo) null);

        if (indexName == null) {
            // This is an unnamed index.

            logger.debug("Creating the new unnamed index on disk.");
            try {
                storageManager.createUnnamedIndex(idxFileInfo);
            }
            catch (IOException e) {
                throw new ExecutionException(String.format(
                    "Could not create unnamed index on table \"%s\".  See " +
                        "nested exception for details.", tableName), e);
            }
        }
        else {
            // This is a named index.

            logger.debug("Creating the new index " + indexName + " on disk.");
            try {
                storageManager.createIndex(idxFileInfo);
            }
            catch (IOException e) {
                throw new ExecutionException(String.format(
                    "Could not create index \"%s\" on table \"%s\".  See " +
                        "nested exception for details.", indexName, tableName), e);
            }
        }

        // TODO:  Store the index info on the table.
        // schema.addIndex();

        indexName = idxFileInfo.getIndexName();
        logger.debug(String.format("New index %s on table %s is created!",
            indexName, tableName));

        System.out.printf("Created index %s on table %s.%n",
            indexName, tableName);
    }
}
