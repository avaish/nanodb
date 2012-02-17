package edu.caltech.nanodb.commands;


import java.io.IOException;

import java.util.ArrayList;
import java.util.LinkedHashSet;

import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.indexes.IndexManager;
import edu.caltech.nanodb.relations.ColumnIndexes;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;


/**
 * This Command class represents the <tt>OPTIMIZE</tt> SQL command, which
 * optimizes a table's representation (along with any indexes) to improve access
 * performance and space utilization.  This is not a standard SQL command.
 */
public class OptimizeCommand extends Command {

    /**
     * Table names are kept in a set so that we don't need to worry about a
     * particular table being specified multiple times.
     */
    private LinkedHashSet<String> tableNames;


    /**
     * Construct a new <tt>OPTIMIZE</tt> command with an empty table list.
     * Tables can be added to the internal list using the {@link #addTable}
     * method.
     */
    public OptimizeCommand() {
        super(Command.Type.UTILITY);
        tableNames = new LinkedHashSet<String>();
    }


    /**
     * Construct a new <tt>OPTIMIZE</tt> command to optimize the specified
     * table.
     *
     * @param tableName the name of the table to optimize.
     */
    public OptimizeCommand(String tableName) {
        this();
        addTable(tableName);
    }


    /**
     * Add a table to the list of tables to optimize.
     *
     * @param tableName the name of the table to optimize.
     */
    public void addTable(String tableName) {
        if (tableName == null)
            throw new NullPointerException("tableName cannot be null");

        tableNames.add(tableName);
    }


    public void execute() throws ExecutionException {
        // Optimize each table!

        ArrayList<TableFileInfo> tblInfos = new ArrayList<TableFileInfo>();

        StorageManager storageMgr = StorageManager.getInstance();
        for (String table : tableNames) {
            try {
                TableFileInfo tblFileInfo = storageMgr.openTable(table);
                tblInfos.add(tblFileInfo);
            }
            catch (IOException ioe) {
                throw new ExecutionException("Could not open table " + table, ioe);
            }
        }

        // Now, optimize each table.

        for (TableFileInfo tblFileInfo : tblInfos) {

            out.println("TODO:  Optimizing table " + tblFileInfo.getTableName());

            /*
            TableSchema schema = tblFileInfo.getSchema();
            for (ColumnIndexes colIndexes : schema.getIndexes().values()) {
                String indexName = colIndexes.getIndexName();

                IndexFileInfo idxFileInfo;
                try {
                    idxFileInfo = storageMgr.openIndex(tblFileInfo, indexName);
                }
                catch (IOException e) {
                    out.printf("ERROR:  Couldn't open index %s on table %s%n",
                        indexName, tblFileInfo.getTableName());
                    continue;
                }

                IndexManager idxMgr = idxFileInfo.getIndexManager();
                try {
                    out.println(" * Optimizing index " + indexName);
                    idxMgr.optimizeIndex(idxFileInfo);
                }
                catch (IOException e) {
                    throw new ExecutionException(
                        "IO error occurred while optimizing index " + indexName +
                            " on table " + tblFileInfo.getTableName(), e);
                }
            }
            */
        }
        out.println("TODO:  Optimization complete.");
    }


    /**
     * Prints a simple representation of the optimize command, including the
     * names of the tables to be optimized.
     *
     * @return a string representing this optimize command
     */
    @Override
    public String toString() {
        return "Optimize[" + tableNames + "]";
    }
}
