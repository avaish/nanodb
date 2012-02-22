package edu.caltech.nanodb.commands;


import java.io.IOException;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.indexes.IndexManager;
import edu.caltech.nanodb.relations.ColumnIndexes;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;


/**
 * This Command class represents the <tt>VERIFY</tt> SQL command, which
 * verifies a table's representation (along with any indexes) to ensure that
 * all structural details are valid.  This is not a standard SQL command, but
 * it is very useful for verifying student implementations of file structures.
 */
public class VerifyCommand extends Command {

    /**
     * Table names are kept in a set so that we don't need to worry about a
     * particular table being specified multiple times.
     */
    private LinkedHashSet<String> tableNames;


    /**
     * Construct a new <tt>VERIFY</tt> command with an empty table list.
     * Tables can be added to the internal list using the {@link #addTable}
     * method.
     */
    public VerifyCommand() {
        super(Command.Type.UTILITY);
        tableNames = new LinkedHashSet<String>();
    }


    /**
     * Construct a new <tt>VERIFY</tt> command to verify the specified table.
     *
     * @param tableName the name of the table to verify.
     */
    public VerifyCommand(String tableName) {
        this();
        addTable(tableName);
    }


    /**
     * Add a table to the list of tables to verify.
     *
     * @param tableName the name of the table to verify.
     */
    public void addTable(String tableName) {
        if (tableName == null)
            throw new NullPointerException("tableName cannot be null");

        tableNames.add(tableName);
    }


    public void execute() throws ExecutionException {
        // Make sure that all the tables are valid.

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

        // Now, verify each table.

        for (TableFileInfo tblFileInfo : tblInfos) {

            out.println("Verifying table " + tblFileInfo.getTableName());

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
                    out.println("Verifying index " + indexName + "...");
                    List<String> errors = idxMgr.verifyIndex(idxFileInfo);
                    
                    if (!errors.isEmpty()) {
                        out.println("\nFOUND " + errors.size() + " ERRORS:\n");
                        for (String e : errors)
                            out.println("  " + e);
                        out.println("\n");
                    }
                    else {
                        out.println("Looks good!");
                    }
                }
                catch (IOException e) {
                    throw new ExecutionException(
                        "IO error occurred while verifying index " + indexName +
                        " on table " + tblFileInfo.getTableName(), e);
                }
            }
        }
        out.println("Verification complete.");
    }


    /**
     * Prints a simple representation of the verify command, including the
     * names of the tables to be verified.
     *
     * @return a string representing this verify command
     */
    @Override
    public String toString() {
        return "Verify[" + tableNames + "]";
    }
}
