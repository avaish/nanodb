package edu.caltech.nanodb.commands;


import java.io.IOException;

import java.util.ArrayList;
import java.util.LinkedHashSet;

import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;
import edu.caltech.nanodb.storage.TableManager;


/**
 * This Command class represents the <tt>ANALYZE</tt> SQL command, which
 * analyzes a table's internal data and updates its cached statistics to be as
 * up-to-date as possible.  This is not a standard SQL command, but virtually
 * every good database has a mechanism to manually perform this task.
 */
public class AnalyzeCommand extends Command {

    /**
     * Table names are kept in a set so that we don't need to worry about a
     * particular table being specified multiple times.
     */
    private LinkedHashSet<String> tableNames;


    private boolean verbose = false;


    /**
     * Construct a new <tt>ANALYZE</tt> command with an empty table list.
     * Tables can be added to the internal list using the {@link #addTable}
     * method.
     *
     * @param verbose a flag indicating whether this command should produce
     *        verbose output
     */
    public AnalyzeCommand(boolean verbose) {
        super(Command.Type.UTILITY);
        tableNames = new LinkedHashSet<String>();

        this.verbose = verbose;
    }


    /**
     * Construct a new <tt>ANALYZE</tt> command with an empty table list.
     * Tables can be added to the internal list using the {@link #addTable}
     * method.
     */
    public AnalyzeCommand() {
        this(false);
    }


    /**
     * Construct a new <tt>ANALYZE</tt> command to analyze the specified table.
     *
     * @param tableName the name of the table to analyze.
     */
    public AnalyzeCommand(String tableName) {
        this(tableName, false);
    }

    /**
     * Construct a new <tt>ANALYZE</tt> command to analyze the specified table.
     *
     * @param tableName the name of the table to analyze.
     * @param verbose a flag indicating whether this command should produce
     *        verbose output
     */
    public AnalyzeCommand(String tableName, boolean verbose) {
        this(verbose);
        addTable(tableName);
    }


    /**
     * Add a table to the list of tables to analyze.
     *
     * @param tableName the name of the table to analyze.
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

        // Now, analyze each table.

        for (TableFileInfo tblFileInfo : tblInfos) {
            try {
                out.println("Analyzing table " + tblFileInfo.getTableName());
                TableManager tableMgr = tblFileInfo.getTableManager();
                tableMgr.analyzeTable(tblFileInfo);
            }
            catch (IOException ioe) {
                throw new ExecutionException("Could not update statistics on table " +
                    tblFileInfo.getTableName(), ioe);
            }
        }
        out.println("Analysis complete.");
    }


    /**
     * Prints a simple representation of the analyze command, including the
     * names of the tables to be analyzed.
     *
     * @return a string representing this analyze command
     */
    @Override
    public String toString() {
        return "Analyze[" + tableNames + "]";
    }
}
