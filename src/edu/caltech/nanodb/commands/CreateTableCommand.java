package edu.caltech.nanodb.commands;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import edu.caltech.nanodb.indexes.IndexInfo;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.indexes.IndexFileInfo;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ForeignKeyColumnIndexes;
import edu.caltech.nanodb.relations.KeyColumnIndexes;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableSchema;

import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;


/**
 * This command handles the <tt>CREATE TABLE</tt> DDL operation.
 */
public class CreateTableCommand extends Command {

    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(CreateTableCommand.class);


    /** Name of the table to be created. */
    private String tableName;

    /** If this flag is <tt>true</tt> then the table is a temporary table. */
    private boolean temporary;


    /**
     * If this flag is <tt>true</tt> then the create-table operation should only
     * be performed if the specified table doesn't already exist.
     */
    private boolean ifNotExists;


    /** List of column-declarations for the new table. */
    private List<ColumnInfo> columnInfos = new ArrayList<ColumnInfo>();

    /** List of constraints for the new table. */
    private List<ConstraintDecl> constraints = new ArrayList<ConstraintDecl>();


    /**
     * Create a new object representing a <tt>CREATE TABLE</tt> statement.
     *
     * @param tableName the name of the table to be created
     * @param temporary true if the table is a temporary table, false otherwise
     * @param ifNotExists If this flag is true, the table will only be created
     *        if it doesn't already exist.
     */
    public CreateTableCommand(String tableName,
                              boolean temporary, boolean ifNotExists) {
        super(Command.Type.DDL);

        this.tableName = tableName;
        this.temporary = temporary;
        this.ifNotExists = ifNotExists;
    }


    /**
     * Adds a column description to this create-table command.  This method is
     * primarily used by the SQL parser.
     *
     * @param colInfo the details of the column to add
     *
     * @throws NullPointerException if colDecl is null
     */
    public void addColumn(ColumnInfo colInfo) {
        if (colInfo == null)
            throw new NullPointerException("colInfo");

        columnInfos.add(colInfo);
    }


    /**
     * Adds a constraint to this create-table command.  This method is primarily
     * used by the SQL parser.
     *
     * @param con the details of the table constraint to add
     *
     * @throws NullPointerException if con is null
     */
    public void addConstraint(ConstraintDecl con) {
        if (con == null)
            throw new NullPointerException("con");

        constraints.add(con);
    }


    public void execute() throws ExecutionException {
        StorageManager storageManager = StorageManager.getInstance();

        // See if the table already exists.
        if (ifNotExists) {
            logger.debug("Checking if table " + tableName + " already exists.");

            try {
                storageManager.openTable(tableName);

                // If we got here then the table exists.  Skip the operation.
                out.printf("Table %s already exists; skipping create-table.%n",
                    tableName);
                return;
            }
            catch (FileNotFoundException e) {
                // Table doesn't exist yet!  This is an expected exception.
            }
            catch (IOException e) {
                // Some other unexpected exception occurred.  Report an error.
                throw new ExecutionException(
                    "Exception while trying to determine if table " +
                    tableName + " exists.", e);
            }
        }

        // Set up the table-file info based on the command details.

        logger.debug("Creating a TableFileInfo object describing the new table " +
            tableName + ".");
        TableFileInfo tblFileInfo = new TableFileInfo(tableName);
        TableSchema schema = tblFileInfo.getSchema();
        for (ColumnInfo colInfo : columnInfos) {
            try {
                schema.addColumnInfo(colInfo);
            }
            catch (IllegalArgumentException iae) {
                throw new ExecutionException("Duplicate or invalid column \"" +
                    colInfo.getName() + "\".", iae);
            }
        }

        // Open all tables referenced by foreign-key constraints, so that we can
        // verify the constraints.
        HashMap<String, TableSchema> referencedTables =
            new HashMap<String, TableSchema>();
        for (ConstraintDecl cd: constraints) {
            if (cd.getType() == TableConstraintType.FOREIGN_KEY) {
                String refTableName = cd.getRefTable();
                try {
                    TableFileInfo refTblFileInfo =
                        storageManager.openTable(refTableName);
                    referencedTables.put(refTableName, refTblFileInfo.getSchema());
                }
                catch (FileNotFoundException e) {
                    throw new ExecutionException(String.format(
                        "Referenced table %s doesn't exist.", refTableName), e);
                }
                catch (IOException e) {
                    throw new ExecutionException(String.format(
                        "Error while loading schema for referenced table %s.",
                        refTableName), e);
                }
            }
        }

        try {
            initTableConstraints(storageManager, schema, referencedTables);
        }
        catch (IOException e) {
            throw new ExecutionException(
                "Couldn't initialize all constraints on table " + tableName, e);
        }

        // Get the table manager and create the table.

        logger.debug("Creating the new table " + tableName + " on disk.");
        try {
            storageManager.createTable(tblFileInfo);
        }
        catch (IOException ioe) {
            throw new ExecutionException("Could not create table \"" + tableName +
                "\".  See nested exception for details.", ioe);
        }
        logger.debug("New table " + tableName + " is created!");

        out.println("Created table:  " + tableName);
    }
    
    
    private void initTableConstraints(StorageManager storageManager,
        TableSchema schema, HashMap<String, TableSchema> referencedTables)
        throws ExecutionException, IOException {

        // Add constraints to the table's schema, creating indexes where
        // appropriate so that the constraints can be enforced.

        HashSet<String> constraintNames = new HashSet<String>();

        for (ConstraintDecl cd : constraints) {
            // Make sure that if constraint names are specified, every
            // constraint is actually uniquely named.
            if (cd.getName() != null) {
                if (!constraintNames.add(cd.getName())) {
                    throw new ExecutionException("Constraint name " +
                        cd.getName() + " appears multiple times.");
                }
            }

            TableConstraintType type = cd.getType();
            if (type == TableConstraintType.PRIMARY_KEY) {
                // Make a primary key constraint and put it on the schema.
                KeyColumnIndexes pk = schema.makeKey(cd.getColumnNames());
                pk.setConstraintName(cd.getName());

                // Make the index, then store the index name on the key object.

                IndexInfo info = new IndexInfo(tableName, schema, pk, true);
                info.setConstraintType(TableConstraintType.PRIMARY_KEY);

                IndexFileInfo idxFileInfo = new IndexFileInfo(null, tableName, info);
                storageManager.createUnnamedIndex(idxFileInfo);
                logger.debug(String.format(
                    "Created index %s on table %s to enforce primary key.",
                    idxFileInfo.getIndexName(), idxFileInfo.getTableName()));

                pk.setIndexName(idxFileInfo.getIndexName());
                schema.setPrimaryKey(pk);
            }
            else if (type == TableConstraintType.UNIQUE) {
                // Make a unique key constraint and put it on the schema.
                KeyColumnIndexes ck = schema.makeKey(cd.getColumnNames());
                ck.setConstraintName(cd.getName());

                // Make the index, then store the index name on the key object.

                IndexInfo info = new IndexInfo(tableName, schema, ck, true);
                info.setConstraintType(TableConstraintType.UNIQUE);

                IndexFileInfo idxFileInfo = new IndexFileInfo(null, tableName, info);
                storageManager.createUnnamedIndex(idxFileInfo);
                logger.debug(String.format(
                    "Created index %s on table %s to enforce candidate key.",
                    idxFileInfo.getIndexName(), idxFileInfo.getTableName()));

                ck.setIndexName(idxFileInfo.getIndexName());
                schema.addCandidateKey(ck);
            }
            else if (type == TableConstraintType.FOREIGN_KEY) {
                // Make a foreign key constraint and put it on the schema.

                // This should never be null since we already resolved all
                // foreign-key table references earlier.
                TableSchema refTableSchema = referencedTables.get(cd.getRefTable());

                // The makeForeignKey() method ensures that the referenced
                // columns are also a candidate key (or primary key) on the
                // referenced table.
                ForeignKeyColumnIndexes fk = schema.makeForeignKey(cd.getColumnNames(),
                    cd.getRefTable(), refTableSchema, cd.getRefColumnNames());
                fk.setConstraintName(cd.getName());
                schema.addForeignKey(fk);
            }
            else if (type == TableConstraintType.NOT_NULL) {
                // TODO:  Record that the column cannot be set to NULL.
                throw new UnsupportedOperationException(
                    "NOT NULL not yet implemented");
            }
            else {
                throw new ExecutionException("Unexpected constraint type " +
                    cd.getType());
            }
        }
    }


    @Override
    public String toString() {
        return "CreateTable[" + tableName + "]";
    }


    /**
     * Returns a verbose, multi-line string containing all of the details of
     * this table.
     *
     * @return a detailed description of the table described by this command
     */
    public String toVerboseString() {
        StringBuilder strBuf = new StringBuilder();

        strBuf.append(toString());
        strBuf.append('\n');

        for (ColumnInfo colInfo : columnInfos) {
            strBuf.append('\t');
            strBuf.append(colInfo.toString());
            strBuf.append('\n');
        }

        for (ConstraintDecl con : constraints) {
            strBuf.append('\t');
            strBuf.append(con.toString());
            strBuf.append('\n');
        }

        return strBuf.toString();
    }
}
