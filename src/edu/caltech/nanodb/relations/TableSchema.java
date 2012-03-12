package edu.caltech.nanodb.relations;


import edu.caltech.nanodb.indexes.IndexFileInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


/**
 * This class extends the <tt>Schema</tt> class with features specific to
 * tables, such as the ability to specify primary-key, foreign-key, and other
 * candidate-key constraints.
 */
public class TableSchema extends Schema {

    /**
     * This object specifies the primary key on this table.  This key is not
     * also included in the {@link #candidateKeys} collection.
     */
    private KeyColumnIndexes primaryKey;


    /**
     * A collection of candidate-key objects specifying the sets of columns that
     * comprise candidate keys on this table.  This collection does not include
     * the primary key specified by the {@link #primaryKey} field.
     */
    private ArrayList<KeyColumnIndexes> candidateKeys =
        new ArrayList<KeyColumnIndexes>();


    /**
     * A collection of foreign-key objects specifying other tables that this
     * table references.
     */
    private ArrayList<ForeignKeyColumnIndexes> foreignKeys =
        new ArrayList<ForeignKeyColumnIndexes>();


    /**
     * This collection provides easy access to all the indexes defined on this
     * table, including those for candidate keys and the primary key.
     */
    private HashMap<String, ColumnIndexes> indexes =
        new HashMap<String, ColumnIndexes>();


    /**
     * Sets the primary key on this table.
     *
     * @param pk the primary key to set on the table, or <tt>null</tt> if the
     *        table has no primary key.
     */
    public void setPrimaryKey(KeyColumnIndexes pk) {
        if (pk == null)
            throw new IllegalArgumentException("pk cannot be null");

        if (pk.getIndexName() == null)
            throw new IllegalArgumentException("pk must specify an index name");

        if (primaryKey != null)
            throw new IllegalStateException("Table already has a primary key");
        
        primaryKey = pk;
        indexes.put(pk.getIndexName(), pk);
    }


    /**
     * Returns the primary key on this table, or <tt>null</tt> if there is no
     * primary key.
     *
     * @return the primary key on this table, or <tt>null</tt> if there is no
     *         primary key.
     */
    public KeyColumnIndexes getPrimaryKey() {
        return primaryKey;
    }


    public void addCandidateKey(KeyColumnIndexes ck) {
        if (ck == null)
            throw new IllegalArgumentException("ck cannot be null");
        
        if (ck.getIndexName() == null)
            throw new IllegalArgumentException("ck must specify an index name");

        if (candidateKeys == null)
            candidateKeys = new ArrayList<KeyColumnIndexes>();

        candidateKeys.add(ck);
        indexes.put(ck.getIndexName(), ck);
    }


    public void addIndex(ColumnIndexes index) {
        if (index == null)
            throw new IllegalArgumentException("index cannot be null");

        if (index.getIndexName() == null)
            throw new IllegalArgumentException("index must specify an index name");

        indexes.put(index.getIndexName(), index);
    }


    public int numCandidateKeys() {
        return candidateKeys.size();
    }


    public List<KeyColumnIndexes> getCandidateKeys() {
        return Collections.unmodifiableList(candidateKeys);
    }


    /**
     * This helper function returns <tt>true</tt> if this table has a primary or
     * candidate key on the set of columns specified in the argument <tt>k</tt>.
     * This method is used to determine if a foreign key references a candidate
     * key on this table.
     *
     * @param k the key to check against this table to see if it's a
     *        candidate key
     *
     * @return true if this table contains a primary or candidate key on the
     *         columns specified in <tt>k</tt>
     */
    public boolean hasKeyOnColumns(ColumnIndexes k) {
        if (primaryKey != null && primaryKey.hasSameColumns(k))
            return true;

        for (KeyColumnIndexes ck : candidateKeys) {
            if (ck.hasSameColumns(k))
                return true;
        }

        return false;
    }


    public void addForeignKey(ForeignKeyColumnIndexes fk) {
        if (foreignKeys == null)
            foreignKeys = new ArrayList<ForeignKeyColumnIndexes>();

        foreignKeys.add(fk);
    }


    public int numForeignKeys() {
        return foreignKeys.size();
    }


    public List<ForeignKeyColumnIndexes> getForeignKeys() {
        return Collections.unmodifiableList(foreignKeys);
    }


    public Map<String, ColumnIndexes> getIndexes() {
        return Collections.unmodifiableMap(indexes);
    }


    private int[] getColumnIndexes(List<String> columnNames) {
        int[] result = new int[columnNames.size()];
        HashSet<String> s = new HashSet<String>();

        int i = 0;
        for (String colName : columnNames) {
            if (!s.add(colName)) {
                throw new SchemaNameException(String.format(
                    "Column %s was specified multiple times", colName));
            }
            result[i] = getColumnIndex(columnNames.get(i));
            i++;
        }

        return result;
    }
    
    
    public ArrayList<ColumnInfo> getColumnInfos(ColumnIndexes colIndexes) {
        ArrayList<ColumnInfo> result = new ArrayList<ColumnInfo>(colIndexes.size());

        for (int i = 0; i < colIndexes.size(); i++)
            result.add(getColumnInfo(colIndexes.getCol(i)));

        return result;
    }


    /**
     * This method constructs a <tt>KeyColumnIndexes</tt> object that includes
     * the columns named in the input list.  Note that this method <u>does
     * not</u> update the schema stored on disk, or create any other supporting
     * files.
     * 
     * @param columnNames a list of column names that are in the key
     *
     * @return a new <tt>KeyColumnIndexes</tt> object with the indexes of the
     *         columns stored in the object
     *
     * @throws SchemaNameException if a column-name cannot be found, or if a
     *         column-name is ambiguous (unlikely), or if a column is specified
     *         multiple times in the input list.
     */
    public KeyColumnIndexes makeKey(List<String> columnNames) {
        if (columnNames == null)
            throw new IllegalArgumentException("columnNames must be specified");

        if (columnNames.isEmpty()) {
            throw new IllegalArgumentException(
                "columnNames must specify at least one column");
        }

        return new KeyColumnIndexes(getColumnIndexes(columnNames));
    }


    /**
     * This method constructs a <tt>ForeignKeyColumns</tt> object that includes
     * the columns named in the input list, as well as the referenced table and
     * column names.  Note that this method <u>does not</u> update the schema
     * stored on disk, or create any other supporting files.
     *
     * @param columnNames a list of column names that are in the key
     *
     * @param refTableName the table referenced by this key
     *
     * @param refTableSchema the schema of the table referenced by this key
     *
     * @param refColumnNames the columns in the referenced table that this
     *        table's columns reference
     *
     * @return a new <tt>ForeignKeyColumns</tt> object with the indexes of the
     *         columns stored in the object
     *
     * @throws SchemaNameException if a column-name cannot be found, or if a
     *         column-name is ambiguous (unlikely), or if a column is specified
     *         multiple times in the input list.
     */
    public ForeignKeyColumnIndexes makeForeignKey(List<String> columnNames,
        String refTableName, TableSchema refTableSchema,
        List<String> refColumnNames) {

        if (columnNames == null)
            throw new IllegalArgumentException("columnNames must be specified");

        if (refTableName == null)
            throw new IllegalArgumentException("refTableName must be specified");

        if (refTableSchema == null)
            throw new IllegalArgumentException("refTableSchema must be specified");

        if (refColumnNames == null)
            throw new IllegalArgumentException("refColumnNames must be specified");

        if (columnNames.isEmpty()) {
            throw new IllegalArgumentException(
                "columnNames must specify at least one column");
        }

        if (columnNames.size() != refColumnNames.size()) {
            throw new IllegalArgumentException("columnNames and " +
                "refColumnNames must specify the same number of columns");
        }

        int[] colIndexes = getColumnIndexes(columnNames);
        int[] refColIndexes = refTableSchema.getColumnIndexes(refColumnNames);

        if (!refTableSchema.hasKeyOnColumns(new ColumnIndexes(colIndexes))) {
            throw new SchemaNameException(String.format("Referenced columns " +
                "%s in table %s are not a primary or candidate key",
                refColumnNames, refTableName));
        }

        return new ForeignKeyColumnIndexes(colIndexes, refTableName, refColIndexes);
    }
}
