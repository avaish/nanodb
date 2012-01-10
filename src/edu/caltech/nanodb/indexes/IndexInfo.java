package edu.caltech.nanodb.indexes;


import edu.caltech.nanodb.indexes.IndexType;
import edu.caltech.nanodb.relations.ColumnIndexes;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableSchema;


/**
 * This class holds all details of an index within the database.
 *
 * @review (Donnie) Maybe rename this to IndexDesc
 */
public class IndexInfo {
    /** The general type of index. */
    private IndexType type;


    /** The name of the table that the index is built against. */
    private String tableName;


    /** The schema for the table that the index is built against. */
    private Schema tableSchema;


    /**
     * If this index was created to enforce a particular table constraint, this
     * field specifies the kind of constraint it is used to enforce.
     */
    private TableConstraintType constraintType;


    /**
     * A flag indicating whether the index is a unique index (i.e. each value
     * appears only once) or not.
     */
    private boolean unique;


    /** The table columns that the index is built against. */
    private ColumnIndexes columnIndexes;


    public TableConstraintType getConstraintType() {
        return constraintType;
    }
    
    
    public IndexInfo(String tableName, TableSchema tableSchema,
                     ColumnIndexes columnIndexes, boolean unique) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.columnIndexes = columnIndexes;
        this.unique = unique;
    }


    public void setConstraintType(TableConstraintType constraintType) {
        this.constraintType = constraintType;
    }


    public ColumnIndexes getColumnIndexes() {
        return columnIndexes;
    }
}
