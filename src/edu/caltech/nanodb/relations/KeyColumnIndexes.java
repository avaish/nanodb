package edu.caltech.nanodb.relations;


/**
 * This class represents a primary key or other unique key, specifying the
 * indexes of the columns in the key.  The class also specifies the index
 * used to enforce the key in the database.
 */
public class KeyColumnIndexes extends ColumnIndexes {

    /** This is the optional name of the constraint specified in the DDL. */
    private String constraintName;


    /** This is the name of the index that is used to enforce the key. */
    private String indexName;


    public KeyColumnIndexes(int[] colIndexes, String indexName) {
        super(colIndexes);
        this.indexName = indexName;
    }


    public KeyColumnIndexes(int[] colIndexes) {
        this(colIndexes, null);
    }
    
    
    public String getConstraintName() {
        return constraintName;
    }
    
    
    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }


    public String getIndexName() {
        return indexName;
    }
    
    
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
}
