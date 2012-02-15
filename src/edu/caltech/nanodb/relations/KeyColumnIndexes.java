package edu.caltech.nanodb.relations;


/**
 * This class represents a primary key or other unique key, specifying the
 * indexes of the columns in the key.  The class also specifies the index
 * used to enforce the key in the database.
 */
public class KeyColumnIndexes extends ColumnIndexes {

    /** This is the optional name of the constraint specified in the DDL. */
    private String constraintName;


    public KeyColumnIndexes(String indexName, int[] colIndexes) {
        super(indexName, colIndexes);
    }


    public KeyColumnIndexes(int[] colIndexes) {
        super(colIndexes);
    }

    
    public String getConstraintName() {
        return constraintName;
    }
    
    
    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }
}
