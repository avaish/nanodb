package edu.caltech.nanodb.relations;


/**
 * This class represents a foreign key to another table in the database.
 */
public class ForeignKeyColumnIndexes {
    /** This is the optional name of the constraint specified in the DDL. */
    private String constraintName;
    
    
    /** This array holds the indexes of the columns in the foreign key. */
    private int[] colIndexes;

    /** This is the name of the table that is referenced by this table. */
    private String referencedTable;

    /** These are the indexes of the columns in the referenced table. */
    private int[] referencedColIndexes;


    public ForeignKeyColumnIndexes(int[] colIndexes, String referencedTable,
                             int[] referencedColIndexes) {
        if (colIndexes == null) {
            throw new IllegalArgumentException(
                "colIndexes must be specified");
        }

        if (referencedTable == null) {
            throw new IllegalArgumentException(
                "referencedTable must be specified");
        }

        if (referencedColIndexes == null) {
            throw new IllegalArgumentException(
                "referencedColIndexes must be specified");
        }

        if (colIndexes.length != referencedColIndexes.length) {
            throw new IllegalArgumentException(
                "colIndexes and referencedColIndexes must have the same length");
        }

        this.colIndexes = colIndexes;
        this.referencedTable = referencedTable;
        this.referencedColIndexes = referencedColIndexes;
    }


    public String getConstraintName() {
        return constraintName;
    }


    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }


    public int size() {
        return colIndexes.length;
    }


    public int getCol(int i) {
        return colIndexes[i];
    }

    
    public String getRefTable() {
        return referencedTable;
    }
    

    public int getRefCol(int i) {
        return colIndexes[i];
    }
    
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        boolean first;

        buf.append('(');
        first = true;
        for (int i : colIndexes) {
            if (first)
                first = false;
            else
                buf.append(", ");

            buf.append(i);
        }
        buf.append(") --> ");

        buf.append(referencedTable);

        buf.append('(');
        first = true;
        for (int i : referencedColIndexes) {
            if (first)
                first = false;
            else
                buf.append(", ");

            buf.append(i);
        }
        buf.append(')');

        return buf.toString();
    }
}
