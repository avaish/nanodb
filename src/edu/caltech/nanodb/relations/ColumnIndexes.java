package edu.caltech.nanodb.relations;


import org.apache.commons.lang.ObjectUtils;

import java.util.Arrays;
import java.util.HashSet;


/**
 * This class represents a set of columns in a schema by specifying the indexes
 * of the columns in the key.  This is used to represent primary keys, candidate
 * keys, foreign keys, and columns in general indexes.
 *
 * a primary key or other unique key, specifying the
 * indexes of the columns in the key.  The class also specifies the index
 * used to enforce the key in the database.
 */
public class ColumnIndexes {
    /** This array holds the indexes of the columns in the key. */
    private int[] colIndexes;


    public ColumnIndexes(int[] colIndexes) {
        if (colIndexes == null)
            throw new IllegalArgumentException("colIndexes must be specified");
        
        if (colIndexes.length == 0) {
            throw new IllegalArgumentException(
                "colIndexes must have at least one element");
        }

        // Make sure that no column-index values are duplicated, and that none
        // are negative values.
        int[] tmp = colIndexes.clone();
        Arrays.sort(tmp);
        for (int i = 0; i < tmp.length; i++) {
            if (tmp[i] < 0) {
                throw new IllegalArgumentException(
                    "colIndexes cannot contain negative values; got " +
                    Arrays.toString(colIndexes));
            }

            if (i > 0 && tmp[i] == tmp[i - 1]) {
                throw new IllegalArgumentException(
                    "colIndexes cannot contain duplicate values; got " +
                    Arrays.toString(colIndexes));
            }
        }

        this.colIndexes = colIndexes;
    }


    public int size() {
        return colIndexes.length;
    }


    public int getCol(int i) {
        return colIndexes[i];
    }


    /**
     * Returns true if the specified <tt>ColumnIndexes</tt> object has the same
     * columns as this object, in the exact same order.
     *
     * @param ci the <tt>ColumnIndexes</tt> object to compare to this object
     *
     * @return true if the two objects have the same column indexes, in the
     *         exact same order
     */
    public boolean equalsColumns(ColumnIndexes ci) {
        return Arrays.equals(colIndexes, ci.colIndexes);
    }


    /**
     * Returns true if the specified <tt>ColumnIndexes</tt> object has the same
     * columns as this object, independent of order.
     *
     * @param ci the <tt>ColumnIndexes</tt> object to compare to this object
     *
     * @return true if the two objects have the same column indexes,
     *         independent of order
     */
    public boolean hasSameColumns(ColumnIndexes ci) {
        HashSet<Integer> indexes = new HashSet<Integer>();
        for (int i : colIndexes)
            indexes.add(i);

        for (int i : ci.colIndexes) {
            if (!indexes.contains(i))
                return false;

            indexes.remove(i);
        }

        return indexes.isEmpty();
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        
        buf.append('(');
        boolean first = true;
        for (int i : colIndexes) {
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
