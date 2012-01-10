package edu.caltech.nanodb.storage;


import java.util.Collections;
import java.util.List;

import edu.caltech.nanodb.expressions.TypeConverter;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.Tuple;


/**
 * <p>
 * This class is a partial implementation of the {@link Tuple} interface that
 * handles reading and writing tuple data against a {@link DBPage} object.
 * This can be used to read and write tuples in a table file, keys in an index
 * file, etc.  It could also be used to store and manage tuples in memory,
 * although it's generally much faster and simpler to use an optimized in-memory
 * representation for tuples in memory.
 * </p>
 * <p>
 * Each tuple is stored in a layout like this:
 * </p>
 * <ul>
 *   <li>The first one or more bytes are dedicated to a <tt>NULL</tt>-bitmap,
 *       which records columns that are currently <tt>NULL</tt>.</li>
 *   <li>The remaining bytes are dedicated to storing the non-<tt>NULL</tt>
 *       values for the columns in the tuple.</li>
 * </ul>
 * <p>
 * In order to make this class' functionality generic, certain operations must
 * be implemented by subclasses:  specifically, any operation that changes a
 * tuple's size (e.g. writing a non-<tt>NULL</tt> value to a previously
 * <tt>NULL</tt> column or vice versa, or changing the size of a variable-size
 * column).  The issue with these operations is that they require page-level
 * data management, which is beyond the scope of what this class can provide.
 * Thus, concrete subclasses of this class can provide page-level data
 * management as needed.
 * </p>
 */
public abstract class PageTuple implements Tuple {

    /**
     * This value is used in {@link #valueOffsets} when a column value is set to
     * <tt>NULL</tt>.
     */
    public static final int NULL_OFFSET = 0;


    /** The database page that contains the tuple's data. */
    private DBPage dbPage;


    /** The offset in the page of the tuple's start. */
    private int pageOffset;


    /**
     * The columns that appear within the tuple.  We don't use a {@link Schema}
     * object so that we can use this class in a wider range of contexts.
     */
    private List<ColumnInfo> colInfos;


    /**
     * This array contains the cached offsets of each value in this tuple.
     * The array is populated when a tuple is constructed.  For columns with
     * a value of <tt>NULL</tt>, the offset will be 0.
     *
     * @see #NULL_OFFSET
     */
    private int[] valueOffsets;


    /**
     * The offset in the page where the tuple's data ends.  Note that this value
     * is <u>one byte past</u> the end of the tuple's data; as with most Java
     * sequences, the starting offset is inclusive and the ending offset is
     * exclusive.  Also, as a consequence, this value could be past the end of
     * the byte-array that the tuple resides in, if the tuple is at the end of
     * the byte-array.
     */
    private int endOffset;


    /**
     * Construct a new tuple object that is backed by the data in the database
     * page.  This tuple is able to be read from or written to.
     *
     * @param dbPage the specific database page that holds the tuple
     * @param pageOffset the offset of the tuple's actual data in the page
     * @param colInfos the details of the columns that appear within the tuple
     */
    public PageTuple(DBPage dbPage, int pageOffset, List<ColumnInfo> colInfos) {

        if (dbPage == null)
            throw new NullPointerException("dbPage must be specified");

        if (pageOffset < 0 || pageOffset >= dbPage.getPageSize()) {
            throw new IllegalArgumentException("pageOffset must be in range [0, " +
                dbPage.getPageSize() + "); got " + pageOffset);
        }

        this.dbPage = dbPage;
        this.pageOffset = pageOffset;
        this.colInfos = colInfos;

        valueOffsets = new int[colInfos.size()];

        computeValueOffsets();
    }


    /**
     * Since page-tuples are backed by data pages managed by the Buffer Manager,
     * they are <em>not</em> cacheable.
     *
     * @return <tt>false</tt> because page-tuples are never cacheable.
     */
    public boolean isCacheable() {
        return false;
    }


    public FilePointer getFilePointer() {
        return new FilePointer(dbPage.getPageNo(), pageOffset);
    }


    public DBPage getDBPage() {
        return dbPage;
    }


    public int getOffset() {
        return pageOffset;
    }


    public int getEndOffset() {
        return endOffset;
    }


    public List<ColumnInfo> getColumnInfos() {
        return Collections.unmodifiableList(colInfos);
    }


    /**
     * This helper method checks the column index for being in the proper
     * range of values.
     *
     * @param colIndex the column index to check
     *
     * @throws java.lang.IllegalArgumentException if the specified column
     *         index is out of range.
     */
    private void checkColumnIndex(int colIndex) {
        if (colIndex < 0 || colIndex >= colInfos.size()) {
            throw new IllegalArgumentException("Column index must be in range [0," +
                (colInfos.size() - 1) + "], got " + colIndex);
        }
    }


    /**
     * Returns the number of attributes in the tuple.  Note that this value
     * may be zero.
     */
    public int getColumnCount() {
        return colInfos.size();
    }


    /**
     * This is a helper function to find out the current value of a column's
     * <tt>NULL</tt> flag.  It is not intended to be used to determine if a
     * column's value is <tt>NULL</tt> since the method does a lot of work;
     * instead, use the {@link #isNullValue} method which relies on cached
     * column information.
     *
     * @param colIndex the index of the column to retrieve the null-flag for
     *
     * @return <tt>true</tt> if the column is null, or <tt>false</tt> otherwise
     */
    private boolean getNullFlag(int colIndex) {
        checkColumnIndex(colIndex);

        // Skip to the byte that contains the NULL-flag for this specific column.
        int nullFlagOffset = pageOffset + (colIndex / 8);

        // Shift the flags in that byte right, so that the flag for the
        // requested column is in the least significant bit (LSB).
        int nullFlag = dbPage.readUnsignedByte(nullFlagOffset);
        nullFlag = nullFlag >> (colIndex % 8);

        // If the LSB is 1 then the column's value is NULL.
        return ((nullFlag & 0x01) != 0);
    }


    /**
     * This is a helper function to set or clear the value of a column's NULL
     * flag.
     *
     * @param colIndex the index of the column to retrieve the null-flag for
     *
     * @param value <tt>true</tt> if the column is null, or <tt>false</tt>
     *        otherwise
     */
    private void setNullFlag(int colIndex, boolean value) {
        checkColumnIndex(colIndex);

        // Skip to the byte that contains the NULL-flag for this specific column.
        int nullFlagOffset = pageOffset + (colIndex / 8);

        // Create a bit-mask for setting or clearing the specified NULL flag,
        // then set/clear the flag in the mask byte.
        int mask = 1 << (colIndex % 8);

        int nullFlag = dbPage.readUnsignedByte(nullFlagOffset);

        if (value)
            nullFlag = nullFlag | mask;
        else
            nullFlag = nullFlag & ~mask;

        dbPage.writeByte(nullFlagOffset, nullFlag);
    }


    /**
     * Returns the offset where the tuple's data actually starts.  This is
     * past the bytes used to store NULL-flags.
     *
     * @return the starting index of the tuple's data
     */
    private int getDataStartOffset() {
        // Compute how many bytes the NULL flags take, at the start of the
        // tuple data.
        int nullFlagBytes = getNullFlagsSize(colInfos.size());
        return pageOffset + nullFlagBytes;
    }


    /**
     * This helper function computes and caches the offset of each column
     * value in the tuple.  If a column has a <tt>NULL</tt> value then
     * {@link #NULL_OFFSET} is used for the offset.
     */
    private void computeValueOffsets() {
        int numCols = colInfos.size();

        int valOffset = getDataStartOffset();

        for (int iCol = 0; iCol < numCols; iCol++) {
            if (getNullFlag(iCol)) {
                // This column is marked as being NULL.
                valueOffsets[iCol] = NULL_OFFSET;
            }
            else {
                // This column is not NULL.  Store the current offset, then
                // move forward past this value's bytes.
                valueOffsets[iCol] = valOffset;

                ColumnType colType = colInfos.get(iCol).getType();
                valOffset += getColumnValueSize(colType, valOffset);
            }
        }
        
        endOffset = valOffset;
    }


    /**
     * Returns the number of bytes used by the column-value at the specified
     * offset, with the specified type.
     *
     * @param colType the column type, which includes both the basic SQL data
     *        type (e.g. <tt>VARCHAR</tt>) and the size/length of the column's
     *        type.
     *
     * @param valueOffset the offset in the data where the column's value
     *        actually starts
     *
     * @return the total number of bytes used by the column-value
     */
    private int getColumnValueSize(ColumnType colType, int valueOffset) {
        // VARCHAR is special - the storage size depends on the size of the
        // data value being stored.  In this case, read out the data length.
        int dataLength = 0;
        if (colType.getBaseType() == SQLDataType.VARCHAR)
            dataLength = dbPage.readUnsignedShort(valueOffset);

        return getStorageSize(colType, dataLength);
    }


    /**
     * Returns true if the specified column is currently set to the SQL
     * <tt>NULL</tt> value.
     *
     * @return <tt>true</tt> if the specified column is currently set to
     *         <tt>NULL</tt>, or <tt>false</tt> otherwise.
     */
    public boolean isNullValue(int colIndex) {
        checkColumnIndex(colIndex);
        return (valueOffsets[colIndex] == NULL_OFFSET);
    }


    /**
     * Returns the specified column's value as an <code>Object</code>
     * reference.  The actual type of the object depends on the column type,
     * and follows this mapping:
     * <ul>
     *   <li><tt>INTEGER</tt> produces {@link java.lang.Integer}</li>
     *   <li><tt>SMALLINT</tt> produces {@link java.lang.Short}</li>
     *   <li><tt>BIGINT</tt> produces {@link java.lang.Long}</li>
     *   <li><tt>TINYINT</tt> produces {@link java.lang.Byte}</li>
     *   <li><tt>FLOAT</tt> produces {@link java.lang.Float}</li>
     *   <li><tt>DOUBLE</tt> produces {@link java.lang.Double}</li>
     *   <li><tt>CHAR(<em>n</em>)</tt> produces {@link java.lang.String}</li>
     *   <li><tt>VARCHAR(<em>n</em>)</tt> produces {@link java.lang.String}</li>
     * </ul>
     */
    public Object getColumnValue(int colIndex) {
        checkColumnIndex(colIndex);

        Object value = null;
        if (!isNullValue(colIndex)) {
            int offset = valueOffsets[colIndex];

            ColumnType colType = colInfos.get(colIndex).getType();
            switch (colType.getBaseType()) {

            case INTEGER:
                value = Integer.valueOf(dbPage.readInt(offset));
                break;

            case SMALLINT:
                value = Short.valueOf(dbPage.readShort(offset));
                break;

            case BIGINT:
                value = Long.valueOf(dbPage.readLong(offset));
                break;

            case TINYINT:
                value = Byte.valueOf(dbPage.readByte(offset));
                break;

            case FLOAT:
                value = Float.valueOf(dbPage.readFloat(offset));
                break;

            case DOUBLE:
                value = Double.valueOf(dbPage.readDouble(offset));
                break;

            case CHAR:
                value = dbPage.readFixedSizeString(offset, colType.getLength());
                break;

            case VARCHAR:
                value = dbPage.readVarString65535(offset);
                break;

            default:
                throw new UnsupportedOperationException(
                    "Cannot currently store type " + colType.getBaseType());
            }
        }

        return value;
    }


    /**
     * Sets the column to the specified value, or <tt>NULL</tt> if the value is
     * the Java <tt>null</tt> value.
     *
     * @param colIndex The index of the column to set.
     *
     * @param value the value to set the column to, or <tt>null</tt> if the
     *        column should be set to the SQL <tt>NULL</tt> value.
     */
    public void setColumnValue(int colIndex, Object value) {
        checkColumnIndex(colIndex);

        if (value == null) {
            // TODO:  Make sure the column allows NULL values.
            setNullColumnValue(colIndex);
        }
        else {
            // Update the value stored in the tuple.
            setNonNullColumnValue(colIndex, value);
        }
    }


    /**
     * This helper function is used by the {@link #setColumnValue} method in
     * the specific case when a column is being set to the SQL <tt>NULL</tt>
     * value.
     *
     * @param iCol the index of the column to set to <tt>NULL</tt>
     */
    private void setNullColumnValue(int iCol) {

        // If the column value wasn't already NULL then we need to shrink down
        // the tuple within the page.
        if (!isNullValue(iCol)) {
            // Mark the value as NULL in the NULL-flags.
            setNullFlag(iCol, true);

            // Figure out how many bytes the current value takes, then shrink
            // the tuple.  (The TableManager will also update other affected
            // tuples' slots.)

            ColumnType colType = colInfos.get(iCol).getType();
            int dataLength = 0;
            if (colType.getBaseType() == SQLDataType.VARCHAR)
            dataLength = dbPage.readUnsignedShort(valueOffsets[iCol]);

            int valueSize = getStorageSize(colType, dataLength);

            deleteTupleDataRange(valueOffsets[iCol], valueSize);

            // Update all affected value-offsets within this tuple.

            for (int jCol = 0; jCol < iCol; jCol++) {
                if (valueOffsets[jCol] != NULL_OFFSET)
                    valueOffsets[jCol] += valueSize;
            }

            valueOffsets[iCol] = NULL_OFFSET;
        }
    }


    /**
     * This helper function is used by the {@link #setColumnValue} method in
     * the specific case when a column is being set to a non-<tt>NULL</tt>
     * value.
     *
     * @todo This function has a bug - it doesn't adjust subsequent columns'
     *       offsets properly.  Also, keep in mind that null column-offsets
     *       are 0 and shouldn't be adjusted!
     *
     * @param colIndex The index of the column to set.
     *
     * @param value the value to set the column to.
     *
     * @throws NullPointerException if the specified value is <tt>null</tt>
     */
    private void setNonNullColumnValue(int colIndex, Object value) {

        if (value == null)
            throw new NullPointerException();

        ColumnInfo colInfo = colInfos.get(colIndex);
        ColumnType colType = colInfo.getType();

        int oldDataSize, newDataSize;

        // This will be the offset of where to store the new non-null value.
        // However, if the current value is NULL then offset will be set to
        // NULL_OFFSET, so we need to compute the actual offset for the column
        // value to be stored at.

        int offset = valueOffsets[colIndex];
        if (offset == NULL_OFFSET) {
            // Find the last column before this one that is not currently NULL.
            // That column's offset, PLUS its size, will give us the offset of
            // this column.  (We could also look for the next non-NULL column,
            // but there may be no more non-NULL columns in the tuple, and we
            // wouldn't have an easy way of determining the proper offset.)

            int prevCol = colIndex - 1;
            while (prevCol >= 0 && valueOffsets[prevCol] == NULL_OFFSET)
                prevCol--;

            if (prevCol < 0) {
                // This value will be added to the front of the tuple's data!
                offset = getDataStartOffset();
            }
            else {
                // This value will be added after the previous non-NULL value
                // that we just found.
                int prevOffset = valueOffsets[prevCol];
                ColumnType prevType = colInfos.get(prevCol).getType();
                offset = prevOffset + getColumnValueSize(prevType, prevOffset);
            }

            oldDataSize = 0;
        }
        else {
            oldDataSize = getColumnValueSize(colType, offset);
        }

        // Next, make sure there is space for the new value.  If the column
        // being written is a variable-size column, we may need to increase or
        // decrease the size of the tuple to make room.

        // VARCHAR is special - the storage size depends on the size of the
        // data value being stored.
        int newDataLength = 0;
        if (colType.getBaseType() == SQLDataType.VARCHAR) {
            String strValue = TypeConverter.getStringValue(value);
            newDataLength = strValue.length();
        }
        newDataSize = getStorageSize(colType, newDataLength);

        if (newDataSize > oldDataSize)
            insertTupleDataRange(offset, newDataSize - oldDataSize);
        else
            deleteTupleDataRange(offset, oldDataSize - newDataSize);

        // Finally, write the value to the column!

        writeNonNullValue(dbPage, offset, colType, value);
    }


    /**
     * This method computes and returns the number of bytes that are used to
     * store the null-flags in each tuple.
     *
     * @param numCols the total number of columns in the table
     *
     * @return the number of bytes used for the null-bitmap.
     *
     * @review (donnie) This is really a table-file-level computation, not a
     *         tuple-level computation.
     */
    public static int getNullFlagsSize(int numCols) {
        if (numCols < 0) {
            throw new IllegalArgumentException("numCols must be >= 0; got " +
                numCols);
        }

        int nullFlagsSize = 0;
        if (numCols > 0)
            nullFlagsSize = 1 + (numCols - 1) / 8;

        return nullFlagsSize;
    }


    /**
     * Returns the storage size of a particular column's (non-<tt>NULL</tt>)
     * value, in bytes.  The length of the value is required in cases where
     * the column value can be variable size, such as if the type is a
     * <tt>VARCHAR</tt>.  Note that the data-length is actually <em>not</em>
     * required when the type is <tt>CHAR</tt>, since <tt>CHAR</tt> fields
     * always have a specific size.
     *
     * @param colType the column's data type
     * @param dataLength for column-types that specify a length, this is the
     *        length value.
     *
     * @return the storage size of the data in bytes
     */
    public static int getStorageSize(ColumnType colType, int dataLength) {
        int size;

        switch (colType.getBaseType()) {

        case INTEGER:
        case FLOAT:
            size = 4;
            break;

        case SMALLINT:
            size = 2;
            break;

        case BIGINT:
        case DOUBLE:
            size = 8;
            break;

        case TINYINT:
            size = 1;
            break;

        case CHAR:
            // CHAR values are of a fixed size, but the size is specified in
            // the length field and there is no other storage required.
            size = colType.getLength();
            break;

        case VARCHAR:
            // VARCHAR values are of a variable size, but there is always a
            // two byte length specified at the start of the value.
            size = 2 + dataLength;
            break;

        default:
            throw new UnsupportedOperationException(
                "Cannot currently store type " + colType.getBaseType());
        }

        return size;
    }




    /**
     * This helper function takes a tuple (from an arbitrary source) and
     * computes how much space it would require to be stored in a heap table
     * file with the specified schema.  This is used to insert new tuples into
     * a table file by computing how much space will be needed, so that an
     * appropriate page can be found.
     *
     * @review (donnie) It doesn't make sense to have this be a non-static
     *         method, since a page-tuple references a specific tuple, not a
     *         data page.  However, having this as a static method on this
     *         class doesn't seem too bad.
     *
     * @param columns the schema information for the table that the tuple will
     *        be stored into
     *
     * @param tuple the tuple to compute the storage size for
     *
     * @return the total size for storing the tuple's data in bytes
     */
    public static int getTupleStorageSize(List<ColumnInfo> columns,
        Tuple tuple) {

        if (columns.size() != tuple.getColumnCount()) {
            throw new IllegalArgumentException(
                "Tuple has different arity than target schema.");
        }

        int storageSize = getNullFlagsSize(columns.size());
        int iCol = 0;
        for (ColumnInfo colInfo : columns) {

            ColumnType colType = colInfo.getType();
            Object value = tuple.getColumnValue(iCol);

            // If the value is NULL (represented by Java's null here...) then
            // it takes no space.  Otherwise, compute the space taken by this
            // value.
            if (value != null) {
                // VARCHAR is special - the storage size depends on the size
                // of the data value being stored.
                int dataLength = 0;
                if (colType.getBaseType() == SQLDataType.VARCHAR) {
                    String strValue = TypeConverter.getStringValue(value);
                    dataLength = strValue.length();
                }

                storageSize += getStorageSize(colType, dataLength);
            }

            iCol++;
        }

        return storageSize;
    }


    public static void storeTuple(DBPage dbPage, int pageOffset,
                                  List<ColumnInfo> colInfos, Tuple tuple) {

        if (colInfos.size() != tuple.getColumnCount()) {
            throw new IllegalArgumentException(
            "Tuple has different arity than target schema.");
        }

        // Start writing data just past the NULL-flag bytes.
        int currOffset = pageOffset + getNullFlagsSize(colInfos.size());
        int iCol = 0;
        for (ColumnInfo colInfo : colInfos) {

            ColumnType colType = colInfo.getType();
            Object value = tuple.getColumnValue(iCol);
            int dataSize = 0;

            // If the value is NULL (represented by Java's null here) then set
            // the corresponding NULL-flag.  Otherwise, write the value.
            if (value == null) {
                setNullFlag(dbPage, pageOffset, iCol, true);
            }
            else {
                // Write in the data value.
                setNullFlag(dbPage, pageOffset, iCol, false);
                dataSize = writeNonNullValue(dbPage, currOffset, colType, value);
            }

            currOffset += dataSize;
            iCol++;
        }
    }


    /**
     * This is a helper function to set or clear the value of a column's
     * <tt>NULL</tt> flag.
     *
     * @param dbPage the file-page that the value will be written into
     *
     * @param tupleStart the byte-offset in the page where the tuple starts
     *
     * @param colIndex the index of the column to set the null-flag for
     *
     * @param value the new value for the null-flag
     */
    public static void setNullFlag(DBPage dbPage, int tupleStart,
        int colIndex, boolean value) {

        //checkColumnIndex(colIndex);

        // Skip to the byte that contains the NULL-flag for this specific column.
        int nullFlagOffset = tupleStart + (colIndex / 8);

        // Create a bit-mask for setting or clearing the specified NULL flag, then
        // set/clear the flag in the mask byte.
        int mask = 1 << (colIndex % 8);

        int nullFlag = dbPage.readUnsignedByte(nullFlagOffset);

        if (value)
            nullFlag = nullFlag | mask;
        else
            nullFlag = nullFlag & ~mask;

        dbPage.writeByte(nullFlagOffset, nullFlag);
    }



    /**
     * This helper function is used by the {@link #setColumnValue} method in
     * the specific case when a column is being set to a non-<tt>NULL</tt>
     * value.
     *
     * @param dbPage the file-page that the value will be written into
     * @param offset the actual byte-offset in the page where the value is
     *        written to
     * @param colType the type of the column that the value is being written for
     * @param value the non-<tt>null</tt> value to store
     *
     * @return The number of bytes written for the specified value.
     *
     * @throws NullPointerException if <tt>dbPage</tt> is <tt>null</tt>, or if
     *         <tt>value</tt> is <tt>null</tt>.
     */
    public static int writeNonNullValue(DBPage dbPage, int offset,
        ColumnType colType, Object value) {
        return dbPage.writeObject(offset, colType, value);
    }


    protected abstract void insertTupleDataRange(int off, int len);


    protected abstract void deleteTupleDataRange(int off, int len);


    /**
     * This method clears the page-tuple's state so that it is completely
     * invalid and unusable.  This is used in circumstances where the tuple's
     * backing data is no longer available, such as when the tuple is deleted.
    public void setInvalid() {
        dbPage = null;
        slot = -1;
        pageOffset = -1;
        tblInfo = null;
        schema = null;
        valueOffsets = null;
        tupleSize = -1;
    }
     */
}

