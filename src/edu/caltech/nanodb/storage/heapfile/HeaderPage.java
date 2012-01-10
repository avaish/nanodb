package edu.caltech.nanodb.storage.heapfile;


import java.util.ArrayList;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.qeval.ColumnStats;
import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageReader;
import edu.caltech.nanodb.storage.PageWriter;
import edu.caltech.nanodb.storage.TableFileInfo;


/**
 * <p>
 * This class contains constants and basic functionality for accessing and
 * manipulating the contents of the header page of a heap table-file.  <b>Note
 * that the first two bytes of the first page is always devoted to the type and
 * page-size of the data file.</b>  (See {@link edu.caltech.nanodb.storage.DBFile}
 * for details.)  All other values must follow the first two bytes.
 * </p>
 * <p>
 * Heap table-file header pages are laid out as follows:
 * </p>
 * <ul>
 *   <li>As with all <tt>DBFile</tt>s, the first two bytes are the file type
 *       and page size, as always.</li>
 *   <li>After this come several values specifying the sizes of various areas in
 *       the header page, including the size of the table's schema specification,
 *       the statistics for the table, and the number of columns.</li>
 *   <li>Next the table's schema is recorded in the header page.  See the
 *       {@link HeapFileTableManager#initTableInfo} and
 *       {@link HeapFileTableManager#loadTableInfo} methods for details on how
 *       these values are stored.</li>
 *   <li>Finally, the table's statistics are stored.</li>
 * </ul>
 * <p>
 * Even with all this information, usually only a few hundred bytes are required
 * for storing the details of most tables.
 * </p>
 *
 * @design (Donnie) Why is this class a static class, instead of a wrapper class
 *         around the {@link DBPage}?  No particular reason, really.  The class
 *         is used relatively briefly when a table is being accessed, and there
 *         is no real need for it to manage its own object-state, so it was just
 *         as convenient to provide all functionality as static methods.  This
 *         avoids the (small) overhead of instantiating an object as well.  But
 *         really, these are not particularly serious concerns.
 */
public class HeaderPage {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(HeaderPage.class);


    /**
     * The offset in the header page where the size of the table schema is
     * stored.  This value is an unsigned short.
     */
    public static final int OFFSET_SCHEMA_SIZE = 2;


    /**
     * The offset in the header page where the size of the table statistics are
     * stored.  This value is an unsigned short.
     */
    public static final int OFFSET_STATS_SIZE = 4;


    /**
     * The offset in the header page where the number of columns is stored.
     * This value is an unsigned byte.
     */
    public static final int OFFSET_NCOLS = 6;


    /**
     * The offset in the header page where the column descriptions start.
     * There could be as many as 255 column descriptions, so this is only the
     * beginning of that data!  This value is an unsigned short.
     */
    public static final int OFFSET_COL_DESCS = 7;


    /**
     * This is the <em>relative</em> offset of the number of data-pages in the
     * table file, relative to the start of the table statistics.  This value is
     * an unsigned short, since we constrain all data files to have at most 64K
     * total pages.
     *
     * @see #getStatsOffset
     */
    public static final int RELOFF_NUM_DATA_PAGES = 0;


    /**
     * This is the <em>relative</em> offset of the number of tuples in the data
     * file, relative to the start of the table statistics.  This value is a
     * signed integer (4 bytes), since it is highly unlikely (in fact, currently
     * impossible) that each tuple could be a single byte.
     *
     * @see #getStatsOffset
     */
    public static final int RELOFF_NUM_TUPLES = 2;


    /**
     * This is the <em>relative</em> offset of the average tuple-size in the
     * data file, relative to the start of the table statistics.  This value is
     * a float (4 bytes).
     * <p>
     * Note that this value is not just the number of pages multiplied by the
     * page size, then divided by the number of tuples.  Data pages may have a
     * lot of empty space in them, and this value does not reflect that empty
     * space; it reflects the actual number of bytes that comprise a tuple, on
     * average, for the data file.
     *
     * @see #getStatsOffset
     */
    public static final int RELOFF_AVG_TUPLE_SIZE = 6;


    /**
     * This is the <em>relative</em> offset of the column statistics in the
     * data file, relative to the start of the table statistics.  The column
     * statistics occupy a variable number of bytes, depending both on the
     * number of columns in the table, and also on whether the stats have
     * actually been computed for the table.
     *
     * @see #getStatsOffset
     */
    public static final int RELOFF_COLUMN_STATS = 10;


    /**
     * A bit-mask used for storing column-stats, to record whether or not the
     * "number of distinct values" value is present for the column.
     */
    private static final int COLSTAT_NULLMASK_NUM_DISTINCT_VALUES = 0x08;

    /**
     * A bit-mask used for storing column-stats, to record whether or not the
     * "number of <tt>NULL</tt> values" value is present for the column.
     */
    private static final int COLSTAT_NULLMASK_NUM_NULL_VALUES = 0x04;

    /**
     * A bit-mask used for storing column-stats, to record whether or not the
     * "minimum value" value is present for the column.
     */
    private static final int COLSTAT_NULLMASK_MIN_VALUE = 0x02;

    /**
     * A bit-mask used for storing column-stats, to record whether or not the
     * "maximum value" value is present for the column.
     */
    private static final int COLSTAT_NULLMASK_MAX_VALUE = 0x01;


    /**
     * This helper method simply verifies that the data page provided to the
     * <tt>HeaderPage</tt> class is in fact a header-page (i.e. page 0 in the
     * data file).
     *
     * @param dbPage the page to check
     *
     * @throws IllegalArgumentException if <tt>dbPage</tt> is <tt>null</tt>, or
     *         if it's not actually page 0 in the table file
     */
    private static void verifyIsHeaderPage(DBPage dbPage) {
        if (dbPage == null)
            throw new IllegalArgumentException("dbPage cannot be null");

        if (dbPage.getPageNo() != 0) {
            throw new IllegalArgumentException(
                "Page 0 is the header page in this storage format; was given page " +
                dbPage.getPageNo());
        }
    }


    /**
     * Returns the number of bytes that the table's schema occupies for storage
     * in the header page.
     *
     * @param dbPage the header page of the heap table file
     * @return the number of bytes that the table's schema occupies
     */
    public static int getSchemaSize(DBPage dbPage) {
        verifyIsHeaderPage(dbPage);
        return dbPage.readUnsignedShort(OFFSET_SCHEMA_SIZE);
    }


    /**
     * Sets the number of bytes that the table's schema occupies for storage
     * in the header page.
     *
     * @param dbPage the header page of the heap table file
     * @param numBytes the number of bytes that the table's schema occupies
     */
    public static void setSchemaSize(DBPage dbPage, int numBytes) {
        verifyIsHeaderPage(dbPage);

        if (numBytes < 0) {
            throw new IllegalArgumentException(
                "numButes must be >= 0; got " + numBytes);
        }

        dbPage.writeShort(OFFSET_SCHEMA_SIZE, numBytes);
    }


    /**
     * Returns the number of bytes that the table's statistics occupy for
     * storage in the header page.
     *
     * @param dbPage the header page of the heap table file
     * @return the number of bytes that the table's statistics occupy
     */
    public static int getStatsSize(DBPage dbPage) {
        verifyIsHeaderPage(dbPage);
        return dbPage.readUnsignedShort(OFFSET_STATS_SIZE);
    }


    /**
     * Sets the number of bytes that the table's statistics occupy for storage
     * in the header page.
     *
     * @param dbPage the header page of the heap table file
     * @param numBytes the number of bytes that the table's statistics occupy
     */
    public static void setStatsSize(DBPage dbPage, int numBytes) {
        verifyIsHeaderPage(dbPage);

        if (numBytes < 0) {
            throw new IllegalArgumentException(
                "numButes must be >= 0; got " + numBytes);
        }

        dbPage.writeShort(OFFSET_STATS_SIZE, numBytes);
    }


    /**
     * Returns the offset in the header page that the table statistics start at.
     * This value changes because the table schema resides before the stats, and
     * therefore the stats don't live at a fixed location.
     *
     * @param dbPage the header page of the heap table file
     * @return the offset within the header page that the table statistics
     *         reside at
     */
    public static int getStatsOffset(DBPage dbPage) {
        verifyIsHeaderPage(dbPage);

        return OFFSET_NCOLS + getSchemaSize(dbPage);
    }


    /**
     * Updates the "number of data pages" statistic for this heap file.
     *
     * @param dbPage the header page of the heap file.
     * @param numPages the "number of data pages" value to store.
     */
    public static void setStatNumDataPages(DBPage dbPage, int numPages) {
        verifyIsHeaderPage(dbPage);

        int offset = getStatsOffset(dbPage) + RELOFF_NUM_DATA_PAGES;
        dbPage.writeShort(offset, numPages);
    }


    /**
     * Returns the "number of data pages" statistic for this heap file.
     *
     * @param dbPage the header page of the heap file.
     * @return the "number of data pages" value to store.
     */
    public static int getStatNumDataPages(DBPage dbPage) {
        verifyIsHeaderPage(dbPage);

        int offset = getStatsOffset(dbPage) + RELOFF_NUM_DATA_PAGES;
        return dbPage.readUnsignedShort(offset);
    }


    /**
     * Updates the "number of tuples" statistic for this heap file.
     *
     * @param dbPage the header page of the heap file.
     * @param numTuples the "number of tuples" value to store.
     */
    public static void setStatNumTuples(DBPage dbPage, int numTuples) {
        verifyIsHeaderPage(dbPage);

        int offset = getStatsOffset(dbPage) + RELOFF_NUM_TUPLES;
        // Casting long to int here is fine, since we are writing an
        // unsigned int.
        dbPage.writeInt(offset, numTuples);
    }


    /**
     * Returns the "number of tuples" statistic for this heap file.
     *
     * @param dbPage the header page of the heap file.
     * @return the "number of tuples" value to store.
     */
    public static int getStatNumTuples(DBPage dbPage) {
        verifyIsHeaderPage(dbPage);

        int offset = getStatsOffset(dbPage) + RELOFF_NUM_TUPLES;
        return dbPage.readInt(offset);
    }


    /**
     * Updates the "average tuple size" statistic for this heap file.
     *
     * @param dbPage the header page of the heap file.
     * @param avgTupleSize the "average tuple size" value to store.
     */
    public static void setStatAvgTupleSize(DBPage dbPage, float avgTupleSize) {
        verifyIsHeaderPage(dbPage);

        int offset = getStatsOffset(dbPage) + RELOFF_AVG_TUPLE_SIZE;
        dbPage.writeFloat(offset, avgTupleSize);
    }


    /**
     * Returns the "average tuple size" statistic for this heap file.
     *
     * @param dbPage the header page of the heap file.
     * @return the "average tuple size" value to store.
     */
    public static float getStatAvgTupleSize(DBPage dbPage) {
        verifyIsHeaderPage(dbPage);

        int offset = getStatsOffset(dbPage) + RELOFF_AVG_TUPLE_SIZE;
        return dbPage.readFloat(offset);
    }


    public static TableStats getTableStats(DBPage dbPage,
                                           TableFileInfo tblFileInfo) {
        verifyIsHeaderPage(dbPage);

        logger.debug("Reading table-statistics from header page.");

        PageReader reader = new PageReader(dbPage);
        reader.setPosition(getStatsOffset(dbPage) + RELOFF_COLUMN_STATS);

        Schema schema = tblFileInfo.getSchema();
        ArrayList<ColumnStats> colStats = new ArrayList<ColumnStats>();
        for (int i = 0; i < schema.numColumns(); i++) {
            // The column-statistics object is initialized to all NULL values.
            ColumnStats c = new ColumnStats();
            ColumnInfo colInfo = schema.getColumnInfo(i);

            // Read each column-stat's NULL-mask.  If the null-bit is 0 then a
            // value is present.
            byte nullMask = reader.readByte();

            if ((nullMask & COLSTAT_NULLMASK_NUM_DISTINCT_VALUES) == 0)
                c.setNumUniqueValues(reader.readInt());

            if ((nullMask & COLSTAT_NULLMASK_NUM_NULL_VALUES) == 0)
                c.setNumNullValues(reader.readInt());

            if ((nullMask & COLSTAT_NULLMASK_MIN_VALUE) == 0)
                c.setMinValue(reader.readObject(colInfo.getType()));

            if ((nullMask & COLSTAT_NULLMASK_MAX_VALUE) == 0)
                c.setMaxValue(reader.readObject(colInfo.getType()));

            logger.debug(String.format("Read column-stat data:  " +
                "nullmask=0x%X, unique=%d, null=%d, min=%s, max=%s",
                nullMask, c.getNumUniqueValues(), c.getNumNullValues(),
                c.getMinValue(), c.getMaxValue()));

            colStats.add(c);
        }

        return new TableStats(getStatNumDataPages(dbPage),
            getStatNumTuples(dbPage), getStatAvgTupleSize(dbPage), colStats);
    }


    public static void setTableStats(DBPage dbPage, TableFileInfo tblFileInfo) {
        verifyIsHeaderPage(dbPage);

        logger.debug("Writing table-statistics to header page.");

        Schema schema = tblFileInfo.getSchema();
        TableStats stats = tblFileInfo.getStats();

        setStatNumDataPages(dbPage, stats.numDataPages);
        setStatNumTuples(dbPage, stats.numTuples);
        setStatAvgTupleSize(dbPage, stats.avgTupleSize);

        PageWriter writer = new PageWriter(dbPage);
        writer.setPosition(getStatsOffset(dbPage) + RELOFF_COLUMN_STATS);

        ArrayList<ColumnStats> colStats = stats.getAllColumnStats();
        for (int i = 0; i < colStats.size(); i++) {
            ColumnStats c = colStats.get(i);
            ColumnInfo colInfo = schema.getColumnInfo(i);

            // There are three values per column-stat, and any of them can be
            // null.  Therefore, each column-stat gets its own NULL-mask.
            byte nullMask = 0;

            int numUnique = c.getNumUniqueValues();
            int numNull   = c.getNumNullValues();
            Object minVal = c.getMinValue();
            Object maxVal = c.getMaxValue();

            // Build up the NULL-mask.

            if (numUnique == -1)
                nullMask |= COLSTAT_NULLMASK_NUM_DISTINCT_VALUES;

            if (numNull == -1)
                nullMask |= COLSTAT_NULLMASK_NUM_NULL_VALUES;

            if (minVal == null)
                nullMask |= COLSTAT_NULLMASK_MIN_VALUE;

            if (maxVal == null)
                nullMask |= COLSTAT_NULLMASK_MAX_VALUE;

            // Store the NULL-mask, then store the non-NULL values.

            logger.debug(String.format("Writing column-stat data:  " +
                "nullmask=0x%X, unique=%d, null=%d, min=%s, max=%s",
                nullMask, numUnique, numNull, minVal, maxVal));

            writer.writeByte(nullMask);

            if (numUnique != -1)
                writer.writeInt(numUnique);

            if (numNull != -1)
                writer.writeInt(numNull);

            if (minVal != null)
                writer.writeObject(colInfo.getType(), minVal);

            if (maxVal != null)
                writer.writeObject(colInfo.getType(), maxVal);
        }

        int statsSize = writer.getPosition() - getStatsOffset(dbPage);
        setStatsSize(dbPage, statsSize);

        logger.debug("Writing statistics completed.  Total size is " +
            statsSize + " bytes.");
    }
}
