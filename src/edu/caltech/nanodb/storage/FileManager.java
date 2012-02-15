package edu.caltech.nanodb.storage;


import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


/**
 * The File Manager provides unbuffered, low-level operations for working with
 * paged data files.  It really doesn't know anything about the internal file
 * formats of the data files, except that the first two bytes of the first page
 * must specify the type and page size for the data file.  (This is a
 * requirement of {@link #openDBFile}.)
 *
 * @design Although it might make more sense to put per-file operations like
 *         "load page" and "store page" on the {@link DBFile} class, we provide
 *         higher-level operations on the Storage Manager so that we can provide
 *         global buffering capabilities in one place.
 */
public class FileManager {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(FileManager.class);


    /**
     * The base directory that the file-manager should use for creating and
     * opening files.
     */
    private File baseDir;


    /**
     * Create a file-manager instance that uses the specified base directory.
     * 
     * @param baseDir the base-directory that the file-manager should use
     */
    public FileManager(File baseDir) {
        if (baseDir == null)
            throw new IllegalArgumentException("baseDir cannot be null");

        if (!baseDir.isDirectory()) {
            throw new IllegalArgumentException("baseDir value " + baseDir +
               " is not a directory");
        }

        this.baseDir = baseDir;
    }
    
    

    /**
     * This helper function calculates the file-position of the specified page.
     * Obviously, this value is dependent on the page size.
     *
     * @param dbFile the database file to compute the page-start for
     * @param pageNo the page number to access
     *
     * @return the offset of the specified page from the start of the database
     *         file
     *
     * @throws IllegalArgumentException if the page number is negative
     */
    private long getPageStart(DBFile dbFile, int pageNo) {
        if (pageNo < 0)
            throw new IllegalArgumentException("pageNo must be >= 0, got " + pageNo);

        long pageStart = pageNo;
        pageStart *= (long) dbFile.getPageSize();

        return pageStart;
    }


    /**
     * This method creates a new database file in the directory used by the
     * storage manager.  An exception is thrown if the file already exists.
     *
     * @param filename the name of the file to open to create the database file
     * @param type the type of database file being created
     * @param pageSize the page size to use when reading and writing the file
     *
     * @return a new database file object for the newly created file
     *
     * @throws IOException if the specified file already exists.
     * @throws IllegalArgumentException if the page size is not valid
     */
    public DBFile createDBFile(String filename, DBFileType type, int pageSize)
        throws IOException {

        File f = new File(baseDir, filename);
        if (f.exists())
            throw new IOException("File " + f + " already exists!");

        logger.debug("Creating new database file " + f + ".");
        return initDBFile(f, type, pageSize);
    }


    public DBFile initDBFile(File f, DBFileType type, int pageSize)
        throws IOException {

        logger.debug("Initializing database file " + f + ".");
        DBFile dbFile = new DBFile(f, type, pageSize);

        // Create the first page, and write the type and encoded page-size into
        // the first two bytes.  Then save it.

        DBPage firstPage = loadDBPage(dbFile, 0, true);
        firstPage.writeByte(0, type.getID());
        firstPage.writeByte(1, DBFile.encodePageSize(pageSize));
        saveDBPage(firstPage);

        return dbFile;
    }


    /**
     * This method opens a database file, and reads in the file's type and page
     * size from the first two bytes of the first page.  The method uses the
     * {@link RandomAccessFile#readUnsignedShort} method to read the page
     * size from the data file when it is opened.
     *
     * @param filename the name of the database file to open
     * @return the successfully opened database file
     *
     * @throws FileNotFoundException if the specified file doesn't exist.
     * @throws IOException if a more general IO issue occurs.
     */
    public DBFile openDBFile(String filename) throws IOException {

        File f = new File(baseDir, filename);
        if (!f.isFile())
            throw new FileNotFoundException("File " + f + " doesn't exist.");

        RandomAccessFile fileContents = new RandomAccessFile(f, "rw");

        int typeID = fileContents.readUnsignedByte();
        int pageSize = DBFile.decodePageSize(fileContents.readUnsignedByte());

        DBFileType type = DBFileType.valueOf(typeID);
        if (type == null)
            throw new IOException("Unrecognized file type ID " + typeID);

        DBFile dbFile;
        try {
            dbFile = new DBFile(f, type, pageSize, fileContents);
        }
        catch (IllegalArgumentException iae) {
            throw new IOException("Invalid page size " + pageSize +
                " specified for data file " + f, iae);
        }

        logger.debug(String.format("Opened existing database file %s; " +
            "type is %s, page size is %d.", f, type, pageSize));

        return dbFile;
    }


    /**
     * Loads a page from the underlying data file, and returns a new
     * {@link DBPage} object containing the data.  The <tt>create</tt> flag
     * controls whether an error is propagated, if the requested page is past
     * the end of the file.  (Note that if a new page is created, the file's
     * size will not reflect the new page until it is actually written to the
     * file.)
     * <p>
     * <em>This function does no page caching whatsoever.</em>  Requesting a
     * particular page multiple times will return multiple page objects, with
     * data loaded from the file each time.
     *
     * @param dbFile the database file to load the page from
     * @param pageNo the number of the page to load
     * @param create a flag specifying whether the page should be created if it
     *        doesn't already exist
     *
     * @return the newly loaded database page
     *
     * @throws IllegalArgumentException if the page number is negative
     *
     * @throws java.io.EOFException if the requested page is not in the data file,
     *         and the <tt>create</tt> flag is set to <tt>false</tt>.
     */
    public DBPage loadDBPage(DBFile dbFile, int pageNo, boolean create)
        throws IOException {

        if (pageNo < 0) {
            throw new IllegalArgumentException("pageNo must be >= 0, got " +
                pageNo);
        }

        // Create the page object, which will receive the data being read.
        DBPage page = new DBPage(dbFile, pageNo);

        long pageStart = getPageStart(dbFile, pageNo);

        RandomAccessFile fileContents = dbFile.getFileContents();
        fileContents.seek(pageStart);
        try {
            fileContents.readFully(page.getPageData());
        }
        catch (EOFException e) {
            if (create) {
                // Caller wants to create the page if it doesn't already exist
                // yet.  Don't let the exception propagate.

                logger.debug(String.format(
                    "Requested page %d doesn't yet exist in file %s; creating.",
                    pageNo, dbFile.getDataFile().getName()));

                // ...of course, we don't propagate the exception, but we also
                // don't actually extend the file's size until the page is
                // stored back to the file...
                long newLength = (1L + (long) pageNo) * (long) dbFile.getPageSize();

                // This check is just for safety.  It would be highly irregular
                // to get an EOF exception and then have the file actually be
                // longer than we expect.  But, if it happens, we'll scream.
                long oldLength = fileContents.length();
                if (oldLength < newLength) {
                    fileContents.setLength(newLength);
                    logger.debug("Set file " + dbFile + " length to " + newLength);
                }
                else {
                    String msg = "Expected DB file to be less than " +
                        newLength + " bytes long, but it's " + oldLength +
                        " bytes long!";

                    logger.error(msg);
                    throw new IOException(msg);
                }
            }
            else {
                // Caller expected the page to exist!  Let the exception propagate.
                throw e;
            }
        }

        return page;
    }


    /**
     * Loads a page from the underlying data file, and returns a new
     * {@link DBPage} object containing the data.  This method always reports an
     * {@link java.io.EOFException} if the specified page is past the end of the
     * database file.
     * <p>
     * <em>This function does no page caching whatsoever.</em>  Requesting a
     * particular page multiple times will return multiple page objects, with
     * data loaded from the file each time.
     * <p>
     * (This method is simply a wrapper of
     * {@link #loadDBPage(DBFile, int, boolean)}, passing <tt>false</tt> for
     * <tt>create</tt>.)
     *
     * @param dbFile the database file to load the page from
     * @param pageNo the number of the page to load
     *
     * @return the newly loaded database page
     *
     * @throws IllegalArgumentException if the page number is negative
     *
     * @throws java.io.EOFException if the requested page is not in the data file.
     */
    public DBPage loadDBPage(DBFile dbFile, int pageNo) throws IOException {
        return loadDBPage(dbFile, pageNo, false);
    }


    /**
     * Saves a page to the DB file, and then clears the page's dirty flag.
     * Note that the data might not actually be written to disk until a sync
     * operation is performed.
     *
     * @param page the page to write back to the data file
     *
     * @throws IOException if an error occurs while writing the page to disk
     */
    public void saveDBPage(DBPage page) throws IOException {
        if (page == null)
            throw new NullPointerException("page cannot be null");

        DBFile dbFile = page.getDBFile();

        long pageStart = getPageStart(dbFile, page.getPageNo());

        RandomAccessFile fileContents = dbFile.getFileContents();
        fileContents.seek(pageStart);
        fileContents.write(page.getPageData());
        page.setDirty(false);
    }


    /**
     * This method ensures that all file-writes on the specified DB-file have
     * actually been synchronized to the disk.  Note that even after a call to
     * {@link #saveDBPage}, the filesystem may postpone the write for various
     * reasons, or disks may also buffer the write operations in order to
     * optimize their storage to disk.  This method ensures that any buffered
     * writes will actually be written to the disk.
     *
     * @param dbFile the database file to synchronize
     *
     * @throws java.io.SyncFailedException if the synchronization operation
     *         cannot be guaranteed successful, or if it fails for some reason.
     *
     * @throws IOException if some other IO problem occurs.
     */
    public void syncDBFile(DBFile dbFile) throws IOException {
        logger.info("Synchronizing database file to disk:  " + dbFile);
        dbFile.getFileContents().getFD().sync();
    }


    /**
     * Closes the underlying data file.  Obviously, subsequent read or write
     * attempts will fail after this method is called.
     *
     * @param dbFile the database file to close
     *
     * @throws IOException if the file cannot be closed for some reason.
     */
    public void closeDBFile(DBFile dbFile) throws IOException {
        dbFile.getFileContents().close();
    }


    /**
     * Deletes the database file with the specified filename from the storage
     * manager's directory.
     *
     * @param filename the name of the file to delete
     *
     * @throws IOException if the file cannot be deleted for some reason.
     */
    public void deleteDBFile(String filename) throws IOException {

        File f = new File(baseDir, filename);
        deleteDBFile(f);
    }


    /**
     * Deletes the specified database file.
     *
     * @param f the file to delete
     *
     * @throws IOException if the file cannot be deleted for some reason.
     */
    public void deleteDBFile(File f) throws IOException {
        if (!f.delete())
            throw new IOException("Couldn't delete file \"" + f.getName() + "\".");
    }


    /**
     * Deletes the specified database file.  The caller should ensure that the
     * database file is closed and is going to be unused.
     *
     * @param dbFile the database file to delete
     *
     * @throws IOException if the file cannot be deleted for some reason.
     */
    public void deleteDBFile(DBFile dbFile) throws IOException {
        deleteDBFile(dbFile.getDataFile());
    }
}
