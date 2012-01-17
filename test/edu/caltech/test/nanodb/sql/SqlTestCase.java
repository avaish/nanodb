package edu.caltech.test.nanodb.sql;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import edu.caltech.nanodb.server.CommandResult;
import org.apache.commons.io.FileUtils;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This base-class provides functionality common to all testing classes that
 * issue SQL and examine the results.
 */
public class SqlTestCase {

    public static final String TEST_SQL_PROPS =
        "edu/caltech/test/nanodb/sql/test_sql.props";


    private Properties testSQL;


    /**
     * The data directory to use for the test cases, separate from the standard
     * data directory.
     */
    private File testBaseDir;


    private String setupSQLPropName;
    
    
    
    protected SqlTestCase(String setupSQLPropName) {
        this.setupSQLPropName = setupSQLPropName;
    }


    protected SqlTestCase() {
        this(null);
    }


    @BeforeClass
    public void beforeClass() throws Exception {
        // Set up a separate testing data-directory so that we don't clobber
        // any existing data.
        testBaseDir = new File("test_datafiles");
        if (!testBaseDir.exists())
            testBaseDir.mkdirs();
        else
            FileUtils.cleanDirectory(testBaseDir);

        // Make sure the database server uses the testing base-directory, not
        // the normal base-directory.
        System.setProperty(StorageManager.PROP_BASEDIR,
            testBaseDir.getAbsolutePath());

        NanoDBServer.startup();

        // Run the initialization SQL, if it has been specified.
        if (setupSQLPropName != null) {
            loadTestSQLProperties();
            String setupSQL = testSQL.getProperty(setupSQLPropName);
            if (setupSQL == null) {
                throw new IOException("Property " + setupSQLPropName +
                    " not specified in " + TEST_SQL_PROPS);
            }

            NanoDBServer.doCommands(setupSQL, false);
        }
    }


    private void loadTestSQLProperties() throws IOException {
        InputStream is =
            getClass().getClassLoader().getResourceAsStream(TEST_SQL_PROPS);
        if (is == null)
            throw new IOException("Couldn't find resource " + TEST_SQL_PROPS);

        testSQL = new Properties();
        testSQL.load(is);
        is.close();
    }



    @AfterClass
    public void afterClass() {
        // Shut down the database server and clean up the testing base-directory.
        NanoDBServer.shutdown();

        // Try to clean up the testing directory.
        try {
            FileUtils.cleanDirectory(testBaseDir);
        }
        catch (IOException e) {
            System.err.println("Couldn't clean directory " + testBaseDir);
            e.printStackTrace();
        }
    }


    /**
     * This helper function examines two collections of tuples, the expected
     * tuples and the actual tuples, and returns <tt>true</tt> if they are the
     * same, regardless of order.
     *
     * @param expected An array of tuple-literals containing the expected values.
     * @param actual A list of the actual tuple values produced by the database.
     * @return true if the two collections are the same, regardless of order, or
     *         false if they are not the same.
     */
    public boolean sameResultsUnordered(TupleLiteral[] expected,
                                        List<TupleLiteral> actual) {
        
        if (expected.length != actual.size())
            return false;
        
        LinkedList<TupleLiteral> expectedList = new LinkedList<TupleLiteral>();
        Collections.addAll(expectedList, expected);

        for (Tuple a : actual) {
            Iterator<TupleLiteral> iter = expectedList.iterator();
            
            boolean found = false;
            while (iter.hasNext()) {
                Tuple e = iter.next();
                if (TupleComparator.areTuplesEqual(e, a)) {
                    iter.remove();
                    found = true;
                    break;
                }
            }
            
            // We saw a tuple in the actual results that doesn't match anything
            // in the expected results.
            if (!found)
                return false;
        }
        
        // If we got here, all actual results matched expected results.
        return true;
    }


    /**
     * This helper function examines a command's results against an expected
     * collection of tuples, and returns <tt>true</tt> if the result tuples
     * are the same, regardless of order.  This function checks that the
     * command didn't throw any exceptions, before calling the
     * {@link #sameResultsUnordered(TupleLiteral[], List)} method to compare
     * the results themselves.
     *
     * @param expected An array of tuple-literals containing the expected values.
     * @param result The command-result containing the actual tuple values
     *               produced by the database.
     *
     * @return true if the two collections are the same, regardless of order, or
     *         false if they are not the same.
     *
     * @throws Exception if an error occurred during command execution, as
     *         reported by the command result.
     */
    public boolean checkUnorderedResults(TupleLiteral[] expected,
        CommandResult result) throws Exception {
        if (result.failed())
            throw result.getFailure();

        return sameResultsUnordered(expected, result.getTuples());
    }


    /**
     * This helper function examines two collections of tuples, the expected
     * tuples and the actual tuples, and returns <tt>true</tt> if they are the
     * same tuples and in the same order.
     *
     * @param expected An array of tuple-literals containing the expected values.
     * @param actual A list of the actual tuple values produced by the database.
     * @return true if the two collections are the same, and in the same order,
     *         or false otherwise.
     */
    public boolean sameResultsOrdered(TupleLiteral[] expected,
                                      List<TupleLiteral> actual) {

        if (expected.length != actual.size())
            return false;

        int i = 0;
        for (Tuple a : actual) {
            Tuple e = expected[i];

            // The expected and actual tuples don't match.
            if (!TupleComparator.areTuplesEqual(e, a))
                return false;

            // Go on to the next tuple in the expected array.
            i++;
        }

        // If we got here, all actual results matched expected results, and they
        // were in the same order.
        return true;
    }


    /**
     * This helper function examines a command's results against an expected
     * collection of tuples, and returns <tt>true</tt> if the result tuples
     * are the same, and in the same order.  This function checks that the
     * command didn't throw any exceptions, before calling the
     * {@link #sameResultsOrdered(TupleLiteral[], List)} method to compare
     * the results themselves.
     *
     * @param expected An array of tuple-literals containing the expected values.
     * @param result The command-result containing the actual tuple values
     *               produced by the database.
     *
     * @return true if the two collections are the same, and in the same order,
     *         or false otherwise.
     *
     * @throws Exception if an error occurred during command execution, as
     *         reported by the command result.
     */
    public boolean checkOrderedResults(TupleLiteral[] expected,
                                       CommandResult result) throws Exception {
        if (result.failed())
            throw result.getFailure();

        return sameResultsOrdered(expected, result.getTuples());
    }
}
