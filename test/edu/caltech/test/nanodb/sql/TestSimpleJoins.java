package edu.caltech.test.nanodb.sql;


import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;


/**
 * This class exercises the database with some simple <tt>JOIN</tt>
 * clauses (inner, right and left), to see if they work properly on
 * multiple kinds of tables.
 */
@Test
public class TestSimpleJoins extends SqlTestCase {

    public TestSimpleJoins() {
        super("setup_testSimpleJoins");
    }
    
    
    /**
     * This test performs various joins with two tables that join.
     * 
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testNormalTables() throws Throwable {
        TupleLiteral[] naturalJoin = {
            new TupleLiteral(1, "alpha", 1, "A"),
            new TupleLiteral(2, "beta", 2, "B"),
            new TupleLiteral(3, "gamma", 3, "C"),
            new TupleLiteral(4, "delta", 4, "D")
        };
        
        TupleLiteral[] joinOn = {
            new TupleLiteral(1, "alpha", 2, "B"),
            new TupleLiteral(2, "beta", 3, "C"),
            new TupleLiteral(3, "gamma", 4, "D")
        };
        
        TupleLiteral[] crossJoin = {
            new TupleLiteral(1, "alpha", 1, "A"),
            new TupleLiteral(1, "alpha", 2, "B"),
            new TupleLiteral(1, "alpha", 3, "C"),
            new TupleLiteral(1, "alpha", 4, "D"),
            new TupleLiteral(1, "alpha", null, "9"),
            new TupleLiteral(2, "beta", 1, "A"),
            new TupleLiteral(2, "beta", 2, "B"),
            new TupleLiteral(2, "beta", 3, "C"),
            new TupleLiteral(2, "beta", 4, "D"),
            new TupleLiteral(2, "beta", null, "9"),
            new TupleLiteral(3, "gamma", 1, "A"),
            new TupleLiteral(3, "gamma", 2, "B"),
            new TupleLiteral(3, "gamma", 3, "C"),
            new TupleLiteral(3, "gamma", 4, "D"),
            new TupleLiteral(3, "gamma", null, "9"),
            new TupleLiteral(4, "delta", 1, "A"),
            new TupleLiteral(4, "delta", 2, "B"),
            new TupleLiteral(4, "delta", 3, "C"),
            new TupleLiteral(4, "delta", 4, "D"),
            new TupleLiteral(4, "delta", null, "9"),
            new TupleLiteral(null, "lorem", 1, "A"),
            new TupleLiteral(null, "lorem", 2, "B"),
            new TupleLiteral(null, "lorem", 3, "C"),
            new TupleLiteral(null, "lorem", 4, "D"),
            new TupleLiteral(null, "lorem", null, "9"),
            new TupleLiteral(null, "ipsum", 1, "A"),
            new TupleLiteral(null, "ipsum", 2, "B"),
            new TupleLiteral(null, "ipsum", 3, "C"),
            new TupleLiteral(null, "ipsum", 4, "D"),
            new TupleLiteral(null, "ipsum", null, "9")
        };
        
        TupleLiteral[] leftJoin = {
            new TupleLiteral(1, "alpha", 1, "A"),
            new TupleLiteral(2, "beta", 2, "B"),
            new TupleLiteral(3, "gamma", 3, "C"),
            new TupleLiteral(4, "delta", 4, "D"),
            new TupleLiteral(null, "lorem", null, null),
            new TupleLiteral(null, "ipsum", null, null)
        };
        
        TupleLiteral[] rightJoin = {
            new TupleLiteral(1, "alpha", 1, "A"),
            new TupleLiteral(2, "beta", 2, "B"),
            new TupleLiteral(3, "gamma", 3, "C"),
            new TupleLiteral(4, "delta", 4, "D"),
            new TupleLiteral(null, null, null, "9")
        };
        
        TupleLiteral[] noJoin = {
        };
        
        TupleLiteral[] multipleJoin = {
            new TupleLiteral(1, "alpha", 2, "B"),
            new TupleLiteral(1, "alpha", 3, "C"),
            new TupleLiteral(1, "alpha", 4, "D"),
            new TupleLiteral(2, "beta", 3, "C"),
            new TupleLiteral(2, "beta", 4, "D"),
            new TupleLiteral(3, "gamma", 4, "D")
        };
        
        CommandResult result;
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 NATURAL JOIN t2", true);
        assert checkUnorderedResults(naturalJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 JOIN t2 USING (id)", true);
        assert checkUnorderedResults(naturalJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id", true);
        assert checkUnorderedResults(naturalJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 JOIN t2 ON t1.id < t2.id", true);
        assert checkUnorderedResults(multipleJoin, result);
            
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id * 10", true);
        assert checkUnorderedResults(noJoin, result);
            
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id - 1", true);
        assert checkUnorderedResults(joinOn, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1, t2", true);
        assert checkUnorderedResults(crossJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1, t2 WHERE t1.id = t2.id", true);
        assert checkUnorderedResults(naturalJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1, t2 WHERE t1.id = t2.id - 1", true);
        assert checkUnorderedResults(joinOn, result);
            
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id", true);
        assert checkUnorderedResults(leftJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id", true);
        assert checkUnorderedResults(rightJoin, result);
    }
    
    /**
     * This test performs various joins with two tables, one being empty.
     * 
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testOneEmptyTable() throws Throwable {
        TupleLiteral[] leftJoin = {
            new TupleLiteral(1, "alpha", null),
            new TupleLiteral(2, "beta", null),
            new TupleLiteral(3, "gamma", null),
            new TupleLiteral(4, "delta", null),
            new TupleLiteral(null, "lorem", null),
            new TupleLiteral(null, "ipsum", null)
        };
        
        TupleLiteral[] noJoin = {
        };
        
        CommandResult result;
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 NATURAL JOIN t3", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t3 NATURAL JOIN t1", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 JOIN t3 USING (id)", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t3 JOIN t1 USING (id)", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 JOIN t3 ON t1.id = t3.id", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t3 JOIN t1 ON t1.id = t3.id", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1, t3", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t3, t1", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1, t3 WHERE t1.id = t3.id", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t3, t1 WHERE t1.id = t3.id", true);
        assert checkUnorderedResults(noJoin, result);
            
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 LEFT JOIN t3 ON t1.id = t3.id", true);
        assert checkUnorderedResults(leftJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t1 RIGHT JOIN t3 ON t1.id = t3.id", true);
        assert checkUnorderedResults(noJoin, result);
    }
    
    /**
     * This test performs various joins with two tables, both being empty.
     * 
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testTwoEmptyTables() throws Throwable {
        TupleLiteral[] noJoin = {
        };
        
        CommandResult result;
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t3 NATURAL JOIN t4", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t3 JOIN t4 USING (id)", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t3 JOIN t4 ON t4.id = t3.id", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t3, t4", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t3, t4 WHERE t4.id = t3.id", true);
        assert checkUnorderedResults(noJoin, result);
            
        result = NanoDBServer.doCommand(
            "SELECT * FROM t4 LEFT JOIN t3 ON t4.id = t3.id", true);
        assert checkUnorderedResults(noJoin, result);
        
        result = NanoDBServer.doCommand(
            "SELECT * FROM t4 RIGHT JOIN t3 ON t4.id = t3.id", true);
        assert checkUnorderedResults(noJoin, result);
    }
}
