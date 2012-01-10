package edu.caltech.test.nanodb.storage.heapfile;

import java.io.IOException;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBFileType;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FileManager;

import edu.caltech.nanodb.storage.heapfile.DataPage;

import edu.caltech.test.nanodb.storage.StorageTestCase;


/**
 * This class exercises some of the core utility methods of the {@link DataPage}
 * class.
 */
@Test
public class TestDataPage extends StorageTestCase {
	
	/**
	 * Keeps an instance of dbPage to be accessed by each test method after
	 * setup. A new dbPage will be created before each test method.
	 */
	private DBPage dbPage;

	
	/**
	 * dbFile is instantiated once for the class.
	 */
	private DBFile dbFile;

	
	/**
	 * A file with this name will be temporarily created under ./datafiles
	 * directory.
	 */
	private final String TEST_FILE_NAME = "TestDataPage_TestFile";


    /** This is the file-manager instance used for the tests in this class. */
    private FileManager fileMgr;


    /**
     * This set-up method initializes the file manager, data-file, and page that
     * all tests will run against.
     */
    @BeforeClass
    public void beforeClass() throws IOException {

        fileMgr = new FileManager(testBaseDir);

        // Get DBFile
        DBFileType type = DBFileType.HEAP_DATA_FILE;

        try {
            int pageSize = DBFile.DEFAULT_PAGESIZE; // 8k
            dbFile = fileMgr.createDBFile(TEST_FILE_NAME, type, pageSize);
        }
        catch (IOException e) {
            // The file is already created
        }

        dbPage = new DBPage(dbFile, 0);
    }
	
	
	/**
	 * Remove the dbFile created in beforeClass().
	 * 
	 * @throws IOException
	 */
	@AfterClass
	public void afterClass() throws IOException {
		fileMgr.deleteDBFile(dbFile);
	}
	
	
	/**
	 * Create new dbPage on the same dbFile will effectively erase all data on
	 * dbFile.
	 * 
	 * @throws IOException
	 */
	@BeforeMethod
	public void beforeMethod() throws IOException {
		DataPage.initNewPage(dbPage);
	}

	
	/**
	 * Test DataPage.insertTupleDataRange().
	 */
	@Test
	public void testInsertTupleDataRange(){
		
		// Here we (i) create two adjacent canary bytes that are at the end
		// of the dbPage and (ii) insert 4 bytes at the end of dbPage to 
		// test if insertTupleDataRange() correctly slide the canary tuple.
		// (iii) insert 5 bytes between canary1 and canary2 as the second
		// test for insertTupleDataRange().
		
        int slot = 0;	// the first (0th) slots point to the canary byte
        int numSlots = DataPage.getNumSlots(dbPage);
        assert numSlots == 0;
        
        // The last index in the data array for dbPage
        int endIndex = DataPage.getTupleDataEnd(dbPage) - 1; // should be 8191
        assert endIndex == dbPage.getPageSize() - 1 :
        	"endIndex = " + endIndex + ", should be " + 
        	(dbPage.getPageSize() - 1) ;
        
        // Write a canary byte (204) to the end of dbPage
        int canary1 = 204;
        int oldCanary1Index = endIndex;
        dbPage.writeByte(oldCanary1Index, canary1);
        DataPage.setNumSlots(dbPage, 1);	// we have 1 slot now.
        DataPage.setSlotValue(dbPage, slot, oldCanary1Index);
        
        
        // Write the second canary byte (170).
        slot = 1;		// the second (1-th) slots point to another canary byte
        int canary2 = 170;
        int oldCanary2Index = endIndex - 1;		// should be 8190
        dbPage.writeByte(oldCanary2Index, canary2);
        DataPage.setNumSlots(dbPage, 2);	// we have 2 slots now.
        DataPage.setSlotValue(dbPage, slot, oldCanary2Index);
       
        // check that the canary values are stored correctly
        int tupleDataStart = DataPage.getTupleDataStart(dbPage);
        assert tupleDataStart == 8190 :
        	"tupleDataStart = " + tupleDataStart + " != " + 8190;
        
        // Insert 4 bytes at the end of dbPage, thereby sliding the two 
        // canary bytes using insertTupleDataRange()
        int off = endIndex + 1;
        DataPage.insertTupleDataRange(dbPage, off, 4);
        
        // Check that the canary values are correctly slid forward
        int newCanary1Index = DataPage.getSlotValue(dbPage, 0); // should be 8187
        int newCanary2Index = DataPage.getSlotValue(dbPage, 1); // should be 8186
        assert newCanary1Index == oldCanary1Index - 4 : 
        	"newCanary1Index " + newCanary1Index + " != " + (oldCanary1Index - 4);
        assert newCanary2Index == oldCanary2Index - 4 :
        	"newCanary2Index " + newCanary2Index + " != " + (oldCanary2Index - 4);
        
        // read back the canary values.
        assert dbPage.readByte(newCanary1Index) == (byte) canary1;
        assert dbPage.readByte(newCanary2Index) == (byte) canary2;
        
        
        // Now we insert 5 bytes between canary1 and canary2
        off = newCanary1Index;
        DataPage.insertTupleDataRange(dbPage, off, 5);
        
        // Check that the canary values are correctly slid forward
        newCanary1Index = DataPage.getSlotValue(dbPage, 0); // should be 8187
        newCanary2Index = DataPage.getSlotValue(dbPage, 1); // should be 8181
        assert newCanary1Index == 8187 : 
        	"newCanary1Index " + newCanary1Index + " != " + 8187;
        assert newCanary2Index == 8181 : 
        	"newCanary1Index " + newCanary2Index + " != " + 8181;
        
        // read back the canary values.
        assert dbPage.readByte(newCanary1Index) == (byte) canary1;
        assert dbPage.readByte(newCanary2Index) == (byte) canary2;
        
	}
	
	
	/**
	 * Test DataPage.deleteTupleDataRange().
	 */
	@Test
	public void testDeleteTupleDataRange(){
		
		// Here we (i) create 3 adjacent canary integer that are at the end
		// of the dbPage and (ii) remove the middle integer to test if
		// DataPage.deleteTupleDataRange() correctly slide the canary values.
		
        int slot = 0;	// the first (0th) slots point to the canary byte
        int numSlots = DataPage.getNumSlots(dbPage);
        assert numSlots == 0;
        
        // The last index in the data array for dbPage
        int endIndex = DataPage.getTupleDataEnd(dbPage) - 1; // should be 8191
        assert endIndex == dbPage.getPageSize() - 1 :
        	"endIndex = " + endIndex + ", should be " + 
        	(dbPage.getPageSize() - 1);
        
        // Write the first canary integer (204) to the end of dbPage
        int canary1 = 204;
        int oldCanary1Index = endIndex - 4;	// int needs 4 bytes
        dbPage.writeInt(oldCanary1Index, canary1);
        DataPage.setNumSlots(dbPage, 1);	// we have 1 slot now.
        DataPage.setSlotValue(dbPage, slot, oldCanary1Index);
        
        // check the first canary values is stored correctly
        int tupleDataStart = DataPage.getTupleDataStart(dbPage);
        assert tupleDataStart == (8191 - 4) :
        	"tupleDataStart = " + tupleDataStart + " != " + (8191 - 4);
        
        
        // Write the second canary integer (170).
        slot = 1;		// the second (1-th) slots point to another canary byte
        int canary2 = 170;
        int oldCanary2Index = oldCanary1Index - 4;	// int needs 4 bytes
        dbPage.writeInt(oldCanary2Index, canary2);
        DataPage.setNumSlots(dbPage, 2);	// we have 2 slots now.
        DataPage.setSlotValue(dbPage, slot, oldCanary2Index);
       
        // check the second canary values is stored correctly
        tupleDataStart = DataPage.getTupleDataStart(dbPage);
        assert tupleDataStart == (8191 - 8) :
        	"tupleDataStart = " + tupleDataStart + " != " + (8191 - 8);
        
        // Write the third canary integer (150).
        slot = 2;		// the second (1-th) slots point to another canary byte
        int canary3 = 150;
        int oldCanary3Index = oldCanary2Index - 4;	// int needs 4 bytes
        dbPage.writeInt(oldCanary3Index, canary3);
        DataPage.setNumSlots(dbPage, 3);	// we have 2 slots now.
        DataPage.setSlotValue(dbPage, slot, oldCanary3Index);
        
        // check the third canary values is stored correctly
        tupleDataStart = DataPage.getTupleDataStart(dbPage);
        assert tupleDataStart == (8191 - 12) :
        	"tupleDataStart = " + tupleDataStart + " != " + (8191 - 12);
        
        
        // Now invoke DataPage.deleteTupleDataRange() to remove the second
        // canary value.
        DataPage.deleteTupleDataRange(dbPage, oldCanary2Index, 4);
        
        // update the slot value to EMPTY_SLOT for canary 2.
        DataPage.setSlotValue(dbPage, 1, DataPage.EMPTY_SLOT);
                
        // Check canary3 is correctly slid
        int newCanary3Index = DataPage.getSlotValue(dbPage, 2); 
        assert newCanary3Index == (8191 - 8) :
        	"newCanary3Index " + newCanary3Index + " != " + (8191 - 8);
        assert dbPage.readInt(newCanary3Index) == canary3;
        
        // Check canary1 is not affected 
        int newCanary1Index = DataPage.getSlotValue(dbPage, 0); 
        assert newCanary1Index == (8191 - 4) :
        	"newCanary1Index " + newCanary1Index + " != " + (8191 - 4);
        assert dbPage.readInt(newCanary1Index) == canary1;
      	
	}
}
