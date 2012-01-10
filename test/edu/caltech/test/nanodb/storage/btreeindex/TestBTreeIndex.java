package edu.caltech.test.nanodb.storage.btreeindex;


import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.indexes.IndexInfo;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.storage.StorageManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;


/**
 * Created by IntelliJ IDEA.
 * User: donnie
 * Date: 12/26/11
 * Time: 10:01 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestBTreeIndex {
/*
    @BeforeClass
    public void setup() throws IOException {
        StorageManager.init();
    }

    public void testCreateNonUniqueIndex() {
        TableSchema schema = new TableSchema();
        schema.addColumnInfo(new ColumnInfo("a", new ColumnType(SQLDataType.INTEGER)));

        IndexInfo idxInfo = new IndexInfo("test_table", schema, colIndexes, false);
        IndexFileInfo idxFileInfo = new IndexFileInfo("test_index", "test_table", )
    }

    @AfterClass
    public void teardown() throws IOException {
        StorageManager.shutdown();
    }
*/
}
