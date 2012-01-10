package edu.caltech.test.nanodb.expressions;


import edu.caltech.nanodb.expressions.TypeCastException;
import org.testng.annotations.*;

import edu.caltech.nanodb.expressions.TypeConverter;

import edu.caltech.nanodb.relations.SQLDataType;


/**
 * This class exercises the type-converter class.
 */
@Test
public class TestTypeConverter {

    public void testGetBooleanValue() {
        assert Boolean.TRUE.equals(TypeConverter.getBooleanValue(new Integer(3)));
        assert Boolean.TRUE.equals(TypeConverter.getBooleanValue(Boolean.TRUE));

        assert Boolean.FALSE.equals(TypeConverter.getBooleanValue(new Integer(0)));
        assert Boolean.FALSE.equals(TypeConverter.getBooleanValue(Boolean.FALSE));

        assert null == TypeConverter.getBooleanValue(null);
    }

    @Test(expectedExceptions={TypeCastException.class})
    public void testGetBooleanValueError() {
        TypeConverter.getBooleanValue(new Object());
    }



    
    public void testGetSQLType() {
        // Recognized types:

        assert TypeConverter.getSQLType(Boolean.TRUE) == SQLDataType.TINYINT;

        assert TypeConverter.getSQLType(new Byte((byte) 3)) == SQLDataType.TINYINT;
        assert TypeConverter.getSQLType(new Short((short) 3)) == SQLDataType.SMALLINT;
        assert TypeConverter.getSQLType(new Integer(3)) == SQLDataType.INTEGER;
        assert TypeConverter.getSQLType(new Long(3)) == SQLDataType.BIGINT;

        assert TypeConverter.getSQLType(new Float(3.0f)) == SQLDataType.FLOAT;
        assert TypeConverter.getSQLType(new Double(3.0)) == SQLDataType.DOUBLE;

        assert TypeConverter.getSQLType("three") == SQLDataType.VARCHAR;

        // Unrecognized types:

        assert TypeConverter.getSQLType(new Object()) == null;
    }
}
