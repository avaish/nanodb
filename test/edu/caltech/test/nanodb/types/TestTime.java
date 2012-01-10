package edu.caltech.test.nanodb.types;


import org.testng.annotations.*;

import edu.caltech.nanodb.types.Time;


/**
 * This test class exercises the functionality of the
 * {@link edu.caltech.nanodb.types.Time} class.
 **/
@Test
public class TestTime {

  public void testSimpleCtors() {
  }


  public void testEquals() {
    Time t0 = new Time(23, 34, 15, 626);
    Time t1 = new Time(23, 34, 15, 626);
    Time t2 = new Time(23, 34, 15,   0);
    Time t3 = new Time(23, 34,  0, 626);
    Time t4 = new Time(23,  0, 15, 626);
    Time t5 = new Time( 0, 34, 15, 626);

    assert !(t0.equals(null));
    assert !(t0.equals("hello"));

    assert !(t1.equals(t2));
    assert !(t1.equals(t3));
    assert !(t1.equals(t4));
    assert !(t1.equals(t5));

    assert  (t1.equals(t0));
    assert  (t0.equals(t1));
    assert  (t1.equals(t1));
  }


  public void testHashCode() {
    int t0Hash = (new Time(23, 34, 15, 626)).hashCode();
    int t1Hash = (new Time(23, 34, 15, 626)).hashCode();
    int t2Hash = (new Time(23, 34, 15,   0)).hashCode();
    int t3Hash = (new Time(23, 34,  0, 626)).hashCode();
    int t4Hash = (new Time(23,  0, 15, 626)).hashCode();
    int t5Hash = (new Time( 0, 34, 15, 626)).hashCode();

    assert t1Hash != t2Hash;
    assert t1Hash != t3Hash;
    assert t1Hash != t4Hash;
    assert t1Hash != t5Hash;

    assert t1Hash == t0Hash;
  }
}
