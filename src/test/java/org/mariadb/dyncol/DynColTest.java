package test.java.org.mariadb.dyncol;

import org.junit.Assert;
import org.junit.Test;
import org.mariadb.dyncol.DynCol;

import java.sql.SQLException;
import java.io.UnsupportedEncodingException;

public class DynColTest {

    @Test
    public void setNullIntTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setInt("1", 0);
        Assert.assertEquals(0, dynCol.getInt("1"));
    }

    @Test
    public void setMinusOneIntTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setInt("1", -1);
        Assert.assertEquals(-1, dynCol.getInt("1"));
    }

    @Test
    public void setOneIntTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setInt("1", 1);
        Assert.assertEquals(1, dynCol.getInt("1"));
    }

    @Test
    public void setMaxValueIntTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setInt("1", Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, dynCol.getInt("1"));
    }

    @Test
    public void setMinValueIntTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setInt("1", Long.MIN_VALUE);
        Assert.assertEquals(Long.MIN_VALUE, dynCol.getInt("1"));
    }

    @Test
    public void setNullUintTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setUint("1", 0);
        Assert.assertEquals(0, dynCol.getUint("1"));
    }

    @Test (expected = Exception.class)
    public void setMinusOneUintTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setUint("1", -1);
    }

    @Test
    public void setOneUintTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setUint("1", 1);
        Assert.assertEquals(1, dynCol.getUint("1"));
    }

    @Test
    public void setMaxValueUintTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setUint("1", Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, dynCol.getUint("1"));
    }

    @Test
    public void setNullDoubleTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setDouble("1", 0.0);
        Assert.assertEquals(0.0, dynCol.getDouble("1"), 0.001);
    }

    @Test
    public void setIntegerDoubleTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setDouble("1", 1212.0);
        Assert.assertEquals(1212.0, dynCol.getDouble("1"), 0.001);
    }

    @Test
    public void setDoubleTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setDouble("1", 12.12);
        Assert.assertEquals(12.12, dynCol.getDouble("1"), 0.001);
    }

    @Test
    public void setMaxValueDoubleTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setDouble("1", Double.MAX_VALUE);
        Assert.assertEquals(Double.MAX_VALUE, dynCol.getDouble("1"), 0.0001);
    }

    @Test
    public void setMinValueDoubleTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setDouble("1", Double.MIN_VALUE);
        Assert.assertEquals(Double.MIN_VALUE, dynCol.getDouble("1"), 0.0001);
    }
    @Test (expected = Exception.class)
    public void setNullTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setString("1", null);
        dynCol.getString("1");
    }
    @Test
    public void setAfafStringTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setString("1", "afaf");
        Assert.assertEquals("afaf", dynCol.getString("1"));
    }

    @Test
    public void setNumStringTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setString("1", "1212");
        Assert.assertEquals("1212", dynCol.getString("1"));
    }

    @Test
    public void setJsonTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setJson("{\"1\":1," +
                       "\"2\":-1," +
                       "\"3\":12.12," +
                       "\"4\":\"afaf\"}");
        Assert.assertEquals(1, dynCol.getUint("1"));
        Assert.assertEquals(-1, dynCol.getInt("2"));
        Assert.assertEquals(12.12, dynCol.getDouble("3"), 0.0001);
        Assert.assertEquals("afaf", dynCol.getString("4"));
    }
    @Test
    public void getJsonTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setJson("{\"1\":1," +
                       "\"2\":-1," +
                       "\"3\":12.12," +
                       "\"4\":\"afaf\"}");
        dynCol.setJson(dynCol.getJson());
        Assert.assertEquals(1, dynCol.getUint("1"));
        Assert.assertEquals(-1, dynCol.getInt("2"));
        Assert.assertEquals(12.12, dynCol.getDouble("3"), 0.0001);
        Assert.assertEquals("afaf", dynCol.getString("4"));
    }
    
    @Test
    public void getBlobTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setJson("{\"1\":1," +
                       "\"2\":-1," +
                       "\"3\":12.12," +
                       "\"4\":\"afaf\"}");
        Assert.assertEquals("00040001000102000803001204005301013D0AD7A3703D28402D61666166", 
        javax.xml.bind.DatatypeConverter.printHexBinary(dynCol.getBlob()));
    }

    @Test
    public void setBlobTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setJson("{\"1\":1," +
                       "\"2\":-1," +
                       "\"3\":12.12," +
                       "\"4\":\"afaf\"}");
        dynCol.setBlob(dynCol.getBlob());
        Assert.assertEquals(1, dynCol.getUint("1"));
        Assert.assertEquals(-1, dynCol.getInt("2"));
        Assert.assertEquals(12.12, dynCol.getDouble("3"), 0.0001);
        Assert.assertEquals("afaf", dynCol.getString("4"));
    }

    @Test
    public void setNullIntNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setInt("n", 0);
        Assert.assertEquals(0, dynCol.getInt("n"));
    }

    @Test
    public void setMinusOneIntNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setInt("n", -1);
        Assert.assertEquals(-1, dynCol.getInt("n"));
    }

    @Test
    public void setOneIntNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setInt("n", 1);
        Assert.assertEquals(1, dynCol.getInt("n"));
    }

    @Test
    public void setMaxValueIntNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setInt("n", Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, dynCol.getInt("n"));
    }

    @Test
    public void setMinValueIntNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setInt("n", Long.MIN_VALUE);
        Assert.assertEquals(Long.MIN_VALUE, dynCol.getInt("n"));
    }

    @Test
    public void setNullUintNamedTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setUint("n", 0);
        Assert.assertEquals(0, dynCol.getUint("n"));
    }

    @Test (expected = Exception.class)
    public void setMinusOneUintNamedTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setUint("n", -1);
    }

    @Test
    public void setOneUintNamedTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setUint("n", 1);
        Assert.assertEquals(1, dynCol.getUint("n"));
    }

    @Test
    public void setMaxValueUintNamedTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setUint("n", Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, dynCol.getUint("n"));
    }

    @Test
    public void setNullDoubleNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setDouble("n", 0.0);
        Assert.assertEquals(0.0, dynCol.getDouble("n"), 0.001);
    }

    @Test
    public void setIntegerDoubleNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setDouble("n", 1212.0);
        Assert.assertEquals(1212.0, dynCol.getDouble("n"), 0.001);
    }

    @Test
    public void setDoubleNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setDouble("n", 12.12);
        Assert.assertEquals(12.12, dynCol.getDouble("n"), 0.001);
    }

    @Test
    public void setMaxValueDoubleNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setDouble("n", Double.MAX_VALUE);
        Assert.assertEquals(Double.MAX_VALUE, dynCol.getDouble("n"), 0.0001);
    }

    @Test
    public void setMinValueDoubleNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setDouble("n", Double.MIN_VALUE);
        Assert.assertEquals(Double.MIN_VALUE, dynCol.getDouble("n"), 0.0001);
    }
    @Test (expected = Exception.class)
    public void setNullNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setString("n", null);
        dynCol.getString("n");
    }
    @Test
    public void setAfafStringNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setString("n", "afaf");
        Assert.assertEquals("afaf", dynCol.getString("n"));
    }

    @Test
    public void setNumStringNamedTest() throws SQLException, UnsupportedEncodingException {
        DynCol dynCol = new DynCol();
        dynCol.setString("n", "1212");
        Assert.assertEquals("1212", dynCol.getString("n"));
    }

    @Test
    public void setJsonNamedTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setJson("{\"1\":1," +
                       "\"2\":-1," +
                       "\"3\":12.12," +
                       "\"4\":{\"1\":-1}}");
        Assert.assertEquals(1, dynCol.getUint("1"));
        Assert.assertEquals(-1, dynCol.getInt("2"));
        Assert.assertEquals(12.12, dynCol.getDouble("3"), 0.0001);
        Assert.assertEquals(-1, dynCol.getDynCol("4").getInt("1"));
    }
    @Test
    public void getJsonNamedTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setJson("{\"1\":1," +
                       "\"2\":-1," +
                       "\"3\":12.12," +
                       "\"4\":{\"1\":-1}}");
        dynCol.setJson(dynCol.getJson());
        Assert.assertEquals(1, dynCol.getUint("1"));
        Assert.assertEquals(-1, dynCol.getInt("2"));
        Assert.assertEquals(12.12, dynCol.getDouble("3"), 0.0001);
        Assert.assertEquals(-1, dynCol.getDynCol("4").getInt("1"));
    }
    
    @Test
    public void getBlobNamedTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setJson("{\"1\":1," +
                       "\"2\":-1," +
                       "\"3\":12.12," +
                       "\"4\":{\"1\":-1}}");
        Assert.assertEquals("04040004000000010001001000020022000300A8003132333401013D0AD7A3703D284000010001000001", 
        javax.xml.bind.DatatypeConverter.printHexBinary(dynCol.getBlob()));
    }

    @Test
    public void setBlobNamedTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setJson("{\"1\":1," +
                       "\"2\":-1," +
                       "\"3\":12.12," +
                       "\"4\":{\"1\":-1}}");
        dynCol.setBlob(dynCol.getBlob());
        Assert.assertEquals(1, dynCol.getUint("1"));
        Assert.assertEquals(-1, dynCol.getInt("2"));
        Assert.assertEquals(12.12, dynCol.getDouble("3"), 0.0001);
        Assert.assertEquals(-1, dynCol.getDynCol("4").getInt("1"));
    }

    @Test
    public void setDynColNamedTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setInt("1", -1);
        dynCol.setDynCol("1", dynCol);
        Assert.assertEquals("0401000100000008003100010001000001",
        javax.xml.bind.DatatypeConverter.printHexBinary(dynCol.getBlob()));
    }

    @Test
    public void getDynColNamedTest() throws SQLException, Exception {
        DynCol dynCol = new DynCol();
        dynCol.setInt("1", -1);
        dynCol.setDynCol("1", dynCol);
        Assert.assertEquals(-1, dynCol.getDynCol("1").getInt("1"));
    }
}