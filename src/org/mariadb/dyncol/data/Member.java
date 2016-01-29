package org.mariadb.dyncol.data;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.mariadb.dyncol.MariaDbDynamicColumn;

/*
 MariaDB Dynamic column java plugin.
 Copyright (c) 2016 MariaDB.
 ...
 ...
 */
public class Member {
    public final static String UTF_8 = "UTF-8";
    public DynamicType recordType;
    public byte[] value;

    /**
     * Decode the value.
     * 
     * @return decoded value
     */
    public long getUint() {
        long val = 0;
        for (int index = 0; index < value.length; index++) {
            val = val | ((long) (value[index] & 0xFF) << (8 * index));
        }
        return val;
    }

    /**
     * Decode the value.
     * 
     * @return decoded value
     */
    public long getInt() {
        long val = 0;
        for (int index = 0; index < value.length; index++) {
            val = val | ((long) (value[index] & 0xFF) << (8 * index));
        }
        if ((val & 1) != 0) {
            val = (val >>> 1) ^ (-1);
        } else {
            val >>>= 1;
        }
        return val;
    }

    /**
     * Decode the value.
     * 
     * @return decoded value
     */
    public double getDouble() {
        return Double.longBitsToDouble(getUint());
    }

    /**
     * Decode the value.
     * 
     * @return decoded value
     */
    public String getString() throws UnsupportedEncodingException {
        String str = new String(Arrays.copyOfRange(value, 1, value.length), UTF_8);
        return str;
    }

    /**
     * Decode the value.
     * 
     * @return decoded value
     * @throws Exception
     *             Dynamic type is unknown and UnsupportedEncodingException
     */
    public MariaDbDynamicColumn getDynCol() throws Exception {
        MariaDbDynamicColumn val = new MariaDbDynamicColumn();
        val.setBlob(value);
        return val;
    }

    /**
     * save value in byte form.
     * 
     * @param val
     *            value that should be saved
     */
    public void setUint(long val) {
        value = new byte[uintBytes(val)];
        for (int i = 0; val != 0; i++) {
            value[i] = (byte) (val & 0xFF);
            val >>>= 8;
        }
    }

    /**
     * save value in byte form.
     * 
     * @param val
     *            value that should be saved
     */
    public void setInt(long val) {
        val = (val << 1) ^ (val < 0 ? (-1) : 0);
        setUint(val);
    }

    /**
     * save value in byte form.
     * 
     * @param val
     *            value that should be saved
     */
    public void setDouble(double val) {
        value = new byte[8];
        long longValue = Double.doubleToRawLongBits(val);
        for (int i = 0; longValue != 0; i++) {
            value[i] = (byte) (longValue & 0xFF);
            longValue >>>= 8;
        }
    }

    /**
     * save value in byte form.
     * 
     * @param val
     *            value that should be saved
     */
    public void setString(String val) {
        value = new byte[val.length() + 1];
        value[0] = 45;// UTF8
        byte[] bytes = val.getBytes();
        for (int i = 0; i < bytes.length; i++) {
            value[1 + i] = bytes[i];
        }
    }

    /**
     * save value in byte form.
     * 
     * @param val
     *            value that should be saved
     */
    public void setDynCol(MariaDbDynamicColumn val) throws Exception {
        value = val.getBlob();
    }

    /**
     * Calculate uint size.
     * 
     * @param val
     *            value
     * @return size of value
     */
    public int uintBytes(long val) {
        int len;
        for (len = 0; val != 0; val >>>= 8, len++) {
        }
        return len;
    }
}
