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

/**
 Contain the value in DynamicColumn blob form.
 */
public class Member {
    public final static String UTF_8 = "UTF-8";
    public DynamicType recordType;
    public byte[] value;

    /**
     * Convert from array of bytes (put 8 bits from every next byte of array on the top of the number).
     * 
     * @return converted value
     */
    public long getUint() {
        long val = 0;
        for (int index = 0; index < value.length; index++) {
            val = val | ((long) (value[index] & 0xFF) << (8 * index));
        }
        return val;
    }

    /**
     * Convert from array of bytes (put 8 bits from every next byte of array on the top of the number)
     * saved as DynamicColumn signet int to normal int:
     * 1 -> 1
     * 2 -> -1
     * 3 -> 2
     * 4 -> -2
     * 5 -> 3
     * 6 -> -3
     * 
     * @return converted value
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
     * Convert from array of bytes (put 8 bits from every next byte of array on the top of the number)
     * 
     * @return converted value
     */
    public double getDouble() {
        return Double.longBitsToDouble(getUint());
    }

    /**
     * Construct a new string from array of bytes
     * and cut the first byte that contains nummer of encoding.
     * It don't read it because encoding can be only UTF8.
     * 
     * @return constructed string
     */
    public String getString() throws UnsupportedEncodingException {
        String str = new String(Arrays.copyOfRange(value, 1, value.length), UTF_8);
        return str;
    }

    /**
     * Create a new inserted MariaDbDynamicColumns object, and parse array of bytes with a setBlob() method.
     * @see org.mariadb.dyncol.MariaDbDynamicColumns#setBlob()
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
     * convert value to array of bytes form:
     * Cut each 8 bits from the head of the namber, and stor it in every next element of array.
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
     * convert value to DynamicColumn signet int form:
     * -1 -> 1
     * 1 -> 2
     * -2 -> 3
     * 2 -> 4
     * -3 -> 5
     * 3 -> 6
     * And then use method setUint to converted parametr.
     * @see org.mariadb.dyncol.data.Member#setUint()
     * 
     * @param val
     *            value that should be saved
     */
    public void setInt(long val) {
        val = (val << 1) ^ (val < 0 ? (-1) : 0);
        setUint(val);
    }

    /**
     * Convert value in byte form:
     * Cut each 8 bits from the head of the namber, and stor it in every next element of array.
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
     * Convert a string in array of bytes and add a nummer of encoding (45 is UTF8)
     * 
     * @param val
     *            string that should be converted
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
     * Converta MariaDbDynamicColumnDynamycColumn to a byte forme with help of getBlob() method.
     * @see org.mariadb.dyncol.data.Member#getBlob()
     * 
     * @param val
     *            value that should be saved
     */
    public void setDynCol(MariaDbDynamicColumn val) throws Exception {
        value = val.getBlob();
    }

    /**
     * Calculate uint size by cutting every next 8 bits untill value become 0
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
