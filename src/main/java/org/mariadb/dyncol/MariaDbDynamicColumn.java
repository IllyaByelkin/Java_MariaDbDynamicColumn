package org.mariadb.dyncol;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.mariadb.dyncol.data.DynamicType;
import org.mariadb.dyncol.data.Member;
import org.mariadb.dyncol.data.State;
import org.mariadb.dyncol.util.Util;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.sql.SQLException;

/*
 MariaDB Dynamic column java plugin
 Copyright (c) 2016 MariaDB.
 ...
 ...
 */
public class MariaDbDynamicColumn<T> {

    public DynamicType recordTypes;
    State currentState = new State();

    /**
     * This function set blob automatically.
     * @param blob DynamicColums
     * @param tt procesed object
     * @throws Exception Dynamic type is unknown and UnsupportedEncodingException
     */
    public void setBlob0(byte[] blob, T tt) throws Exception {
        //parse blob
        setBlob(blob);

        //create a new instance of class, and populate data
        Class cl = tt.getClass();
        Object instance = cl.newInstance();

        for (Map.Entry<String, Member> entry : currentState.getData().entrySet()) {
            String memberCapitalizeMethod = entry.getKey().substring(0, 1).toUpperCase() + entry.getKey().substring(1);
            Method[] allMethods = cl.getMethods();
            for (Method method : allMethods) {
                String methodName = method.getName();
                if (methodName.equals(memberCapitalizeMethod)) {
                    Type[] prametrType = method.getGenericParameterTypes();
                    if (prametrType.length != 1) {
                        continue;
                    }
                    try {
                        Member member = entry.getValue();
                        method.setAccessible(true);
                        switch (prametrType[0].toString()) {
                            case "byte":
                                method.invoke(instance, (byte)Util.getMemberIntValue(member));
                                break;
                            case "short":
                                method.invoke(instance, (short)Util.getMemberIntValue(member));
                                break;
                            case "int":
                                method.invoke(instance, (int)Util.getMemberIntValue(member));
                                break;
                            case "double":
                                method.invoke(instance, Util.getMemberDoubleValue(member));
                                break;
                            case "String":
                                method.invoke(instance, Util.getMemberStringValue(member));
                                break;
                            case "byte[]":
                                method.invoke(instance, Util.getMemberDynColValue(member).getBlob());
                                break;
                            default:
                                throw new SQLException("Invalid parameter type : asking for setBlobO on a " + member.recordType + " type", "HY105");
                        }
                    } catch (InvocationTargetException x) {
                        //handle exception
                    }
                }

            }
        }
    }

    /**
     * Check the format of the name, change the format of the sring if it necessary.
     * Save the value with help of setInt() method.
     * @see org.mariadb.dyncol.data.Member#setInt()
     *
     * @param name
     *            Name of member
     * @param value
     *            Value of member
     */
    public void setInt(String name, long value) {
        currentState.getData().put(name, new Member());
        if (!currentState.isColumnsWithStringFormat()) {
            currentState.setColumnsFormat(Util.getStrType(name));
        }
        Member mem = currentState.getData().get(name);
        mem.recordType = DynamicType.INT;
        mem.setInt(value);
    }

    /**
     * Check the format of the name, change the format of the sring if it necessary.
     * Save the value with help of setUint() method.
     * @see org.mariadb.dyncol.data.Member#setUint()
     *
     * @param name
     *            Name of member
     * @param value
     *            Value of member
     * @throws Exception illegal argument
     */
    public void setUint(String name, long value) throws Exception {
        if (!currentState.isColumnsWithStringFormat()) {
            currentState.setColumnsFormat(Util.getStrType(name));
        }
        if (value < 0) {
            throw new Exception("value " + value + " is illegal");
        } else {
            currentState.getData().put(name, new Member());
            Member mem = currentState.getData().get(name);
            mem.recordType = DynamicType.UINT;
            mem.setUint(value);
        }
    }

    /**
     * If the String is null delete the member.
     * Check the format of the name, change the format of the sring if it necessary.
     * Save the value with help of setString() method.
     * @see org.mariadb.dyncol.data.Member#setString()
     *
     * @param name
     *            Name of member
     * @param value
     *            Value of member
     */
    public void setString(String name, String value) {
        if (value == null) {
            if (currentState.getData().containsKey(name)) {
                currentState.getData().remove(name);
            }
        } else {
            if (!currentState.isColumnsWithStringFormat()) {
                currentState.setColumnsFormat(Util.getStrType(name));
            }
            currentState.getData().put(name, new Member());
            Member mem = currentState.getData().get(name);
            mem.recordType = DynamicType.STRING;
            mem.setString(value);
        }
    }

    /**
     * Check the format of the name, change the format of the sring if it necessary.
     * Save the value with help of setDouble() method.
     * @see org.mariadb.dyncol.data.Member#setDouble()
     *
     * @param name
     *            Name of member
     * @param value
     *            Value of member
     */
    public void setDouble(String name, double value) {
        if (!currentState.isColumnsWithStringFormat()) {
            currentState.setColumnsFormat(Util.getStrType(name));
        }
        currentState.getData().put(name, new Member());
        Member mem = currentState.getData().get(name);
        mem.recordType = DynamicType.DOUBLE;
        mem.setDouble(value);
    }

    /**
     * If the DynCol is null delete the member.
     * Don't check the format of the name, because inserted DynCol can be only in String format DynamicColumns.
     * Save the value with help of setDynCol() method.
     * @see org.mariadb.dyncol.data.Member#setDynCol()
     *
     * @param name
     *            Name of member
     * @param value
     *            Value of member
     * @throws Exception
     *             Dynamic type is unknown and UnsupportedEncodingException
     */
    public void setDynCol(String name, MariaDbDynamicColumn value) throws Exception {
        if (value == null) {
            if (currentState.getData().containsKey(name)) {
                currentState.getData().remove(name);
            }
        } else {
            MariaDbDynamicColumn tempd = new MariaDbDynamicColumn();
            tempd.setBlob(value.getBlob());
            currentState.setColumnsFormat(true);
            currentState.getData().put(name, new Member());
            Member mem = currentState.getData().get(name);
            mem.recordType = DynamicType.DYNCOL;
            mem.setDynCol(tempd);
        }
    }

    /**
     * Delete the member from DynCol.
     *
     * @param name
     *            name of the member
     */
    public void remove(String name) {
        currentState.getData().remove(name);
    }

    /**
     * Return the int value with help of getMemberIntValue() method.
     * @see org.mariadb.dyncol.util.Util#getMemberIntValue().
     *
     * @param name
     *            name of the member
     * @return value
     * @throws SQLException Parameter unknown and Invalid parameter type
     * @throws NumberFormatException if something wrong with a number format
     * @throws UnsupportedEncodingException if encoding is wrong
     */
    public long getInt(String name) throws SQLException, NumberFormatException, UnsupportedEncodingException {
        Member mem = currentState.getData().get(name);
        if (mem == null) {
            throw new SQLException("Parameter '" + name + "' unknown", "07002");
        }
        return Util.getMemberIntValue(mem);
    }

    /**
     * Return the int value with help of getMemberUintValue() method.
     * @see org.mariadb.dyncol.util.Util#getMemberUintValue().
     *
     * @param name
     *            name of the member
     * @return value
     * @throws SQLException Parameter unknown and Invalid parameter type
     * @throws NumberFormatException if something wrong with a number format
     * @throws UnsupportedEncodingException if encoding is wrong
     */
    public long getUint(String name) throws SQLException, NumberFormatException, UnsupportedEncodingException {
        Member mem = currentState.getData().get(name);
        if (mem == null) {
            throw new SQLException("Parameter '" + name + "' unknown", "07002");
        }
        return Util.getMemberUintValue(mem);
    }

    /**
     * Return the string value with help of getMemberStringValue() method.
     * @see org.mariadb.dyncol.util.Util#getMemberStringValue().
     *
     * @param name
     *            name of the member
     * @return value
     * @throws SQLException Parameter unknown and Invalid parameter type
     * @throws NumberFormatException if something wrong with a number format
     * @throws UnsupportedEncodingException if encoding is wrong
     */
    public String getString(String name) throws SQLException, UnsupportedEncodingException {
        Member mem = currentState.getData().get(name);
        if (mem == null) {
            throw new SQLException("Parameter '" + name + "' unknown", "07002");
        }
        return Util.getMemberStringValue(mem);
    }

    /**
     * Return the double value with help of getMemberDoubleValue() method.
     * @see org.mariadb.dyncol.util.Util#getMemberDoubleValue().
     *
     * @param name
     *            name of the member
     * @return value
     * @throws SQLException Parameter unknown and Invalid parameter type
     * @throws NumberFormatException if something wrong with a number format
     * @throws UnsupportedEncodingException if encoding is wrong
     */
    public double getDouble(String name) throws SQLException, NumberFormatException, UnsupportedEncodingException {
        Member mem = currentState.getData().get(name);
        if (mem == null) {
            throw new SQLException("Parameter '" + name + "' unknown", "07002");
        }
        return Util.getMemberDoubleValue(mem);
    }

    /**
     * Return the MariaDbDynamicColumn value with help of getMemberDynColValue() method.
     * @see org.mariadb.dyncol.util.Util#getMemberDynColValue().
     *
     * @param name
     *            name of the member
     * @return value
     * @throws Exception Dynamic type is unknown and UnsupportedEncodingException
     */
    public MariaDbDynamicColumn getDynCol(String name) throws Exception {
        Member mem = currentState.getData().get(name);
        if (mem == null) {
            throw new SQLException("Parameter '" + name + "' unknown", "07002");
        }
        return Util.getMemberDynColValue(mem);
    }

    /**
     *
     * @param name.
     *            name of the member
     * @return type of the member
     */
    public DynamicType getRecodFormat(String name) {
        Member mem = currentState.getData().get(name);
        return mem.recordType;
    }

    /**
     * Clear the old DynCol (if it was) and save Json string in DynamicColumn format.
     *
     * @param sstr
     *            Json string
     * @throws Exception Dynamic type is unknown and UnsupportedEncodingException
     */
    public void setJson(String sstr) throws Exception {
        if (sstr == null) {
            throw new NullPointerException();
        }
        currentState.getData().clear();
        if (!Util.parseJson(sstr, currentState.getData(), currentState)) {
            currentState.getData().clear();
            throw new SQLException("syntax error", "22000");
        }
    }

    /**
     * Save Json string in DynamicColumn format.
     *
     * @throws Exception Dynamic type is unknown and UnsupportedEncodingException
     */
    public void addJson(String sstr) throws Exception {
        if (sstr == null) {
            throw new SQLException("no data", "02000");
        }
        Map<String, Member> data = new HashMap<String, Member>();
        if (!Util.parseJson(sstr, data, currentState)) {
            data.clear();
            throw new SQLException("syntax error", "22000");
        } else {
            this.currentState.getData().putAll(data);
        }
    }

    /**
     * Convert DynamycColumn format to Json string.
     *
     * @return Json string
     * @throws Exception Dynamic type is unknown and UnsupportedEncodingException
     */
    public String getJson() throws Exception {
        String result = "{";
        if (currentState.getData().size() == 0) {
            result = "{}";
            return result;
        }
        boolean isFirst = true;
        for (Entry<String, Member> entry : currentState.getData().entrySet()) {
            if (!isFirst) {
                result += ',';
            } else {
                isFirst = false;
            }
            result += "\"" + entry.getKey() + "\":";
            switch (entry.getValue().recordType) {
                case NULL:
                    result += "null";
                    break;
                case INT:
                    result += entry.getValue().getInt();
                    break;
                case UINT:
                    result += entry.getValue().getUint();
                    break;
                case DOUBLE:
                    result += entry.getValue().getDouble();
                    break;
                case STRING:
                    result += "\"" + entry.getValue().getString() + "\"";
                    break;
                case DATETIME:
                case DATE:
                case TIME:
                    result += "\"" + entry.getValue().getString() + "\"";
                    break;
                case DYNCOL:
                    result += entry.getValue().getDynCol().getJson();
                    break;
                default:
                    throw new Exception("Dynamic type " + entry.getValue().recordType + " is unknown.");
            }
        }
        result += '}';
        return result;
    }

    /**
     * save the DynCol.
     *
     * @param blob
     *            DynamicColums
     * @throws Exception Dynamic type is unknown and UnsupportedEncodingException
     */
    public void setBlob(byte[] blob) throws Exception {
        if (blob == null) {
            throw new NullPointerException();
        } else if (blob.length == 3 || blob.length == 5) {
            currentState.getData().clear();
        } else {
            currentState.getData().clear();
            currentState.createBlobDescription();
            currentState.getBlobDescription().setBlobSize(blob.length);
            // if 2nd bit is 1, then type of string is named
            if (((blob[0] & 4) >>> 2) == 1) {
                currentState.setColumnsFormat(true);
            } else {
                currentState.setColumnsFormat(false);
            }
            // read the offset size from first 2 bits
            currentState.getBlobDescription().setOffsetSize((blob[0] & 3) + (currentState.isColumnsWithStringFormat() ? 2 : 1));
            int columnCount = currentState.getData().size();
            // read the column count from 1st and 2nd bytes
            columnCount = ((blob[2] & 0xFF) << 8) | (blob[1] & 0xFF);
            // calculating offset of all the header
            currentState.getBlobDescription().setHeaderOffset((currentState.isColumnsWithStringFormat() ? 5 : 3)
                + columnCount
                * (2 + currentState.getBlobDescription().getOffsetSize()));
            if (currentState.isColumnsWithStringFormat()) {
                // read name pool size from bytes #3 and 4
                currentState.getBlobDescription().setNmpoolSize(((blob[4] & 0xFF) << 8) | (blob[3] & 0xFF));
                String name;
                Member rc = new Member();
                int realOffset = 5;
                int nextOffsetName = 0;
                int realOffsetName = 0;
                int nextOffsetValue = 0;
                // read offset of the name, from beginning of name pool
                realOffsetName = (blob[realOffset] & 0xFF) | ((blob[realOffset + 1] & 0xFF) << 8);
                realOffset += 2;
                // read the record type
                rc.recordType = DynamicType.get((blob[realOffset] & 15) + 1);
                int realOffsetValue = 0;
                // read offset of the value, from beginning of the data pool
                realOffsetValue = (blob[realOffset] & 0xFF) >>> 4;
                realOffset++;
                if (currentState.getBlobDescription().getOffsetSize() > 1) {
                    realOffsetValue = realOffsetValue | ((blob[realOffset] & 0xFF) << 4);
                    realOffset++;
                }
                for (int index = 2; index < currentState.getBlobDescription().getOffsetSize(); index++) {
                    realOffsetValue = realOffsetValue | (blob[realOffset] & 0xFF) << (4 + 8 * (index - 1));
                    realOffset++;
                }
                for (int i = 0; i < columnCount; i++) {
                    // the next record type
                    int recordType = 0;
                    // if it isn't the last column
                    if (i != columnCount - 1) {
                        // reading the next name offset
                        nextOffsetName = (blob[realOffset] & 0xFF) | ((blob[realOffset + 1] & 0xFF) << 8);
                        realOffset += 2;
                        // reading the next record type
                        recordType = (blob[realOffset] & 15) + 1;
                        // reading the next value offset
                        nextOffsetValue = (blob[realOffset] & 0xFF) >>> 4;
                        realOffset++;
                        if (currentState.getBlobDescription().getOffsetSize() > 1) {
                            nextOffsetValue = nextOffsetValue | (blob[realOffset] & 0xFF) << (4);
                            realOffset++;
                        }
                        for (int index = 2; index < currentState.getBlobDescription().getOffsetSize(); index++) {
                            nextOffsetValue = nextOffsetValue | (blob[realOffset] & 0xFF) << (4 + 8 * (index - 1));
                            realOffset++;
                        }
                    } else {
                        nextOffsetName = currentState.getBlobDescription().getNmpoolSize();
                        nextOffsetValue = currentState.getBlobDescription().getBlobSize()
                                - currentState.getBlobDescription().getHeaderOffset()
                                - currentState.getBlobDescription().getNmpoolSize();
                    }
                    // calculating the name size
                    int byteLong = nextOffsetName - realOffsetName;
                    // read the name
                    byte[] arrayByte = new byte[byteLong];
                    for (int index = 0; index < byteLong; index++) {
                        arrayByte[index] = blob[currentState.getBlobDescription().getHeaderOffset() + realOffsetName + index];
                    }
                    name = new String(arrayByte, "UTF-8");
                    realOffsetName = nextOffsetName;
                    // read the values
                    Util.saveValue(blob, rc, realOffsetValue, nextOffsetValue - realOffsetValue, currentState);
                    currentState.getData().put(name, rc);
                    realOffsetValue = nextOffsetValue;
                    rc = new Member();
                    rc.recordType = DynamicType.get(recordType);
                }
            } else {
                String name;
                String nextName;
                Member rc = new Member();
                int realOffset = 3;
                int nextOffsetValue = 0;

                // read the name
                name = "" + ((blob[realOffset] & 0xFF) | ((blob[realOffset + 1] & 0xFF) << 8));
                realOffset += 2;
                // read the record type
                rc.recordType = DynamicType.get((blob[realOffset] & 7) + 1);
                int realOffsetValue = 0;
                // read offset of the value, from beginning of the data pool
                realOffsetValue = (blob[realOffset] & 0xFF) >>> 3;
                realOffset++;
                if (currentState.getBlobDescription().getOffsetSize() > 1) {
                    realOffsetValue = realOffsetValue | ((blob[realOffset] & 0xFF) << 3);
                    realOffset++;
                }
                for (int index = 2; index < currentState.getBlobDescription().getOffsetSize(); index++) {
                    realOffsetValue = realOffsetValue | (blob[realOffset] & 0xFF) << (3 + 8 * (index - 1));
                    realOffset++;
                }
                for (int i = 0; i < columnCount; i++) {
                    // the next record type
                    int recordType = 0;
                    // if it isn't the last column
                    if (i != columnCount - 1) {
                        nextName = "" + ((blob[realOffset] & 0xFF) | ((blob[realOffset + 1] & 0xFF) << 8));
                        realOffset += 2;
                        // reading the next record type
                        recordType = (blob[realOffset] & 7) + 1;
                        // reading the next value offset
                        nextOffsetValue = (blob[realOffset] & 0xFF) >>> 3;
                        realOffset++;
                        if (currentState.getBlobDescription().getOffsetSize() > 1) {
                            nextOffsetValue = nextOffsetValue | (blob[realOffset] & 0xFF) << (3);
                            realOffset++;
                        }
                        for (int index = 2; index < currentState.getBlobDescription().getOffsetSize(); index++) {
                            nextOffsetValue = nextOffsetValue | (blob[realOffset] & 0xFF) << (3 + 8 * (index - 1));
                            realOffset++;
                        }
                    } else {
                        nextName = null;
                        nextOffsetValue = currentState.getBlobDescription().getBlobSize() - currentState.getBlobDescription().getHeaderOffset();
                    }
                    // read the values
                    Util.saveValue(blob, rc, realOffsetValue, nextOffsetValue - realOffsetValue, currentState);
                    currentState.getData().put(name, rc);
                    name = nextName;
                    realOffsetValue = nextOffsetValue;
                    rc = new Member();
                    rc.recordType = DynamicType.get(recordType);
                }
            }
        }
    }

    /**
     * Create the Blob.
     *
     * @return the DynCol
     * @throws SQLException SQLException Unsupported data length
     */
    public byte[] getBlob() throws SQLException {
        byte[] blob;
        int columnCount = currentState.getData().size();
        if (columnCount == 0) {
            blob = new byte[3];
            return blob;
        }
        currentState.createBlobDescription();
        currentState.getBlobDescription().setBlobSize(Util.getBlobLength(currentState));
        blob = new byte[currentState.getBlobDescription().getBlobSize()];
        // put the flags
        blob[0] = (byte) (((currentState.getBlobDescription().getOffsetSize()
                - (currentState.isColumnsWithStringFormat() ? 2 : 1)) & 3) | (currentState.isColumnsWithStringFormat() ? 4 : 0));
        // put column count
        blob[1] = (byte) (columnCount & 0xFF);
        blob[2] = (byte) ((columnCount >>> 8) & 0xFF);
        //offset of header entry, header entry is column number or string pointer + offset & type
        int offset = 0;
        // offset from beginning of the data pool
        int dataOffset = 0;
        if (currentState.isColumnsWithStringFormat()) {
            String[] keyarray = new String[columnCount];
            // put the length of name pool
            blob[3] = (byte) (currentState.getBlobDescription().getNmpoolSize() & 0xFF);
            blob[4] = (byte) ((currentState.getBlobDescription().getNmpoolSize() >>> 8) & 0xFF);
            int inex = 0;
            for (String key : currentState.getData().keySet()) {
                keyarray[inex] = key;
                inex++;
            }
            // sort the names
            Util.quSort(keyarray, 0, columnCount - 1);
            offset = 5;// flag byte + number of columns 2 bytes + name pool length 2 bytes = 5 bytes
            // offset from beginning of the name pool
            int nameOffset = 0;
            for (inex = 0; inex < columnCount; inex++) {
                // put offset of the name
                blob[offset] = (byte) (nameOffset & 0xFF);
                blob[offset + 1] = (byte) ((nameOffset >>> 8) & 0xFF);
                offset += 2;
                Member rec = currentState.getData().get("" + keyarray[inex]);
                // put offset of the data + 4 byte data type
                blob[offset] = (byte) ((rec.recordType.ordinal() - 1) | ((dataOffset & 15) << 4));
                int tdataOffset = dataOffset >>> 4;
                for (int j = 1; j < currentState.getBlobDescription().getOffsetSize(); j++) {
                    blob[offset + j] = (byte) ((tdataOffset & 0xFF));
                    tdataOffset = dataOffset >>> 8;
                }
                offset += currentState.getBlobDescription().getOffsetSize();
                // put the name
                byte[] name = keyarray[inex].getBytes();
                for (int ind = 0; ind < name.length; ind++) {
                    blob[currentState.getBlobDescription().getHeaderOffset() + nameOffset + ind] = name[ind];
                }
                nameOffset += name.length;
                // put the data
                dataOffset += Util.putValue(blob, rec, currentState.getBlobDescription().getHeaderOffset()
                + currentState.getBlobDescription().getNmpoolSize()
                + dataOffset);
            }
        } else {
            int[] keyarray = new int[columnCount];
            int index = 0;
            for (String key : currentState.getData().keySet()) {
                keyarray[index] = Integer.parseInt(key);
                index++;
            }
            // sort the names
            Util.quSort(keyarray, 0, columnCount - 1);
            offset = 3;// flag byte + number of columns 2 bytes = 3 bytes
            for (index = 0; index < columnCount; index++) {
                // put the name
                blob[offset] = (byte) (keyarray[index] & 0xFF);
                blob[offset + 1] = (byte) ((keyarray[index] >>> 8) & 0xFF);
                offset += 2;
                Member rec = currentState.getData().get("" + keyarray[index]);
                // put offset of the data + 3 byte data type
                blob[offset] = (byte) ((rec.recordType.ordinal() - 1) | ((dataOffset & 31) << 3));
                int tdataOffset = dataOffset >>> 5;
                for (int j = 1; j < currentState.getBlobDescription().getOffsetSize(); j++) {
                    blob[offset + j] = (byte) ((tdataOffset & 0xFF));
                    tdataOffset = dataOffset >>> 8;
                }
                offset += currentState.getBlobDescription().getOffsetSize();
                // put the data
                dataOffset += Util.putValue(blob, rec, currentState.getBlobDescription().getHeaderOffset() + dataOffset);
            }
        }
        return blob;
    }
}
