package org.mariadb.dyncol;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.mariadb.dyncol.blob.Blob;
import org.mariadb.dyncol.data.DynamicType;
import org.mariadb.dyncol.data.Member;
import org.mariadb.dyncol.util.Util;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

/*
 MariaDB Dynamic column java plugin
 Copyright (c) 2016 MariaDB.
 ...
 ...
 */
public class DynCol {

    public DynamicType recordTypes;
    Map<String, Member> data = new HashMap<String, Member>();
    boolean strType = false; // true - Columns with names, false - Numeric format
    int errorIndex = 0;
    Blob blb = new Blob();
    public Util utils = new Util(blb, strType, data);

    /**
     * Add the member to the DynCol.
     * 
     * @param name
     *            Name of member
     * @param value
     *            Value of member
     */
    public void setInt(String name, long value) {
        data.put(name, new Member());
        if (!strType) {
            utils.strType = strType = utils.getStrType(name);
        }
        Member mem = data.get(name);
        mem.recordType = DynamicType.INT;
        mem.setInt(value);
    }

    /**
     * Add the member to the DynCol.
     * 
     * @param name
     *            Name of member
     * @param value
     *            Value of member
     * @throws Exception illegal argument
     */
    public void setUint(String name, long value) throws Exception {
        if (!strType) {
            utils.strType = strType = utils.getStrType(name);
        }
        if (value < 0) {
            throw new Exception("value " + value + " is illegal");
        } else {
            data.put(name, new Member());
            Member mem = data.get(name);
            mem.recordType = DynamicType.UINT;
            mem.setUint(value);
        }
    }

    /**
     * Add the member to the DynCol.
     * 
     * @param name
     *            Name of member
     * @param value
     *            Value of member
     */
    public void setString(String name, String value) {
        if (value == null) {
            if (data.containsKey(name)) {
                data.remove(name);
            }
        } else {
            if (!strType) {
                utils.strType = strType = utils.getStrType(name);
            }
            data.put(name, new Member());
            Member mem = data.get(name);
            mem.recordType = DynamicType.STRING;
            mem.setString(value);
        }
    }

    /**
     * Add the member to the DynCol.
     * 
     * @param name
     *            Name of member
     * @param value
     *            Value of member
     */
    public void setDouble(String name, double value) {
        if (!strType) {
            utils.strType = strType = utils.getStrType(name);
        }
        data.put(name, new Member());
        Member mem = data.get(name);
        mem.recordType = DynamicType.DOUBLE;
        mem.setDouble(value);
    }

    /**
     * Add the member to the DynCol.
     * 
     * @param name
     *            Name of member
     * @param value
     *            Value of member
     * @throws Exception
     *             Dynamic type is unknown and UnsupportedEncodingException
     */
    public void setDynCol(String name, DynCol value) throws Exception {
        if (value == null) {
            if (data.containsKey(name)) {
                data.remove(name);
            }
        } else {
            DynCol tempd = new DynCol();
            tempd.setBlob(value.getBlob());
            utils.strType = strType = true;
            data.put(name, new Member());
            Member mem = data.get(name);
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
        data.remove(name);
    }

    /**
     * Conversion a type of the member.
     * 
     * @param name
     *            name of the member
     * @return value
     * @throws SQLException Parameter unknown and Invalid parameter type
     * @throws NumberFormatException if something wrong with a number format
     * @throws UnsupportedEncodingException if encoding is wrong
     */
    public long getInt(String name) throws SQLException, NumberFormatException, UnsupportedEncodingException {
        Member mem = data.get(name);
        if (mem == null) {
            throw new SQLException("Parameter '" + name + "' unknown", "07002");
        }
        switch (mem.recordType) {
            case INT:
                return mem.getInt();
            case UINT:
                return mem.getUint();
            case DOUBLE:
                return (long) mem.getDouble();
            case STRING:
                return Long.parseLong(mem.getString());
            default:
                throw new SQLException("Invalid parameter type : asking fpr getInt on a " + mem.recordType + " type", "HY105");
        }
    }

    /**
     * Conversion a type of the member.
     * 
     * @param name
     *            name of the member
     * @return value
     * @throws SQLException Parameter unknown and Invalid parameter type
     * @throws NumberFormatException if something wrong with a number format
     * @throws UnsupportedEncodingException if encoding is wrong
     */
    public long getUint(String name) throws SQLException, NumberFormatException, UnsupportedEncodingException {
        Member mem = data.get(name);
        if (mem == null) {
            throw new SQLException("Parameter '" + name + "' unknown", "07002");
        }
        switch (mem.recordType) {
            case INT:
                return mem.getInt();
            case UINT:
                return mem.getUint();
            case DOUBLE:
                return (long) mem.getDouble();
            case STRING:
                return Long.parseLong(mem.getString());
            default:
                throw new SQLException("Invalid parameter type : asking fpr getUint on a " + mem.recordType + " type", "HY105");
        }
    }

    /**
     * Conversion a type of the member.
     * 
     * @param name
     *            name of the member
     * @return value
     * @throws SQLException Parameter unknown and Invalid parameter type
     * @throws NumberFormatException if something wrong with a number format
     * @throws UnsupportedEncodingException if encoding is wrong
     */
    public String getString(String name) throws SQLException, UnsupportedEncodingException {
        Member mem = data.get(name);
        if (mem == null) {
            throw new SQLException("Parameter '" + name + "' unknown", "07002");
        }
        switch (mem.recordType) {
            case INT:
                return "" + mem.getInt();
            case UINT:
                return "" + mem.getUint();
            case DOUBLE:
                return "" + mem.getDouble();
            case STRING:
                return mem.getString();
            default:
                throw new SQLException("Invalid parameter type : asking fpr getString on a " + mem.recordType + " type", "HY105");
        }
    }

    /**
     * Conversion a type of the member.
     * 
     * @param name
     *            name of the member
     * @return value
     * @throws SQLException Parameter unknown and Invalid parameter type
     * @throws NumberFormatException if something wrong with a number format
     * @throws UnsupportedEncodingException if encoding is wrong
     */
    public double getDouble(String name) throws SQLException, NumberFormatException, UnsupportedEncodingException {
        Member mem = data.get(name);
        if (mem == null) {
            throw new SQLException("Parameter '" + name + "' unknown", "07002");
        }
        switch (mem.recordType) {
            case INT:
                return (double) mem.getInt();
            case UINT:
                return (double) mem.getUint();
            case DOUBLE:
                return mem.getDouble();
            case STRING:
                return Long.parseLong(mem.getString());
            default:
                throw new SQLException("Invalid parameter type : asking fpr getDouble on a " + mem.recordType + " type", "HY105");
        }
    }

    /**
     * Conversion a type of the member.
     * 
     * @param name
     *            name of the member
     * @return value
     * @throws Exception Dynamic type is unknown and UnsupportedEncodingException
     */
    public DynCol getDynCol(String name) throws Exception {
        Member mem = data.get(name);
        if (mem == null) {
            throw new SQLException("Parameter '" + name + "' unknown", "07002");
        }
        switch (mem.recordType) {
            case DYNCOL:
                return mem.getDynCol();
            default:
                throw new SQLException("Invalid parameter type : asking fpr getDouble on a " + mem.recordType + " type", "HY105");
        }
    }

    /**
     * 
     * @param name.
     *            name of the member
     * @return type of the member
     */
    public DynamicType getRecodFormat(String name) {
        Member mem = data.get(name);
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
        data.clear();
        if (!utils.parseJson(sstr, data)) {
            data.clear();
            throw new SQLException("syntax error", "22000");
        } else {
            utils.strType = strType = utils.strType;
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
        if (!utils.parseJson(sstr, data)) {
            data.clear();
            throw new SQLException("syntax error", "22000");
        } else {
            utils.strType = strType = utils.strType;
            this.data.putAll(data);
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
        if (data.size() == 0) {
            result = "{}";
            return result;
        }
        boolean isFirst = true;
        for (Entry<String, Member> entry : data.entrySet()) {
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
            data.clear();
        } else {
            errorIndex = 0;
            data.clear();
            blb = new Blob();
            blb.blobSize = blob.length;
            // if 2nd bit is 1, then type of string is named
            if (((blob[0] & 4) >>> 2) == 1) {
                utils.strType = strType = true;
            } else {
                utils.strType = strType = false;
            }
            utils = new Util(blb, strType, data);
            // read the offset size from first 2 bits
            blb.offsetSize = (blob[0] & 3) + (strType ? 2 : 1);
            int columnCount = data.size();
            // read the column count from 1st and 2nd bytes
            columnCount = ((blob[2] & 0xFF) << 8) | (blob[1] & 0xFF);
            // calculating offset of all the header
            blb.headerOffset = (strType ? 5 : 3) + columnCount * (2 + blb.offsetSize);
            if (strType) {
                // read name pool size from bytes #3 and 4
                blb.nmpoolSize = ((blob[4] & 0xFF) << 8) | (blob[3] & 0xFF);
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
                if (blb.offsetSize > 1) {
                    realOffsetValue = realOffsetValue | ((blob[realOffset] & 0xFF) << 4);
                    realOffset++;
                }
                for (int index = 2; index < blb.offsetSize; index++) {
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
                        if (blb.offsetSize > 1) {
                            nextOffsetValue = nextOffsetValue | (blob[realOffset] & 0xFF) << (4);
                            realOffset++;
                        }
                        for (int index = 2; index < blb.offsetSize; index++) {
                            nextOffsetValue = nextOffsetValue | (blob[realOffset] & 0xFF) << (4 + 8 * (index - 1));
                            realOffset++;
                        }
                    } else {
                        nextOffsetName = blb.nmpoolSize;
                        nextOffsetValue = blb.blobSize - blb.headerOffset - blb.nmpoolSize;
                    }
                    // calculating the name size
                    int byteLong = nextOffsetName - realOffsetName;
                    // read the name
                    byte[] arrayByte = new byte[byteLong];
                    for (int index = 0; index < byteLong; index++) {
                        arrayByte[index] = blob[blb.headerOffset + realOffsetName + index];
                    }
                    name = new String(arrayByte, "UTF-8");
                    realOffsetName = nextOffsetName;
                    // read the values
                    utils.saveValue(blob, rc, realOffsetValue, nextOffsetValue - realOffsetValue);
                    data.put(name, rc);
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
                if (blb.offsetSize > 1) {
                    realOffsetValue = realOffsetValue | ((blob[realOffset] & 0xFF) << 3);
                    realOffset++;
                }
                for (int index = 2; index < blb.offsetSize; index++) {
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
                        if (blb.offsetSize > 1) {
                            nextOffsetValue = nextOffsetValue | (blob[realOffset] & 0xFF) << (3);
                            realOffset++;
                        }
                        for (int index = 2; index < blb.offsetSize; index++) {
                            nextOffsetValue = nextOffsetValue | (blob[realOffset] & 0xFF) << (3 + 8 * (index - 1));
                            realOffset++;
                        }
                    } else {
                        nextName = null;
                        nextOffsetValue = blb.blobSize - blb.headerOffset;
                    }
                    // read the values
                    utils.saveValue(blob, rc, realOffsetValue, nextOffsetValue - realOffsetValue);
                    data.put(name, rc);
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
        int columnCount = data.size();
        if (columnCount == 0) {
            blob = new byte[3];
            return blob;
        }
        int blbSize = utils.getBlobLength();
        blob = new byte[(blb.blobSize = blbSize)];
        // put the flags
        blob[0] = (byte) (((blb.offsetSize - (strType ? 2 : 1)) & 3) | (strType ? 4 : 0));
        // put column count
        blob[1] = (byte) (columnCount & 0xFF);
        blob[2] = (byte) ((columnCount >>> 8) & 0xFF);
        // offset of
        /*
         * offset is offset of header entry header entry is column number or string pointer + offset & type
         */
        int offset = 0;
        // offset from beginning of the data pool
        int dataOffset = 0;
        if (strType) {
            String[] keyarray = new String[columnCount];
            // put the length of name pool
            blob[3] = (byte) (blb.nmpoolSize & 0xFF);
            blob[4] = (byte) ((blb.nmpoolSize >>> 8) & 0xFF);
            int inex = 0;
            for (String key : data.keySet()) {
                keyarray[inex] = key;
                inex++;
            }
            // sort the names
            utils.quSort(keyarray, 0, columnCount - 1);
            offset = 5;// flag byte + number of columns 2 bytes + name pool length 2 bytes = 5 bytes
            // offset from beginning of the name pool
            int nameOffset = 0;
            for (inex = 0; inex < columnCount; inex++) {
                // put offset of the name
                blob[offset] = (byte) (nameOffset & 0xFF);
                blob[offset + 1] = (byte) ((nameOffset >>> 8) & 0xFF);
                offset += 2;
                Member rec = data.get("" + keyarray[inex]);
                // put offset of the data + 4 byte data type
                blob[offset] = (byte) ((rec.recordType.ordinal() - 1) | ((dataOffset & 15) << 4));
                int tdataOffset = dataOffset >>> 4;
                for (int j = 1; j < blb.offsetSize; j++) {
                    blob[offset + j] = (byte) ((tdataOffset & 0xFF));
                    tdataOffset = dataOffset >>> 8;
                }
                offset += blb.offsetSize;
                // put the name
                byte[] name = keyarray[inex].getBytes();
                for (int ind = 0; ind < name.length; ind++) {
                    blob[blb.headerOffset + nameOffset + ind] = name[ind];
                }
                nameOffset += name.length;
                // put the data
                dataOffset += utils.putValue(blob, rec, blb.headerOffset + blb.nmpoolSize + dataOffset);
            }
        } else {
            int[] keyarray = new int[columnCount];
            int index = 0;
            for (String key : data.keySet()) {
                keyarray[index] = Integer.parseInt(key);
                index++;
            }
            // sort the names
            utils.quSort(keyarray, 0, columnCount - 1);
            offset = 3;// flag byte + number of columns 2 bytes = 3 bytes
            for (index = 0; index < columnCount; index++) {
                // put the name
                blob[offset] = (byte) (keyarray[index] & 0xFF);
                blob[offset + 1] = (byte) ((keyarray[index] >>> 8) & 0xFF);
                offset += 2;
                Member rec = data.get("" + keyarray[index]);
                // put offset of the data + 3 byte data type
                blob[offset] = (byte) ((rec.recordType.ordinal() - 1) | ((dataOffset & 31) << 3));
                int tdataOffset = dataOffset >>> 5;
                for (int j = 1; j < blb.offsetSize; j++) {
                    blob[offset + j] = (byte) ((tdataOffset & 0xFF));
                    tdataOffset = dataOffset >>> 8;
                }
                offset += blb.offsetSize;
                // put the data
                dataOffset += utils.putValue(blob, rec, blb.headerOffset + dataOffset);
            }
        }
        return blob;
    }
}