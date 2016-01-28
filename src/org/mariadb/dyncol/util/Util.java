package org.mariadb.dyncol.util;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.Map;

import org.mariadb.dyncol.DynCol;
import org.mariadb.dyncol.blob.Blob;
import org.mariadb.dyncol.data.DynamicType;
import org.mariadb.dyncol.data.Member;

/*
 MariaDB Dynamic column java plugin
 Copyright (c) 2016 MariaDB.
 ...
 ...
 */
public class Util {

    public final static String UTF_8 = "UTF-8";
    public final static String EUC_JP = "euc_jp";
    public final static String GB2312 = "gb2312";
    public final static String EUC_KR = "euc_kr";
    public final static String CP850 = "cp850";
    public final static String CP852 = "cp852";
    public final static String CP866 = "cp66";
    public final static String LATIN1 = "latin1";
    public final static String LATIN2 = "latin2";
    public final static String GREEK = "greek";
    public final static String HEBREW = "hebrew";
    public final static String LATIN5 = "latin5";
    public final static String KOI8_R = "koi8_r";
    public final static String KOI8_U = "koi8_u";
    public final static String SJIS = "sjis";
    public final static String TIS620 = "tis620";
    public final static String ASCII = "ASCII";
    public final static String UTF16 = "utf16";
    public final static String ISO_10646_UCS_2 = "ISO-10646-UCS-2";
    public final static String UTF_16LE = "UTF_16LE";
    public final static String UTF32 = "UTF32";
    public final static String CP1250 = "cp1250";
    public final static String CP1251 = "cp1251";
    public final static String CP1256 = "cp1256";
    public final static String CP1257 = "cp1257";
    public final static String MACROMAN = "MacRoman";
    Blob blb;
    public boolean strType;
    Map<String, Member> data;

    /**
     * Util constructor.
     * 
     * @param blb
     *            information about DynamycColums
     * @param strType
     *            type of this DynamycColums
     * @param data
     *            class with data
     */
    public Util(Blob blb, boolean strType, Map<String, Member> data) {
        this.blb = blb;
        this.strType = strType;
        this.data = data;
    }

    /**
     * Decide witch type of string would be, if you add this member.
     * 
     * @param name
     *            the name of member
     * @return the type of DynamycColums true - named type, false - numeric
     */
    public boolean getStrType(String name) {
        if (!isNum(name)) {
            return true;
        }
        return false;
    }

    /**
     * Check the Json, Save Json It use Finite-state machine.
     * 
     * @param sstr
     *            is Json string
     * @param data
     *            is Map where data will store
     * @return true - success false - mistake
     * @throws Exception
     *             Dynamic type is unknown
     */
    public boolean parseJson(String sstr, Map<String, Member> data) throws Exception {
        boolean result = false;
        int testTable[][] = { { 0, 1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 }, { 1, -1, -1, 2, -1, -1, 7, -1, -1, -1, -1, -1 },
                { 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 2 }, { 3, -1, -1, -1, 4, -1, -1, -1, -1, -1, -1, -1 },
                { 4, 1, 5, 8, -1, -1, -1, -1, -1, 10, 6, -1 }, { 6, -1, 5, -1, -1, -1, 7, 1, -1, -1, -1, 5 },
                { 6, -1, -1, -1, -1, -1, 7, 1, -1, -1, -1, -1 }, { 7, -1, -1, -1, -1, -1, 7, 1, -1, -1, -1, -1 },
                { 8, 8, 11, 9, 8, 8, 8, 8, 8, 8, 8, 8 }, { 9, -1, -1, -1, -1, -1, 7, 1, -1, -1, -1, -1 },
                { -1, -1, 5, -1, -1, -1, 7, 1, -1, -1, -1, -1 }, { 8, 8, 11, 9, 12, 8, 8, 8, 8, 15, 8, 8 }, { 8, 8, 12, 9, 13, 8, 8, 8, 8, 8, 8, 8 },
                { 8, 8, 13, 9, 14, 8, 8, 8, 8, 8, 8, 8 }, { 8, 8, 14, 9, 8, 8, 8, 8, 8, 8, 8, 8 }, { 17, 8, 15, 9, 8, 8, 8, 8, 8, 16, 8, 8 },
                { 17, 8, 16, 9, 8, 8, 8, 8, 8, 8, 8, 8 }, { 8, 8, 11, 9, 8, 8, 8, 8, 8, 8, 8, 8 } };
        char character;
        int abscissa = 0;
        int ordinate = 0;
        boolean nested = false;
        boolean dateTime = false;
        String string = "";
        String name = "";
        int braces = 0;
        for (int i = 0; i < sstr.length(); i++) {
            character = sstr.charAt(i);
            switch (character) {
                case '{':
                    abscissa = 1;
                    break;
                case '"':
                    abscissa = 3;
                    break;
                case ':':
                    abscissa = 4;
                    break;
                case '\\':
                    abscissa = 5;
                    break;
                case '}':
                    abscissa = 6;
                    break;
                case ',':
                    abscissa = 7;
                    break;
                case '-':
                    abscissa = 9;
                    break;
                case '.':
                    abscissa = 11;
                    break;
                case 'n':
                    if (sstr.substring(i, i + 4).equals("null")) {
                        abscissa = 10;
                        i += 3;
                    } else {
                        abscissa = 8;
                    }
                    break;
                default:
                    if (character == ' ' || character == '	') {
                        abscissa = 0;
                    } else if (character >= '0' && character <= '9') {
                        abscissa = 2;
                    } else {
                        abscissa = 8;
                    }
                    break;
            }
            if (testTable[ordinate][abscissa] < 0) {
                return false;
            } /*if nested Json*/ else if (ordinate == 4 && abscissa == 1) {
                nested = true;
                strType = true;
                braces++;
                int coma = sstr.indexOf(",", i);
                if (coma < 0) {
                    coma = sstr.lastIndexOf("}");
                }
                int end = sstr.lastIndexOf('}', coma - 1);
                string = sstr.substring(i, end + 1);
                Member mem = data.get(name);
                mem.recordType = DynamicType.get(9);
                DynCol tempDynCol = new DynCol();
                try {
                    tempDynCol.setJson(string);
                    mem.setDynCol(tempDynCol);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                i = end;
                ordinate = 7;
                abscissa = 6;
            }
            // if string continuing
            if (ordinate == 7) {
                result = false;
            } /*if inside the name characters*/ else if (((ordinate == 2
                    && (abscissa == 0 || abscissa == 1 || abscissa == 4 || abscissa == 5 || abscissa == 6 || abscissa == 7
                    || abscissa == 8 || abscissa == 9 || abscissa == 10 || abscissa == 11)))
                    && (!nested)) {
                strType = true;
            }
            // if inside the string back slash
            if ((ordinate == 2 || ordinate == 8 || ordinate == 11 || ordinate == 12 || ordinate == 13 || ordinate == 14 || ordinate == 15
                    || ordinate == 16 || ordinate == 17)
                    && abscissa == 5) {
                i++;
            }
            // if nested string is ended
            if (abscissa == 6 && (ordinate == 1 || ordinate == 5 || ordinate == 6 || ordinate == 7 || ordinate == 9)) {
                braces--;
                result = true;
                nested = false;
            }
            // if new name starting, clear the name string
            if (ordinate == 1 && abscissa == 3) {
                name = "";
            } /*if inside the string name back slash, write next symbol*/ else if ((ordinate == 2) && abscissa == 5) {
                name = name + sstr.charAt(i);
            } /*if string name ended, save name*/ else if ((ordinate == 2) && abscissa == 3) {
                data.put(name, new Member());
            } else if (ordinate == 2) {
                name = name + character;
            } /*if character is ':' clear value string*/ else if (ordinate == 3 && abscissa == 4) {
                string = "";
            } /*if value string ended save it*/ else if ((ordinate == 8 || ordinate == 11 || ordinate == 17) && abscissa == 3) {
                Member mem = data.get(name);
                mem.recordType = DynamicType.get(4);
                mem.setString(string);
            } else if ((ordinate == 12 || ordinate == 13 || ordinate == 14) && abscissa == 3) {
                Member mem = data.get(name);
                mem.recordType = DynamicType.get((dateTime ? 6 : 8));
                mem.setString(string);
                dateTime = false;
            } else if ((ordinate == 15 || ordinate == 16) && abscissa == 3) {
                Member mem = data.get(name);
                mem.recordType = DynamicType.get(7);
                mem.setString(string);
            } else if (ordinate == 17 && abscissa == 2) {
                dateTime = true;
                string += character;
            } else if (ordinate >= 11 && ordinate <= 17) {
                string += character;
            } /*if inside the value string back slash, write next symbol*/ else if ((ordinate == 8 
                    || ordinate == 11 
                    || ordinate == 12 
                    || ordinate == 13 
                    || ordinate == 14 
                    || ordinate == 15 
                    || ordinate == 16 
                    || ordinate == 17)
                    && abscissa == 5) {
                string = string + sstr.charAt(i);
            } /*Just write value string*/ else if (ordinate == 8) {
                string = string + character;
            } /*if first symbol of value is numeral*/ else if (ordinate == 4 && abscissa == 2) {
                data.get(name).recordType = DynamicType.get(2);
                string += character;
            } /*if first symbol of value is minus*/ else if (ordinate == 4 && abscissa == 9) {
                data.get(name).recordType = DynamicType.get(1);
                string += character;
            } /*if numeral continue*/ else if ((ordinate == 5 || ordinate == 10) && abscissa == 2) {
                string += character;
            } /*if punkt*/ else if (ordinate == 5 && abscissa == 11) {
                data.get(name).recordType = DynamicType.get(3);
                string += character;
            } /*if number is ended, save it*/ else if (ordinate == 5) {
                Member mem = data.get(name);
                // if double
                if (mem.recordType == DynamicType.DOUBLE) {
                    mem.setDouble(Double.parseDouble(string));
                } else if (mem.recordType == DynamicType.INT) {
                    mem.setInt(Long.parseLong(string));
                } else {
                    mem.setUint(Long.parseLong(string));
                }
            } /*if null, delete column*/ else if (ordinate == 4 && abscissa == 10) {
                data.remove(name);
            }
            ordinate = testTable[ordinate][abscissa];

        }
        // because first '{' was not reading
        braces++;
        if (!result || braces != 0) {
            result = false;
        }
        return result;
    }

    /**
     * Check is string s a number.
     * 
     * @param sstr
     *            string to test
     * @return true if inside the string is a number false if not
     */
    public boolean isNum(String sstr) {
        if ((sstr.charAt(0) == '-' && sstr.length() > 1) || !(sstr.charAt(0) >= '0' && sstr.charAt(0) <= '9')) {
            return false;
        }
        for (int i = 1; i < sstr.length(); i++) {
            if (!(sstr.charAt(i) >= '0' && sstr.charAt(i) <= '9')) {
                return false;
            }
        }
        return true;
    }

    /**
     * Calculate size of the blob.
     * 
     * @return length of the blob
     * @throws SQLException Unsupported data length
     */
    public int getBlobLength() throws SQLException {
        int columnCount = data.size();
        int res = 0;
        blb.nmpoolSize = 0;
        for (String key : data.keySet()) {
            Member rec = data.get(key);
            res += rec.value.length;
            if (strType) {
                blb.nmpoolSize += key.getBytes().length;
            }
        }
        blb.offsetSize = offsetBytes(res);
        if (strType) {
            blb.headerOffset = columnCount * (2 /* 2 bytes offset from the name pool */ + blb.offsetSize) + 5; /*length of fixed string header with
             * names 1 byte - flags, 2 bytes - columns
             * counter, 2 bytes - name pool size
             */
        } else {
            blb.headerOffset = columnCount * (2 /* 2 bytes offset from the name pool */ + blb.offsetSize) + 3;
            // length of fixed string header 1 byte -
            // flags, 2 bytes - columns counter
        }
        res += blb.nmpoolSize;
        res += blb.headerOffset;
        return res;
    }

    /**
     * Calculate size of the offset pool.
     * 
     * @param dataLength
     *            is length of data
     * @return size of the offset pool
     * @throws SQLException Unsupported data length
     */
    public int offsetBytes(int dataLength) throws SQLException {
        if (strType) {
            if (dataLength < 0xfff) /* all 1 value is reserved */ {
                return 2;
            }
            if (dataLength < 0xfffff) /* all 1 value is reserved */ {
                return 3;
            }
            if (dataLength < 0xfffffff) /* all 1 value is reserved */ {
                return 4;
            }
            // without if (dataLength < 0xfffffffff), because of max array index is signed int
            throw new SQLException("Unsupported data length", "22000");
        } else {
            if (dataLength < 0x1f) /* all 1 value is reserved */ {
                return 1;
            }
            if (dataLength < 0x1fff) /* all 1 value is reserved */ {
                return 2;
            }
            if (dataLength < 0x1fffff) /* all 1 value is reserved */ {
                return 3;
            }
            if (dataLength < 0x1fffffff) /* all 1 value is reserved */ {
                return 4;
            }
            throw new SQLException("Unsupported data length", "22000");
        }
    }

    /**
     * Calculate how many bytes do you need to store this unsignet int.
     * 
     * @param val
     *            is value
     * @return how many bytes do you need to store this unsignet int
     */
    public int uintBytes(long val) {
        int len;
        for (len = 0; val != 0; val >>>= 8, len++) {}
        return len;
    }

    /**
     * Calculate how many bytes do you need to store this int.
     * 
     * @param val
     *            is value
     * @return how many bytes do you need to store this int
     */
    public int sintBytes(long val) {
        return uintBytes((val << 1) ^ (val < 0 ? (-1) : 0));
    }

    /**
     * Sort members by the name.
     * 
     * @param array
     *            is array that should be sorting
     * @param low
     *            is index of first element
     * @param high
     *            is index of last element
     */
    public void quSort(int[] array, int low, int high) {
        int iiAxis = low;
        int jjAxis = high;
        int middle = array[(low + high) / 2];
        do {
            while (array[iiAxis] < middle) {
                ++iiAxis;
            }
            while (array[jjAxis] > middle) {
                --jjAxis;
            }
            if (iiAxis <= jjAxis) {
                int tmp = array[iiAxis];
                array[iiAxis] = array[jjAxis];
                array[jjAxis] = tmp;
                iiAxis++;
                jjAxis--;
            }
        } while (iiAxis <= jjAxis);
        if (low < jjAxis) {
            quSort(array, low, jjAxis);
        }
        if (iiAxis < high) {
            quSort(array, iiAxis, high);
        }
    }

    /**
     * Sort array of strings: first sort by the length, then sort strings with the same length by symbols.
     * 
     * @param array
     *            is array that should be sorting
     * @param low
     *            is index of first element
     * @param high
     *            is index of last element
     */
    public void quSort(String[] array, int low, int high) {
        int iiAxis = low;
        int jjAxis = high;
        String middle = array[(low + high) / 2];
        do {
            while (compareStr(array[iiAxis], middle) < 0) {
                ++iiAxis;
            }
            while (compareStr(array[jjAxis], middle) > 0) {
                --jjAxis;
            }
            if (iiAxis <= jjAxis) {
                String tmp = array[iiAxis];
                array[iiAxis] = array[jjAxis];
                array[jjAxis] = tmp;
                iiAxis++;
                jjAxis--;
            }
        } while (iiAxis <= jjAxis);
        if (low < jjAxis) {
            quSort(array, low, jjAxis);
        }
        if (iiAxis < high) {
            quSort(array, iiAxis, high);
        }
    }

    /**
     * Compare string.
     * 
     * @param s1
     *            string #1
     * @param s2
     *            string #2
     * @return 0 if strings are the same, < 0 if string #1 is smaller then string #2, > 0 if string #1 is bigger then string #2
     */
    public int compareStr(String s1, String s2) {
        int res = (s1.length() > s2.length() ? 1 : (s1.length() < s2.length() ? -1 : 0));
        if (res == 0) {
            res = s1.compareTo(s2);
        }
        return res;
    }

    /**
     * Change the encoding of the string to UTF-8.
     * 
     * @param arrayByte
     *            string in old encoding
     * @param encoding
     *            number of encoding
     * @return string in UTF-8 encoding
     * @throws UnsupportedEncodingException
     *             if encoding is wrong
     */
    public String parseStr(byte[] arrayByte, int encoding) throws UnsupportedEncodingException {
        String res;
        String encodingstr;
        switch (encoding) {
            case 33:
            case 223:
            case 83:
            case 254:
            case 45:
            case 46:
                encodingstr = UTF_8;
                break;
            case 97:
            case 98:
                encodingstr = EUC_JP;
                break;
            case 24:
            case 86:
                encodingstr = GB2312;
                break;
            case 19:
            case 85:
                encodingstr = EUC_KR;
                break;
            case 4:
            case 80:
                encodingstr = CP850;
                break;
            case 40:
            case 81:
                encodingstr = CP852;
                break;
            case 36:
            case 68:
                encodingstr = CP866;
                break;
            case 5:
            case 8:
            case 15:
            case 31:
            case 47:
            case 48:
            case 49:
            case 94:
                encodingstr = LATIN1;
                break;
            case 2:
            case 9:
            case 21:
            case 27:
            case 77:
                encodingstr = LATIN2;
                break;
            case 25:
            case 70:
                encodingstr = GREEK;
                break;
            case 16:
            case 71:
                encodingstr = HEBREW;
                break;
            case 30:
            case 78:
                encodingstr = LATIN5;
                break;
            case 7:
            case 74:
                encodingstr = KOI8_R;
                break;
            case 22:
            case 75:
                encodingstr = KOI8_U;
                break;
            case 13:
            case 88:
                encodingstr = SJIS;
                break;
            case 18:
            case 89:
                encodingstr = TIS620;
                break;
            case 11:
            case 65:
                encodingstr = ASCII;
                break;
            case 54:
            case 55:
                encodingstr = UTF16;
                break;
            case 35:
            case 159:
            case 90:
                encodingstr = ISO_10646_UCS_2;
                break;
            case 56:
            case 62:
                encodingstr = UTF_16LE;
                break;
            case 60:
            case 61:
                encodingstr = UTF32;
                break;
            case 26:
            case 34:
            case 44:
            case 66:
            case 99:
                encodingstr = CP1250;
                break;
            case 14:
            case 23:
            case 50:
            case 51:
            case 52:
                encodingstr = CP1251;
                break;
            case 57:
            case 67:
                encodingstr = CP1256;
                break;
            case 29:
            case 58:
            case 59:
                encodingstr = CP1257;
                break;
            case 39:
            case 53:
                encodingstr = MACROMAN;
                break;
            default:
                throw new UnsupportedEncodingException();
        }
        res = new String(arrayByte, encodingstr);
        return res;
    }

    /**
     * Put the value in the DynCol.
     * @param blob
     *            DynamicColumns
     * @param rc
     *            member
     * @param offset
     *            offset from the start of the array
     * @return how much bytes use this member
     */
    public int putValue(byte[] blob, Member rc, int offset) {
        for (int index = 0; index < rc.value.length; index++) {
            blob[offset + index] = rc.value[index];
        }
        return rc.value.length;
    }

    /**
     * take the value from the DynCol.
     * @param blob
     *            DynamicColumns
     * @param rc
     *            member
     * @param offsetValue offset of value
     * @param byteLong
     *            how much bytes use this member
     */
    public void saveValue(byte[] blob, Member rc, int offsetValue, int byteLong) {
        rc.value = new byte[byteLong];
        for (int index = 0; index < byteLong; index++) {
            rc.value[index] = blob[blb.headerOffset + blb.nmpoolSize + offsetValue + index];
        }
    }
}
