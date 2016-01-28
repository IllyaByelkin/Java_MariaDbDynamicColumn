package org.mariadb.dyncol.data;

/*
MariaDB Dynamic column java plugin
Copyright (c) 2016 MariaDB.
...
...
 */
public enum DynamicType {
    NULL, INT, UINT, DOUBLE, STRING, DECIMAL, DATETIME, DATE, TIME, DYNCOL;
    
    /**
     * choose the type of member by the index of the type.
     * @param type index of the type
     * @return type of member
     * @throws Exception unknown type
     */
    public static DynamicType get(int type) throws Exception {
        switch (type) {
            case 0 : return NULL;
            case 1 : return INT;
            case 2 : return UINT;
            case 3 : return DOUBLE;
            case 4 : return STRING;
            case 5 : return DECIMAL;
            case 6 : return DATETIME;
            case 7 : return DATE;
            case 8 : return TIME;
            case 9:
                return DYNCOL;
            default:
                throw new Exception("Dynamic type " + type + " is unknown.");
        }
    }
}