package io.stargate.sdk.grpc.domain;


import io.stargate.proto.QueryOuterClass;

import java.util.HashMap;

/**
 * Grpc Row.
 */
public class ResultRow extends HashMap<String, QueryOuterClass.Value> {

    /** Serial. */
    private static final long serialVersionUID = 3279531139420446635L;

    /**
     * Retrieve value and check existence.
     *
     * @param colName
     *      column name
     * @return
     *      value if exist or error
     */
    public Object get(String colName) {
        if (!containsKey(colName)) {
            throw new IllegalArgumentException("Cannot find column "
                    + "with name '" + colName + "', available columns are " + keySet());
        }
        return super.get(colName);
    }

    /**
     * Retrieve a column value as a String.
     *
     * @param colName String
     * @return String
     */
    public String getString(String colName) {
        return ((QueryOuterClass.Value) get(colName)).getString();
    }

    /**
     * Retrieve a column value as a Double.
     *
     * @param colName String
     * @return Double
     */
    public Double getDouble(String colName) {
        return ((QueryOuterClass.Value) get(colName)).getDouble();
    }

    /**
     * Retrieve a column value as an Integer.
     *
     * @param colName String
     * @return Integer
     */
    public Integer getInt(String colName) {
        return new Long(((QueryOuterClass.Value) get(colName)).getInt()).intValue();
    }
}


