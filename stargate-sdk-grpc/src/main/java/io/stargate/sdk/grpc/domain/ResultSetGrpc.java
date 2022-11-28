package io.stargate.sdk.grpc.domain;

import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.ResultSet;

import java.util.*;
import java.util.stream.Stream;

/**
 * Helper to parse the grpoc output.
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class ResultSetGrpc {
    
    /** Object returned by the grpc.*/
    private final ResultSet resultSet;
    
    /** Index columns names. */
    private final List<String> columnsNames = new ArrayList<>();
    
    /** Access one column in particular. */
    private final Map<String, ColumnSpec> columnsSpecs = new HashMap<>();
    
    /** Access one column in particular. */
    private final Map<String, Integer> columnsIndexes = new HashMap<>();

    /**
     * Get row counts.
     *
     * @return
     *      rows counts
     */
    public int getRowCount() {
        return resultSet.getRowsCount();
    }

    /**
     * Get columns counts.
     *
     * @return
     *      columns counts
     */
    public int getColumnsCount() {
        return resultSet.getColumnsCount();
    }

    /**
     * Get paging state
     *
     * @return
     *      paging state
     */
    public Optional<String> getPagingState() {
        Optional<String> pg = Optional.empty();
        if (resultSet.hasPagingState()) {
            pg = Optional.ofNullable(resultSet.getPagingState().getValue().toStringUtf8());
        }
        return pg;
    }

    /**
     * Constructor for the wrapper.
     * 
     * @param rs
     *      resultset
     */
    public ResultSetGrpc(ResultSet rs) {
        this.resultSet = rs;
        for (int i=0; i<rs.getColumnsCount();i++) {
            ColumnSpec cs = rs.getColumns(i);
            this.columnsSpecs.put(cs.getName(), cs);
            this.columnsIndexes.put(cs.getName(), i);
            this.columnsNames.add(cs.getName());
        }
    }
    
    /**
     * You know you do have a single line.
     * 
     * @return
     *      single row
     */
    public RowGrpc one() {
        if (1 != resultSet.getRowsCount()) {
            throw new IllegalArgumentException("Resultset contains more than 1 row");
        }
        return getRows().findFirst().get();
    }
    
    /**
     * Access a row by its index.
     * 
     * @param idx
     *      row index
     * @return
     *      row value
     */
    public RowGrpc getRow(int idx) {
        if (idx > resultSet.getRowsCount()) {
            throw new IllegalArgumentException("Resulset contains only " +  resultSet.getRowsCount() + " row(s).");
        }
        return  new RowGrpc(this , resultSet.getRowsList().get(idx)) ;
    }
    
    /**
     * Access Rows.
     * 
     * @return
     *      list if items
     */
    public Stream<RowGrpc> getRows() {
        return resultSet.getRowsList()
                 .stream()
                 .map(r -> new RowGrpc(this, r));
    }
    
    /**
     * Accessor for internal object.
     * 
     * @return
     *      internal object
     */
    public ResultSet getResultSet() {
        return resultSet;
    }
    
    /**
     * Access column index based on offset.
     * 
     * @param name
     *      column name
     * @return
     *      column index
     */
    public int getColumnIndex(String name) {
        if (!columnsIndexes.containsKey(name)) {
            throw new IllegalArgumentException("Column '" + name + "' is unknown, use " + columnsNames);
        }
        return columnsIndexes.get(name);
    }
    
    /**
     * Return column name based on index.
     * @param idx
     *      column index
     * @return
     *      column name
     */
    public String getColumnName(int idx) {
        if (idx > columnsNames.size()) {
            throw new IllegalArgumentException("Invalid index, only '" 
                            + columnsNames.size() + "' size available");
        }
        return columnsNames.get(idx);
    }

    /**
     * Gets columnsNames
     *
     * @return value of columnsNames
     */
    public List<String> getColumnsNames() {
        return columnsNames;
    }

    /**
     * Gets columnsSpecs
     *
     * @return value of columnsSpecs
     */
    public Map<String, ColumnSpec> getColumnsSpecs() {
        return columnsSpecs;
    }

    /**
     * Gets columnsIndexes
     *
     * @return value of columnsIndexes
     */
    public Map<String, Integer> getColumnsIndexes() {
        return columnsIndexes;
    }
}
