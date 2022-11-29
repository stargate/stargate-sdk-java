package io.stargate.sdk.grpc.domain;

import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.sdk.core.domain.Page;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper to parse the grpc output.
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class ResultSetGrpc extends Page<RowGrpc> {
    
    /** Object returned by the grpc.*/
    private final ResultSet resultSet;
    
    /** Index columns names. */
    private final List<String> columnsNames = new ArrayList<>();

    /** Access one column in particular. */
    private final Map<String, Integer> columnsIndexes = new HashMap<>();

    /**
     * Constructor for the wrapper.
     *
     * @param rs
     *      result set
     */
    public ResultSetGrpc(ResultSet rs) {
        super(rs.getRowsCount(), rs.hasPagingState() ? rs.getPagingState().getValue().toStringUtf8() : null);
        this.resultSet = rs;
        // Building mapping name -> index maps
        for (int i=0; i<rs.getColumnsCount();i++) {
            ColumnSpec cs = rs.getColumns(i);
            this.columnsIndexes.put(cs.getName(), i);
            this.columnsNames.add(cs.getName());
        }
        // Mapping Results
        setResult(rs.getRowsList()
                .stream()
                .map(r -> new RowGrpc(this, r))
                .collect(Collectors.toList()));
    }

    /**
     * You know you do have a single line.
     * 
     * @return
     *      single row
     */
    public RowGrpc one() {
        if (1 != resultSet.getRowsCount()) {
            throw new IllegalArgumentException("Result set contains more than 1 row");
        }
        return getResults().get(0);
    }

    /**
     * Access column index based on offset.
     * 
     * @param name
     *      column name
     * @return
     *      column index
     */
     int getColumnIndex(String name) {
        if (!columnsIndexes.containsKey(name)) {
            throw new IllegalArgumentException("Column '" + name + "' is unknown, use " + columnsNames);
        }
        return columnsIndexes.get(name);
    }

}
