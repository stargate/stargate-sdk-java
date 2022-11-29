package io.stargate.sdk.grpc.domain;

import io.stargate.sdk.core.domain.Page;

import java.util.List;

/**
 * Result of API
 *
 * @author Cedrick LUNVEN (@clunven)s
 */
public class ResultPage extends Page<ResultRow> {

    /**
     * Default constructor.
     */
    public ResultPage() {
        super();
    }

    /**
     * Full constructor.
     *
     * @param pageSize  int
     * @param pageState String
     * @param results   List
     */
    public ResultPage(int pageSize, String pageState, List<ResultRow> results) {
        super(pageSize, pageState, results);
    }
}
