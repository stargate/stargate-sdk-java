package io.stargate.sdk.data.client.model.find;

import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.utils.Assert;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;

@Getter
public class FindOneOptions {

    public FindOneOptions() {
    }

    /**
     * Order by.
     */
    private Map<String, Object> sort;

    /**
     * Select.
     */
    private Map<String, Integer> projection;

    /**
     * Options.
     */
    private FindOneCommand.FindOneCommandOptions options;

    /**
     * Fluent api.
     *
     * @return
     *      add a filter
     */
    public FindOneOptions includeSimilarity() {
        if (options != null) {
            options = new FindOneCommand.FindOneCommandOptions();
            options.setIncludeSimilarity(true);
        }
        return this;
    }

    /**
     * Fluent api.
     *
     * @param pProjection
     *      add a project field
     * @return
     *      current command.
     */
    public FindOneOptions projection(Map<String, Integer> pProjection) {
        Assert.notNull(pProjection, "projection");
        if (this.projection == null) {
            this.projection = new LinkedHashMap<>();
        }
        this.projection.putAll(pProjection);
        return this;
    }

    /**
     * Fluent api.
     *
     * @param pSort
     *      add a filter
     * @return
     *      current command.
     */
    public FindOneOptions sort(Document pSort) {
        Assert.notNull(pSort, "projection");
        if (this.sort == null) {
            sort = new LinkedHashMap<>();
        }
        this.sort.putAll(pSort);
        return this;
    }

    /**
     * Add vector in the sort block.
     *
     * @param vector
     *      vector float
     * @return
     *      current command
     */
    public FindOneOptions sortByVector(float[] vector) {
        return sort(new Document().append(Document.VECTOR, vector));
    }

}
