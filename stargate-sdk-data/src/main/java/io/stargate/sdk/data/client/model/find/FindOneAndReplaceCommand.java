package io.stargate.sdk.data.client.model.find;

import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.client.model.Filter;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Data;

import java.util.Map;

/**
 * Represents the `findOneAndReplace` operation against the DataAPI, designed to find a document within a collection
 * and replace it entirely with a new document. This command offers the flexibility to either update an existing document
 * if a match is found or insert a new document if no existing document matches the search criteria. This functionality
 * is particularly useful for operations requiring atomic updates or upserts (update or insert) within a collection,
 * ensuring that the data remains consistent and current without the need for separate find and update operations.
 * The behavior of this command, including whether a new document is inserted when no match is found, can be
 * customized through options specified in the payload.
 * <p>
 * This class extends {@code DataApiCommand<CommandFindOneAndReplace.Payload>} to encapsulate the specific payload
 * required for the `findOneAndReplace` operation. The payload includes the filter criteria to locate the document,
 * the new document to replace the existing one, and optional settings to control the command's execution.
 * </p>
 */
public class FindOneAndReplaceCommand<DOC> extends DataApiCommand<FindOneAndReplaceCommand.Payload> {

    /**
     * Specialization of the command.
     */
    public FindOneAndReplaceCommand() {
        super("findOneAndReplace");
        payload = new FindOneAndReplaceCommand.Payload();
    }

    public FindOneAndReplaceCommand<DOC> withFilter(Filter filter) {
        if (filter != null) {
            payload.setFilter(filter.getFilter());
        }
        return this;
    }

    public FindOneAndReplaceCommand<DOC> withReplacement(DOC doc) {
       payload.setReplacement(JsonUtils.convertValueForDataApi(doc, Document.class));
       return this;
    }

    public FindOneAndReplaceCommand<DOC> withOptions(FindOneAndReplaceOptions options) {
        if (options != null) {
            payload.setSort(options.getSort());
            payload.setProjection(options.getProjection());
            FindOneAndReplaceCommand.Options cmdOptions = new FindOneAndReplaceCommand.Options();
            cmdOptions.setUpsert(options.getUpsert());
            cmdOptions.setReturnDocument(options.getReturnDocument().name());
            payload.setOptions(cmdOptions);
        }
        return this;
    }

    /**
     * Json Api Query Payload Wrapper.
     */
    @Data
    public static class Options {
        Boolean upsert;
        String returnDocument;
    }

    /**
     * Json Api Query Payload Wrapper.
     */
    @Data
    public static class Payload {

        /**
         * where clause
         */
        private Map<String, Object> filter;

        /**
         * Order by.
         */
        private Map<String, Object> sort;

        /**
         * Select.
         */
        private Map<String, Integer> projection;

        /**
         * Replacement
         */
        private Document replacement;

        /**
         * Options for this payload.
         */
        private Options options;

    }
}
