package io.stargate.sdk.data.client.model.insert;

import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.data.client.model.find.CommandFindOne;
import io.stargate.sdk.data.client.model.find.FindOneOptions;
import io.stargate.sdk.utils.Assert;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

public class CommandInsertMany<DOC> extends DataApiCommand<CommandInsertMany.Payload<DOC>> {

    /**
     * Default FindCollection
     */
    public CommandInsertMany(List<? extends DOC> documentsList, boolean ordered) {
        this(documentsList);
        CommandInsertMany.Options options = new CommandInsertMany.Options();
        options.setOrdered(options.ordered);
        payload.setOptions(options);
    }

    /**
     * Default FindCollection.
     *
     * @param documentsList
     *      list of documents to process
     */
    public CommandInsertMany(List<? extends DOC> documentsList) {
        super("insertMany");
        Assert.notNull(documentsList, "document");
        Assert.isTrue(!documentsList.isEmpty(), "document list cannot be empty");
        this.payload = new CommandInsertMany.Payload<>();
        payload.setDocuments(documentsList);
    }

    /**
     * Add the options to InsertMany.
     *
     * @param pOptions
     *      specialization for insert many option
     * @return
     *      current reference to insert many
     */
    public CommandInsertMany<DOC> withOptions(InsertManyOptions pOptions) {
        if (pOptions != null) {
            CommandInsertMany.Options options = new CommandInsertMany.Options();
            options.setOrdered(options.ordered);
            payload.setOptions(options);
        }
        return this;
    }

    @Data
    @NoArgsConstructor
    public static class Payload<DOC> {
        /**
         * List of documents to insert
         */
        List<? extends DOC> documents;
        /**
         * Options.
         */
        private Options options;
    }

    /**
     * Options of the FindOne command.
     */
    @Data
    public static class Options {
        Boolean ordered = false;
    }


}
