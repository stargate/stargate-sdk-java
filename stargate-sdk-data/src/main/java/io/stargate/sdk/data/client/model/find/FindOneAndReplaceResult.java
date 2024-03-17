package io.stargate.sdk.data.client.model.find;

import io.stargate.sdk.data.client.model.DataApiResponse;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class FindOneAndReplaceResult<DOC> {

    /**
     * Replacement if provided
     */
    private DOC document;

    /**
     * Number of matched documents
     */
    private Integer matchedCount;

    /**
     * Number of modified documents
     */
    private Integer modifiedCount;


}
