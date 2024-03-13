package io.stargate.sdk.data.client.model.misc;

import lombok.Data;

/**
 * Object returned by a count Operation.
 */
@Data
public class CountDocumentsResult {

    Boolean moreData;

    int count;
}
