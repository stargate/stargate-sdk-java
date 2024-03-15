package io.stargate.sdk.data.client.model.insert;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents the result returned by command 'insertMany()', mainly the insertedIds.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class InsertManyResult {

    /** Inserted Ids. */
    List<Object> insertedIds =  new ArrayList<>();

}
