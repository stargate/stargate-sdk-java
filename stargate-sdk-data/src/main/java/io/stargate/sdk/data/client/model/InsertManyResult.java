package io.stargate.sdk.data.client.model;


import lombok.Data;

import java.util.Map;

@Data
public class InsertManyResult {

    Map<Integer, Object> insertedIds;

}
