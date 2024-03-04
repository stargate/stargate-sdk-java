package io.stargate.data_api.internal.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class InsertOneResult {

    public Object insertedId;
}
