package io.stargate.sdk.data.client.model.find;

import lombok.Data;

@Data
public class FindOneAndDeleteOptions {
    Object projection;
    Object sort;
}
