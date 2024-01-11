package io.stargate.sdk.data.domain;


import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
public class UpsertResult {

    List<String> insertedIds;

    List<String> updatedIds;

    public List<String> getAllIds() {
        return Stream.of(Optional.ofNullable(insertedIds), Optional.ofNullable(updatedIds))
                .flatMap(optList -> optList.orElseGet(ArrayList::new).stream())
                .collect(Collectors.toList());
    }

}
