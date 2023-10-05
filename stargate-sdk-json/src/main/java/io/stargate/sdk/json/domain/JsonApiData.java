package io.stargate.sdk.json.domain;

import lombok.Data;

import java.util.List;

@Data
public class JsonApiData {

    private List<JsonDocumentResult> documents;

    private String nextPageState;

}
