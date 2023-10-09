package io.stargate.sdk.json.domain;

import lombok.Data;

import java.util.List;

@Data
public class JsonApiData {

    private List<JsonResult> documents;

    private JsonResult document;

    private String nextPageState;

}
