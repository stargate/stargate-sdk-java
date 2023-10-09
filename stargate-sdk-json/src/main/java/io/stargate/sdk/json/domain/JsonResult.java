package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sdk.json.domain.JsonRecord;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

/**
 * Json Results.
 */
@Getter @Setter @NoArgsConstructor
public class JsonResult extends AbstractJsonRecord {

    @JsonProperty("$similarity")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    protected Float similarity;

    @JsonAnySetter
    protected Map<String, Object> data;
}
