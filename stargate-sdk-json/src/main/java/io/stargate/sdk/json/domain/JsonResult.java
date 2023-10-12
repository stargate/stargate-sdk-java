package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

/**
 * Json Results.
 */
@Getter @Setter
public class JsonResult extends AbstractJsonRecord {

    /**
     * Similarity value in the response.
     */
    @JsonProperty("$similarity")
    protected Float similarity;

    /**
     * Output as a map (to use JsonAySetter annotation).
     */
    @JsonAnySetter
    protected Map<String, Object> data;

    /**
     * Default constructor.
     */
    public JsonResult() {
    }

}
