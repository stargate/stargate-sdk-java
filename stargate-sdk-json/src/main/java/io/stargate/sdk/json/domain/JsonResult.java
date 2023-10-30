package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * Json Results.
 */
@Getter @Setter
public class JsonResult extends AbstractDocument {

    /**
     * Similarity value in the response, can be null, using an Object
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
