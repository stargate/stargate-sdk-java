package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sdk.http.domain.FilterKeyword;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Json Results.
 */
@Getter @Setter @NoArgsConstructor
public class JsonDocumentResult extends JsonDocument {

    @JsonProperty("$similarity")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Double similarity;
}
