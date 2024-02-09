package io.stargate.sdk.http.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Add user chunks to the UserAgent.
 */
@Data @AllArgsConstructor
public class UserAgentChunk {

    /**
     * Component name
     */
    String component;

    /**
     * Version number
     */
    String version;

}
