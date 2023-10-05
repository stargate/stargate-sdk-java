package io.stargate.sdk.json.domain;

import lombok.Data;

@Data
public class JsonApiError {

    String message;

    String errorCode;

    String exceptionClass;

}
