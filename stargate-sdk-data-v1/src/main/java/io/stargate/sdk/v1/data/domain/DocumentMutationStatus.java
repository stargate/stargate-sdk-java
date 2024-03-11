package io.stargate.sdk.v1.data.domain;

/**
 * Status and document mutation like <code>CREATED</code>, <code>UPDATED</code> or <code>UNCHANGED</code>...
 */
public enum DocumentMutationStatus {
    /** Document Created. */
    CREATED,

    /** Document Updated. */
    UPDATED,

    /** Document existed but not change needed. */
    UNCHANGED,

    /** Document Untouched as error before. */
    NOT_PROCESSED,

    /** Error already exists. */
    ALREADY_EXISTS;
}
