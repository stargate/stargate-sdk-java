package io.stargate.sdk.data;

/**
 * Each document will have a STATUS.
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
