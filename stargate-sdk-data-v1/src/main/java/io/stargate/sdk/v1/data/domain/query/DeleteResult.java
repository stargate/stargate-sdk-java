package io.stargate.sdk.v1.data.domain.query;

import io.stargate.sdk.v1.data.domain.ApiResponse;
import lombok.Data;

/**
 * Hold response when you delete document.
 */
@Data
public class DeleteResult {

    /**
     * number of deleted documents
     */
    int deletedCount = 0;

    /**
     * asset if more data are needed to be deleted
     */
    boolean moreData = false;

    /**
     * All arguments constructor.
     *
     * @param deletedCount
     *      deleted count
     * @param moreData
     *      more data
     */
    public DeleteResult(int deletedCount, boolean moreData) {
        this.deletedCount = deletedCount;
        this.moreData = moreData;
    }

    /**
     * Constructor with api response.
     *
     * @param apiResponse
     *      data api response
     */
    public DeleteResult(ApiResponse apiResponse) {
        if (apiResponse!=null) {
            if (apiResponse.getStatus().containsKey("deletedCount")) {
                deletedCount = (Integer) apiResponse.getStatus().get("deletedCount");
            }
            if (apiResponse.getStatus().containsKey("moreData")) {
                moreData = (Boolean) apiResponse.getStatus().get("moreData");
            }
        }
    }
}
