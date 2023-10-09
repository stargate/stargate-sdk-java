package io.stargate.sdk.audit;

import io.stargate.sdk.Service;
import com.evanlennick.retry4j.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import static io.stargate.sdk.utils.AnsiUtils.*;


/**
 * Listener that log call in the Db
 *
 * @author Cedrick LUNVEN (@clunven)
 *
 */
public class ServiceCallObserverAnsiLogger implements ServiceCallObserver<String, Service, ServiceCallEvent<Service>> {

    /** Logger for our Client. */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceCallObserverAnsiLogger.class);

    /**
     * Default constructor.
     */
    public ServiceCallObserverAnsiLogger() {}

    /** {@inheritDoc} */
    @Override
    public void onCall(ServiceCallEvent<Service> event) {
        LOGGER.info("Service [" + yellow(event.getService().getId()) + "]");
        LOGGER.info("[" + yellow(event.getRequestId()) + "] Endpoint         : [" + green(event.getService().getId()) + "]");
        LOGGER.info("Request [" + yellow(event.getRequestId()) + "]");
        LOGGER.info("[" + yellow(event.getRequestId()) + "] Date             : [" + green(new Date(event.getTimestamp()).toString()) + "]");
        LOGGER.info("Response [" + magenta(event.getRequestId()) + "]");
        LOGGER.info("[" + magenta(event.getRequestId()) + "] Elapse Time      : [" + green(String.valueOf(event.getResponseElapsedTime())) + "] millis");
        LOGGER.info("[" + magenta(event.getRequestId()) + "] Total Time       : [" + green(String.valueOf(event.getResponseTime())) + "] millis");
        LOGGER.info("[" + magenta(event.getRequestId()) + "] Total Tries      : [" + green(String.valueOf(event.getTotalTries())) + "]");
        if (event.getErrorClass() != null) {
            LOGGER.info("Errors [" + red(event.getRequestId()) + "]");
            LOGGER.error("[" + red(event.getRequestId()) + "] Error Class      : [" + green(event.getErrorClass()) + "]");
            LOGGER.error("[" + red(event.getRequestId()) + "] Error Message    : [" + green(event.getErrorMessage()) + "]");
            LOGGER.error("[" + red(event.getRequestId()) + "] Error Exception  : [" + green(event.getLastException().getClass().getName()) + "]");
        }
    }

    @Override
    public void onSuccess(Status<String> s) {
        LOGGER.info("SUCCESS");
    }

    @Override
    public void onCompletion(Status<String> s) {
        LOGGER.info("COMPLETION");
    }

    @Override
    public void onFailure(Status<String> s) {
        LOGGER.info("FAILURE");
    }

    @Override
    public void onFailedTry(Status<String> s) {
        LOGGER.info("FAILED_TRY");
    }
}
