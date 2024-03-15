package io.stargate.sdk.data.client.observer;

import io.stargate.sdk.data.client.model.DataApiCommandExecutionInfos;
import io.stargate.sdk.data.client.model.DataApiData;
import io.stargate.sdk.data.client.model.DataApiError;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.List;
import java.util.UUID;

import static io.stargate.sdk.utils.AnsiUtils.cyan;
import static io.stargate.sdk.utils.AnsiUtils.magenta;
import static io.stargate.sdk.utils.AnsiUtils.yellow;

/**
 * Logging of the command.
 */
public class LoggerCommandObserver implements DataApiCommandObserver {

    private final Logger logger;
    /**
     * Log level.
     */
    private final Level logLevel;

    /**
     * Initialize with the logLevel.
     *
     * @param sourceName
     *      source name
     */
    public LoggerCommandObserver(String sourceName) {
        this(Level.DEBUG, sourceName);
    }

    /**
     * Initialize with the logLevel.
     *
     * @param sourceClass
     *      list source class
     */
    public LoggerCommandObserver(Class<?> sourceClass) {
        this(Level.DEBUG, sourceClass);
    }

    /**
     * Initialize with the logLevel.
     *
     * @param logLevel
     *      current log level
     * @param sourceClass
     *      source class
     */
    public LoggerCommandObserver(Level logLevel, Class<?> sourceClass) {
        this.logLevel = logLevel;
        this.logger   = LoggerFactory.getLogger(sourceClass);
    }

    /**
     * Initialize with the logLevel.
     *
     * @param logLevel
     *      current log level
     * @param sourceName
     *      source name
     */
    public LoggerCommandObserver(Level logLevel, String sourceName) {
        this.logLevel = logLevel;
        this.logger   = LoggerFactory.getLogger(sourceName);
    }

    /** {@inheritDoc} */
    @Override
    public void onCommand(DataApiCommandExecutionInfos executionInfo) {
        if (executionInfo != null) {
            String req = UUID.randomUUID().toString().substring(30);
            // Log Command
            log("Command [" + cyan(executionInfo.getCommand().getName()) + "] at [" + cyan(executionInfo.getExecutionDate().toString()) + "]");
            log(magenta(".[request_" + req + "]") + "=" + yellow("{}"),
                    JsonUtils.marshallForDataApi(executionInfo.getCommand()));
            log(magenta(".[response_" + req + "]") + "=" + yellow("{}"),
                    JsonUtils.marshallForDataApi(executionInfo.getResponse()));
            log(magenta(".[responseTime_" + req + "]") + "=" + yellow("{}") + " millis.",
                    executionInfo.getExecutionTime());
            // Log Data
            DataApiData data = executionInfo.getResponse().getData();
            if (data != null && data.getDocument() != null) {
                log(magenta(".[apiData/document_" + req + "]") + "=" + yellow("1 document retrieved, id='{}'"), data.getDocument().get(Document.ID));
            }
            if (data != null && data.getDocuments() != null) {
                log(magenta(".[apiData/documents_" + req + "]") + "=" + yellow("{} document(s)."), data.getDocuments().size());
            }

            // Log Errors
            List<DataApiError> errors = executionInfo.getResponse().getErrors();
            if (errors != null) {
                log(magenta(".[errors_" + req + "]") + "="+ yellow("{}") +" errors detected.", errors.size());
                for (DataApiError error : errors) {
                    log(magenta(".[errors_" + req + "]") + "="+ yellow("{} [code={}]"), error.getErrorMessage(), error.getErrorCode());
                }
            }
        }
    }

    /**
     * Convenient method to adjust dynamically the log level.
     * @param message
     *      log message
     * @param params
     *      arguments for the log message.
     */
    public void log(String message, Object... params) {
        switch (this.logLevel) {
            case TRACE:
                logger.trace(message, params);
                break;
            case DEBUG:
                logger.debug(message, params);
                break;
            case INFO:
                logger.info(message, params);
                break;
            case WARN:
                logger.warn(message, params);
                break;
            case ERROR:
                logger.error(message, params);
                break;
        }
    }
}
