package jbroker.admin.api;

import io.grpc.StatusRuntimeException;
import jbroker.admin.dto.RestError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Converts internal failures into the PRD §8.7 REST error envelope. Keep
 * the mapping narrow — one handler per recoverable exception type — so
 * controllers stay free of error-translation boilerplate.
 */
@RestControllerAdvice
public class RestExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(RestExceptionHandler.class);

    @ExceptionHandler(StatusRuntimeException.class)
    public ResponseEntity<RestError> handleGrpcFailure(StatusRuntimeException ex) {
        log.warn("gRPC call failed: {}", ex.getStatus(), ex);
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(RestError.of(
                        ex.getStatus().getCode().name(), ex.getMessage() == null ? "gRPC error" : ex.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<RestError> handleUnreachable(IllegalStateException ex) {
        // firstSuccessful() throws IllegalStateException when every broker is
        // unreachable — surface as 503 so downstream clients (k6 / UI) can
        // retry or back off.
        log.warn("no broker reachable: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(RestError.of("CLUSTER_UNREACHABLE", ex.getMessage()));
    }
}
