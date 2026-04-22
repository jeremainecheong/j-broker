package jbroker.admin.dto;

import java.util.Map;

/**
 * Error envelope defined in PRD §8.7. The {@code error_code} field is the
 * canonical string name from {@code ErrorCodeNames} (e.g.
 * {@code "NOT_LEADER"}); {@code hint} is an open-ended map of suggestions
 * (current leader id/host/port, suggested retry delay, etc.).
 */
public record RestError(String errorCode, String message, Map<String, Object> hint) {
    public RestError {
        hint = hint == null ? Map.of() : Map.copyOf(hint);
    }

    public static RestError of(String code, String message) {
        return new RestError(code, message, Map.of());
    }
}
