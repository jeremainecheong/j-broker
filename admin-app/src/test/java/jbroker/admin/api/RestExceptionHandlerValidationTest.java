package jbroker.admin.api;

import static org.assertj.core.api.Assertions.assertThat;

import jbroker.admin.dto.RestError;
import org.junit.jupiter.api.Test;
import org.springframework.core.MethodParameter;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;

/**
 * unit coverage for the validation branch of {@link RestExceptionHandler}.
 * Builds a synthetic {@code MethodArgumentNotValidException} with two field
 * errors and asserts the handler maps them into the RestError envelope.
 */
final class RestExceptionHandlerValidationTest {

    @Test
    void twoFieldErrorsProduceValidationFailedEnvelope() throws Exception {
        BindingResult br = new org.springframework.validation.BeanPropertyBindingResult(new Object(), "target");
        br.addError(new FieldError("target", "name", "must not be blank"));
        br.addError(new FieldError("target", "partitions", "must be ≥ 1"));
        // Use any public constructor available on the target method. We pick
        // String(byte[]) because it's always resolvable across JDKs.
        var paramMethod = String.class.getDeclaredConstructor(byte[].class);
        var param = MethodParameter.forExecutable(paramMethod, 0);
        MethodArgumentNotValidException ex = new MethodArgumentNotValidException(param, br);

        var handler = new RestExceptionHandler();
        var resp = handler.handleValidation(ex);

        assertThat(resp.getStatusCode().value()).isEqualTo(400);
        RestError body = resp.getBody();
        assertThat(body).isNotNull();
        assertThat(body.errorCode()).isEqualTo("VALIDATION_FAILED");
        assertThat(body.message()).contains("name").contains("must not be blank");
        assertThat(body.hint()).containsEntry("name", "must not be blank").containsEntry("partitions", "must be ≥ 1");
    }
}
