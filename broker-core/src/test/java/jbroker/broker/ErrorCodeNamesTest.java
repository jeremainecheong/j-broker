package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

class ErrorCodeNamesTest {

    /**
     * Every public int constant on {@link ErrorCodes} must be addressable by
     * {@link ErrorCodeNames#name(int)} — the REST error envelope can't emit
     * an "UNKNOWN" string for a code the broker just produced.
     */
    @Test
    void everyKnownCodeResolvesToANonPlaceholderName() throws Exception {
        var codes = new ArrayList<Integer>();
        for (var f : ErrorCodes.class.getDeclaredFields()) {
            if (!Modifier.isPublic(f.getModifiers()) || f.getType() != int.class) continue;
            codes.add(f.getInt(null));
        }
        assertThat(codes).isNotEmpty();
        for (int code : codes) {
            assertThat(ErrorCodeNames.name(code))
                    .as("code %d must have a name", code)
                    .isNotEqualTo("UNKNOWN");
        }
    }

    @Test
    void unknownCodeResolvesToUnknownPlaceholder() {
        assertThat(ErrorCodeNames.name(99999)).isEqualTo("UNKNOWN");
    }

    @Test
    void restFacingCodesHaveStableNames() {
        assertThat(ErrorCodeNames.name(ErrorCodes.NONE)).isEqualTo("NONE");
        assertThat(ErrorCodeNames.name(ErrorCodes.NOT_LEADER)).isEqualTo("NOT_LEADER");
        assertThat(ErrorCodeNames.name(ErrorCodes.UNKNOWN_TOPIC)).isEqualTo("UNKNOWN_TOPIC");
        assertThat(ErrorCodeNames.name(ErrorCodes.COORDINATOR_NOT_AVAILABLE)).isEqualTo("COORDINATOR_NOT_AVAILABLE");
        assertThat(ErrorCodeNames.name(ErrorCodes.UNIMPLEMENTED)).isEqualTo("UNIMPLEMENTED");
    }
}
