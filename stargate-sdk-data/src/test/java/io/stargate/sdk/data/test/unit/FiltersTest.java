package io.stargate.sdk.data.test.unit;

import io.stargate.sdk.data.client.model.Filter;
import io.stargate.sdk.data.client.model.Filters;
import io.stargate.sdk.utils.JsonUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FiltersTest {

    @Test
    public void testFiltersSerializations() {
        Filter f = Filters.eq("hello", 3);
        assertThat(JsonUtils.marshallForDataApi(f)).isEqualTo("{\"filter\":{\"hello\":3}}");
    }
}
