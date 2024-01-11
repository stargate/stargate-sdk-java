package io.stargate.test.data;

import io.stargate.sdk.data.domain.query.SelectQuery;
import io.stargate.sdk.utils.JsonUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectQueryBuilderTest {

    @Test
    public void testSelectQuery() {
        SelectQuery sq1 = SelectQuery.builder()
                .where("product_price").isEqualsTo(9.99)
                .build();
        System.out.println(JsonUtils.marshall(sq1));

        SelectQuery sq2 = new SelectQuery();
        sq2.setFilter(new HashMap<>());
        Map<String, List<Map<String, Object>>> or1Criteria = new HashMap<>();
        or1Criteria.put("$or", new ArrayList<Map<String, Object>>());
        or1Criteria.get("$or").add(Map.of("product_price", 9.99));
        or1Criteria.get("$or").add(Map.of("product_name", "HealthyFresh - Beef raw dog food"));

        Map<String, List<Map<String, Object>>> or2Criteria = new HashMap<>();
        or2Criteria.put("$or", new ArrayList<Map<String, Object>>());
        or2Criteria.get("$or").add(Map.of("product_price", 12.99));
        or2Criteria.get("$or").add(Map.of("product_name", "HealthyFresh - Beef raw dog food"));

        List<Map<String, List<Map<String, Object>>>> andCriteria = new ArrayList<>();
        andCriteria.add(or1Criteria);
        andCriteria.add(or2Criteria);
        sq2.getFilter().put("$and", andCriteria);

        System.out.println(JsonUtils.marshall(sq2));
    }
}
