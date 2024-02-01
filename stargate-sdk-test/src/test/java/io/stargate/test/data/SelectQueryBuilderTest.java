package io.stargate.test.data;

import io.stargate.sdk.data.domain.query.Filter;
import io.stargate.sdk.data.domain.query.FilterBuilderList;
import io.stargate.sdk.data.domain.query.SelectQuery;
import io.stargate.sdk.utils.JsonUtils;
import org.junit.jupiter.api.Test;
import static io.stargate.sdk.http.domain.FilterOperator.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectQueryBuilderTest {

    @Test
    public void testSelectQuery() {
        Filter f1 = new Filter("product_price", EQUALS_TO , 9.99);
        SelectQuery sq1 = new SelectQuery(f1);
        System.out.println(JsonUtils.marshall(sq1));

        Filter f2 = new Filter().where("product_price").isEqualsTo(9.99);
        SelectQuery sq3 = SelectQuery.findWithFilter(f1);
        System.out.println(JsonUtils.marshall(sq3));
    }

    @Test
    public void testFilterAnd() {
        Filter f1 = new Filter()
                .and()
                .where("a", EQUALS_TO, 10)
                .where("b", EXISTS, true)
                .where("c", LESS_THAN_OR_EQUALS_TO, 5)
                .end();
        System.out.println(JsonUtils.marshall(f1));
    }

    @Test
    public void testFilterOr() {
        Filter f2 = new Filter()
                .or()
                .where("a", EQUALS_TO, 10)
                .where("b", EXISTS, true)
                .where("c", LESS_THAN_OR_EQUALS_TO, 5)
                .end();
        System.out.println(JsonUtils.marshall(f2));
    }

    @Test
    public void testFilterNot() {

        Filter f3 = new Filter()
                .not()
                .where("a", EQUALS_TO, 10)
                .end();
        System.out.println(JsonUtils.marshall(f3));
    }

    @Test
    public void testFilterNested() {
        Filter f4 = new Filter()
                .and()
                  .or()
                    .where("a", EQUALS_TO, 10)
                    .where("b", EXISTS, true)
                  .end()
                  .or()
                    .where("c", GREATER_THAN, 5)
                    .where("d", GREATER_THAN_OR_EQUALS_TO, 5)
                  .end()
                  .not()
                    .where("e", LESS_THAN, 5)
                  .end();

        System.out.println(JsonUtils.marshall(f4));
    }

    @Test
    public void testSelectQueryAndSimple() {



    }

    @Test
    public void testSelectQueryAnd() {

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
