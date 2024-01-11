package io.stargate.test.data;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.data.CollectionClient;
import io.stargate.sdk.data.DataApiClient;
import io.stargate.sdk.data.domain.JsonResult;
import io.stargate.sdk.data.domain.JsonResultUpdate;
import io.stargate.sdk.data.domain.odm.Document;
import io.stargate.sdk.data.domain.odm.Result;
import io.stargate.sdk.data.domain.query.SelectQuery;
import io.stargate.sdk.data.domain.query.UpdateQuery;
import io.stargate.sdk.data.domain.query.UpdateQueryBuilder;
import io.stargate.sdk.test.json.AbstractJsonClientNamespacesTest;
import io.stargate.sdk.utils.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.stream.Stream;


/**
 * Default settings (localhost:8180).
 */
public class DataApiClientTest extends AbstractJsonClientNamespacesTest {
    @BeforeAll
    public static void initStargateRestApiClient() {
        jsonApiClient = new DataApiClient();
        jsonApiClient.dropNamespace(TEST_NAMESPACE_1);
        jsonApiClient.dropNamespace(TEST_NAMESPACE_2);
    }

    @Test
    public void testFindOneById() {
        CollectionClient colClient = jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION_VECTOR);
        Optional<JsonResult> opt = colClient.findById("pf1843");
        Assertions.assertTrue(opt.isPresent());
        System.out.println(opt.get());
    }

    @Test
    public void testFindOneByProperty() {
        CollectionClient colClient = jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION_VECTOR);
        Optional<JsonResult> opt = colClient.findOne(SelectQuery.builder()
                .where("product_price").isEqualsTo(9.99)
                .build());
        Assertions.assertTrue(opt.isPresent());
        System.out.println(opt.get());
    }

    @Test
    public void testFindOneVector() {
        CollectionClient colClient = jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION_VECTOR);
        Optional<JsonResult> opt = colClient.findOneByVector(new float[] {1.0f,1.0f,1.0f,1.0f,1.0f,0.0f,0.0f,0.0f,0.0f,0.0f,0.0f,0.0f,0.0f,0.0f});
        Assertions.assertTrue(opt.isPresent());
        System.out.println(opt.get());

        Result<Product> vector = new Result<>(colClient.findById("pf1844").get(), Product.class);
    }

    @Test
    public void  testFindMany() {

        CollectionClient colClient = jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION_VECTOR);

        Page<JsonResult> page = colClient.findPage(SelectQuery.builder()
                // Projection
                //.selectVector()
                //.selectSimilarity()
                // ann search
                //.orderByAnn(1f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f)
                .withLimit(2)
                .build());
        System.out.println("Result Size=" + page.getPageSize());
        for(JsonResult result : page.getResults()) {
            System.out.println(result.toString());
        }
    }

    @Test
    @Disabled
    public void testInsert100() {
        CollectionClient col1 = jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION);

        for(int i=0; i<100; i++) {
            col1.insertOne(new Document<>(new Product("product"+i, 9.99)));
        }
    }

    @Test
    @Disabled
    public void testFindWithPaging() {
        CollectionClient col1 = jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION);
        Page<JsonResult> resultSet = col1.findPage(SelectQuery.builder().build());
        System.out.println("Page 1: state=" + resultSet.getPageState());
        for(JsonResult result : resultSet.getResults()) {
            System.out.println(result.toString());
        }
    }

    @Test
    @Disabled
    public void testFindAll() {
        CollectionClient col1 = jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION);
        System.out.println(col1.countDocuments());
        Stream<JsonResult> resultSet = col1.find(SelectQuery.builder().build());
        System.out.println(resultSet.count());
    }

    @Test
    @Disabled
    public void testDeleteOne() {
        CollectionClient vector1 = jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION_VECTOR);
        System.out.println(vector1.deleteById("pf1843"));
    }

    @Test
    public void findOneAndUpdateTest() {
        jsonApiClient.createNamespace(TEST_NAMESPACE_1);
        jsonApiClient.namespace(TEST_NAMESPACE_1).createCollection(TEST_COLLECTION);
        CollectionClient col1 = jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION);

        JsonResultUpdate result = col1.findOneAndUpdate(UpdateQuery.builder()
                .where("_id").isEqualsTo("9")
                .updateSet("val1", "updated_from_code")
                .withReturnDocument(UpdateQueryBuilder.ReturnDocument.after)
                .enableUpsert()
                .build());
        System.out.println(JsonUtils.marshall(result));
    }


}
