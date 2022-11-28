package io.stargate.test.grpc;

import io.stargate.sdk.grpc.StargateGrpcApiClient;
import org.junit.jupiter.api.Test;

public class SampleTest {

    @Test
    public void testRequest() {
        StargateGrpcApiClient grpcClient = new StargateGrpcApiClient();
        System.out.println(grpcClient
                .execute("SELECT data_center from system.local")
                .getResults()
                .get(0).get("data_center"));
    }
}
