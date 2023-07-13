package io.stargate.test.grpc;

import io.stargate.sdk.grpc.StargateGrpcApiClient;
import io.stargate.sdk.grpc.domain.ResultSetGrpc;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

public class SampleTest {

    @Test
    public void testRequest() {
        StargateGrpcApiClient grpcClient = new StargateGrpcApiClient();
        System.out.println(grpcClient
                .execute("SELECT data_center from system.local")
                .one()
                .getString("data_center"));
    }
}
