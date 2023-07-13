package io.stargate.sdk.grpc;

import io.stargate.sdk.ManagedServiceDeployment;
import io.stargate.sdk.ServiceDeployment;
import io.stargate.sdk.api.ApiConstants;
import io.stargate.sdk.grpc.domain.BatchGrpc;
import io.stargate.sdk.grpc.domain.QueryGrpc;
import io.stargate.sdk.grpc.domain.ResultSetGrpc;
import io.stargate.sdk.loadbalancer.LoadBalancedResource;
import io.stargate.sdk.loadbalancer.NoneResourceAvailableException;
import io.stargate.sdk.loadbalancer.UnavailableResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

/**
 * Client to achieve load balancing and fail over across grpc endpoints.
 */
public class GrpcClientLoadBalanced implements ApiConstants {

    /** Logger for our Client. */
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcClientLoadBalanced.class);

    /**
     * Hold the configuration of the cluster with list of dc and service instances.
     */
    private final ManagedServiceDeployment<ServiceGrpc> deployment;

    /**
     * Complete configuration.
     * @param conf
     *      configuration
     */
    public GrpcClientLoadBalanced(ServiceDeployment<ServiceGrpc> conf) {
        this.deployment = new ManagedServiceDeployment<>(conf);
    }

    /**
     * Execute query: Pick an available resource from the
     * load-balancer trigger a request with retries.
     *
     * @param query
     *      current grpc query.
     * @return
     *    service response
     */
    public ResultSetGrpc execute(QueryGrpc query) {
        LoadBalancedResource<ServiceGrpc> lb = null;
        while (true) {
            try {
                // Get an available node from LB
                lb = deployment.lookupStargateNode();
                ServiceGrpc serviceGrpc = lb.getResource();
                if (serviceGrpc != null) {
                    return GrpcClient
                            .getInstance()
                            .execute(serviceGrpc, query, deployment.lookupToken());
                }
            } catch(UnavailableResourceException rex) {
                LOGGER.warn("A stargate node is down [], falling back to another node...");
                try {
                    deployment.failOverStargateNode(lb, rex);
                } catch (NoneResourceAvailableException nex) {
                    LOGGER.warn("No node available is localDc [{}], falling back to another DC if available ...",
                            deployment.getLocalDatacenterClient().getDatacenterName());
                    deployment.failOverDatacenter();
                }
            } catch(NoneResourceAvailableException nex) {
                LOGGER.warn("No node available is DataCenter [{}], falling back to another DC if available ...",
                        deployment.getLocalDatacenterClient().getDatacenterName());
                deployment.failOverDatacenter();
            }
        }
    }

    /**
     * Execute a batch coming from elsewhere.
     *
     * @param batch
     *      gRPC batch
     * @return
     *      service response
     */
    public ResultSetGrpc executeBatch(BatchGrpc batch) {
        LoadBalancedResource<ServiceGrpc> lb = null;
        while (true) {
            try {
                // Get an available node from LB
                lb = deployment.lookupStargateNode();
                ServiceGrpc serviceGrpc = lb.getResource();
                if (serviceGrpc != null) {
                    return GrpcClient
                            .getInstance()
                            .executeBatch(serviceGrpc, batch, deployment.lookupToken());
                }
            } catch(UnavailableResourceException rex) {
                LOGGER.warn("A stargate node is down [], falling back to another node...");
                try {
                    deployment.failOverStargateNode(lb, rex);
                } catch (NoneResourceAvailableException nex) {
                    LOGGER.warn("No node available is localDc [{}], falling back to another DC if available ...",
                            deployment.getLocalDatacenterClient().getDatacenterName());
                    deployment.failOverDatacenter();
                }
            } catch(NoneResourceAvailableException nex) {
                LOGGER.warn("No node available is DataCenter [{}], falling back to another DC if available ...",
                        deployment.getLocalDatacenterClient().getDatacenterName());
                deployment.failOverDatacenter();
            }
        }
    }

    /**
     * Execute an asynchronous query.
     *
     * @param  query
     *      gRPC query
     * @return
     *      service response
     */
    public CompletableFuture<ResultSetGrpc> executeAsync(QueryGrpc query) {
        LoadBalancedResource<ServiceGrpc> lb = null;
        while (true) {
            try {
                // Get an available node from LB
                lb = deployment.lookupStargateNode();
                ServiceGrpc sGrpc = lb.getResource();
                return GrpcClient.getInstance().executeAsync(sGrpc, query, deployment.lookupToken());
            } catch (UnavailableResourceException rex) {
                LOGGER.warn("A stargate node is down [], falling back to another node...");
                try {
                    deployment.failOverStargateNode(lb, rex);
                } catch (NoneResourceAvailableException nex) {
                    LOGGER.warn("No node available is localDc [{}], falling back to another DC if available ...",
                            deployment.getLocalDatacenterClient().getDatacenterName());
                    deployment.failOverDatacenter();
                }
            } catch (NoneResourceAvailableException nex) {
                LOGGER.warn("No node available is DataCenter [{}], falling back to another DC if available ...",
                        deployment.getLocalDatacenterClient().getDatacenterName());
                deployment.failOverDatacenter();
            }
        }
    }

    /**
     * Execute a query getting back a flux.
     *
     * @param query
     *      input query
     * @return
     *      flux of data
     */
    public Mono<ResultSetGrpc> executeReactive(QueryGrpc query) {
        LoadBalancedResource<ServiceGrpc> lb = null;
        while (true) {
            try {
                lb = deployment.lookupStargateNode();
                return GrpcClient.getInstance().executeReactive(lb.getResource(), query, deployment.lookupToken());
            } catch (UnavailableResourceException rex) {
                LOGGER.warn("A stargate node is down [], falling back to another node...");
                try {
                    deployment.failOverStargateNode(lb, rex);
                } catch (NoneResourceAvailableException nex) {
                    LOGGER.warn("No node available is localDc [{}], falling back to another DC if available ...",
                            deployment.getLocalDatacenterClient().getDatacenterName());
                    deployment.failOverDatacenter();
                }
            } catch (NoneResourceAvailableException nex) {
                LOGGER.warn("No node available is DataCenter [{}], falling back to another DC if available ...",
                        deployment.getLocalDatacenterClient().getDatacenterName());
                deployment.failOverDatacenter();
            }
        }
    }

}
