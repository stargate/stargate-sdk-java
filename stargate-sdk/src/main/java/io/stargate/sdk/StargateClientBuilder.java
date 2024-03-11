package io.stargate.sdk;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import io.stargate.sdk.auth.FixedTokenAuthenticationService;
import io.stargate.sdk.auth.TokenProvider;
import io.stargate.sdk.auth.StargateAuthenticationService;
import io.stargate.sdk.grpc.ServiceGrpc;
import io.stargate.sdk.http.HttpClientOptions;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.Utils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Stargate SDK allows to connect to multiple APIS and provides a wide range of options. With the more advanced settings we
 * need an abstraction for the configuration. It can then be loaded from multiple sources like a builder, an application.yaml, a
 * service discovery with different configuration loader.
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
@Slf4j
@Getter
public class StargateClientBuilder implements Serializable {

    /** Serial. */
    private static final long serialVersionUID = -4662012136342903695L;

    /** default datacenter id. */
    public static final String DEFAULT_DATACENTER = "dc1";

    // ------------------------------------------------
    // -----         Authentication           ---------
    // ------------------------------------------------

    /** user name. */
    protected String username;

    /** This the endPoint to invoke to work with different API(s). */
    protected String password;

    /** if provided the authentication URL is not use to get token. */
    protected String appToken = null;

    /** if an apiToken is provided it will be used for all nodes. */
    protected Map<String, StargateDataCenter> stargateNodesDC = new HashMap<>();

    /**
     * Fill username and password.
     * 
     * @param username
     *            user identifier
     * @param password
     *            password
     * @return current reference
     */
    public StargateClientBuilder withAuthCredentials(String username, String password) {
        checkNoCqlSession();
        Assert.hasLength(username, "username");
        Assert.hasLength(password, "password");
        this.username = username;
        this.password = password;
        withCqlOptionString(TypedDriverOption.AUTH_PROVIDER_USER_NAME, username);
        withCqlOptionString(TypedDriverOption.AUTH_PROVIDER_PASSWORD, password);
        withCqlOptionString(TypedDriverOption.AUTH_PROVIDER_CLASS,  PlainTextAuthProvider.class.getName());
        return this;
    }
    
    /**
     * Retrieve an {@link TokenProvider} based on token.
     * 
     * @param dc
     *            current DC
     * @return the token provider
     */
    public TokenProvider getApiTokenProvider(String dc) {
        Assert.hasLength(dc, "dc");
        TokenProvider dcTokenProvider = null;
        // As a token is explicitly provided (e.g: ASTRA) no computation of token
        if (Utils.hasLength(appToken)) {
            dcTokenProvider = new FixedTokenAuthenticationService(appToken);
        } else if (stargateNodesDC.containsKey(dc)) {
            // An ApiToken as been provided for the DC (can be different by DC, external config etc.)
            dcTokenProvider = stargateNodesDC.get(dc).getTokenProvider();
        } else {
            // No solution to enable authentication has been found
            throw new IllegalArgumentException("No token provider found for DC" + dc);
        }
        return dcTokenProvider;
    }
    
    /**
     * Api token available for all the nodes.
     * 
     * @param token
     *            current token
     * @return self reference
     */
    public StargateClientBuilder withApiToken(String token) {
        Assert.hasLength(token, "token");
        this.appToken = token;
        return this;
    }
    
    /**
     * You will get one token provider for a DC
     * 
     * @param url
     *      list of URL for authentication
     *      
     * @return slef reference
     */
    public StargateClientBuilder withApiTokenProvider(String... url) {
        Assert.notNull(url, "url list");
        return withApiTokenProviderDC(localDatacenter, url);
    }
    
    /**
     * Provide token provider for a DC.
     *
     * @param dc
     *            datacentername
     * @param tokenProvider
     *            token provider
     * @return self reference
     */
    public StargateClientBuilder withApiTokenProviderDC(String dc, TokenProvider tokenProvider) {
        Assert.hasLength(dc, "dc");
        if (!stargateNodesDC.containsKey(dc)) {
            stargateNodesDC.put(dc, new StargateDataCenter(dc, tokenProvider));
        }
        stargateNodesDC.get(dc).setTokenProvider(tokenProvider);
        return this;
    }

    /**
     * You will get one token provider for a DC
     * 
     * @param dc
     *            datacenter name
     * @param url
     *      list of URL for authentication
     * @return slef reference
     */
    public StargateClientBuilder withApiTokenProviderDC(String dc, String... url) {
        Assert.hasLength(dc, "dc");
        Assert.notNull(url, "url list");
        if (!Utils.hasLength(username)) {
            throw new IllegalStateException("Username is empty please .withAuthCredentials() before .withApiTokenProvider()");
        }
        return withApiTokenProviderDC(dc, new StargateAuthenticationService(username, password, url));
    }
    
    // ------------------------------------------------
    // -------  Topology of Nodes per DC --------------
    // ------------------------------------------------

    /** Local datacenter. */
    protected String localDatacenter;

    /**
     * Add a node to the cluster.
     * @param rest
     *      rest node
     * @return
     *      current builder
     */
    public StargateClientBuilder addServiceRest(ServiceHttp rest) {
        return addServiceRest(localDatacenter, rest);
    }

    /**
     * Populate current datacenter. Will be used as localDc in cqlSession if provided
     * or local DC at Http level.
     *
     * @param dc
     *     target datacenter
     * @param rest
     *      target service
     * @return
     *      current reference
     */
    public StargateClientBuilder addServiceRest(String dc, ServiceHttp rest) {
        if (!stargateNodesDC.containsKey(dc)) {
            stargateNodesDC.put(dc, new StargateDataCenter(dc));
        }
        this.stargateNodesDC.get(dc).addRestService(rest);
        return this;
    }

    /**
     * Populate current datacenter. Will be used as localDc in cqlSession if provided
     * or local DC at Http level.
     *
     * @param dc
     *     target datacenter
     * @param json
     *      target json service
     * @return
     *      current reference
     */
    public StargateClientBuilder addServiceJson(String dc, ServiceHttp json) {
        if (!stargateNodesDC.containsKey(dc)) {
            stargateNodesDC.put(dc, new StargateDataCenter(dc));
        }
        this.stargateNodesDC.get(dc).addJsonService(json);
        return this;
    }

    /**
     * Populate current datacenter. Will be used as localDc in cqlSession if provided
     * or local DC at Http level.
     *
     * @param dc
     *     target datacenter
     * @param rest
     *      target service
     * @return
     *      current reference
     */
    public StargateClientBuilder addDocumentService(String dc, ServiceHttp rest) {
        if (!stargateNodesDC.containsKey(dc)) {
            stargateNodesDC.put(dc, new StargateDataCenter(dc));
        }
        this.stargateNodesDC.get(dc).addDocumenService(rest);
        return this;
    }

    /**
     * Add a new Document Service.
     *
     * @param doc
     *      document service
     * @return
     *      current reference
     */
    public StargateClientBuilder addDocumentService(ServiceHttp doc) {
        return addDocumentService(localDatacenter, doc);
    }

    /**
     * Populate current datacenter. Will be used as localDc in cqlSession if provided
     * or local DC at Http level.
     *
     * @param dc
     *     target datacenter
     * @param rest
     *      target service
     * @return
     *      current reference
     */
    public StargateClientBuilder addGraphQLService(String dc, ServiceHttp rest) {
        if (!stargateNodesDC.containsKey(dc)) {
            stargateNodesDC.put(dc, new StargateDataCenter(dc));
        }
        this.stargateNodesDC.get(dc).addGraphQLService(rest);
        return this;
    }

    /**
     * Add a GraphQL Service in the current DC
     * @param rest
     *      current graphQL service
     * @return
     *      current reference.
     */
    public StargateClientBuilder addGraphQLService(ServiceHttp rest) {
        return addGraphQLService(localDatacenter, rest);
    }

    /**
     * Populate current datacenter. Will be used as localDc in cqlSession if provided
     * or local DC at Http level.
     *
     * @param dc
     *     target datacenter
     * @param grpc
     *      target service
     * @return
     *      current reference
     */
    public StargateClientBuilder addGrpcService(String dc, ServiceGrpc grpc) {
        if (!stargateNodesDC.containsKey(dc)) {
            stargateNodesDC.put(dc, new StargateDataCenter(dc));
        }
        this.stargateNodesDC.get(dc).addGrpcService(grpc);
        return this;
    }

    /**
     * Populate current datacenter. Will be used as localDc in cqlSession if provided
     * or local DC at Http level.
     *
     * @param grpc
     *      target service
     * @return
     *      current reference
     */
    public StargateClientBuilder addGrpcService(ServiceGrpc grpc) {
        return addGrpcService(localDatacenter, grpc);
    }

    /**
     * Populate current datacenter. Will be used as localDc in cqlSession if provided
     * or local DC at Http level.
     *
     * @param localDc
     *            localDataCernter Name
     * @return current reference
     */
    public StargateClientBuilder withLocalDatacenter(String localDc) {
        Assert.hasLength(localDc, "localDc");
        this.localDatacenter = localDc;
        // Only when you do not use SCB
        driverConfig.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, localDc);
        driverConfig.withBoolean(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS, true);
        cqlOptions.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, localDc);
        cqlOptions.put(TypedDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS, true);
        return this;
    }

    // ------------------------------------------------
    // -----            CqlSession            ---------
    // ------------------------------------------------
    
    /** If the flag is enabled the Cql Session will not be created even if parameters are ok. */
    protected boolean enabledCql = false;
   
    /** Providing Ad Hoc CqlSession. */
    protected CqlSession cqlSession = null;

    /** Dynamic configuration. */
    protected ProgrammaticDriverConfigLoaderBuilder driverConfig = new DefaultProgrammaticDriverConfigLoaderBuilder();

    /** 
     * Track options in the object to help retrive values. The configuration is loaded with a Programmatic Loader and not the options Map.
     **/
    protected OptionsMap cqlOptions = OptionsMap.driverDefaults();
    
    /** Metrics Registry if provided. */
    protected Object cqlMetricsRegistry;
    
    /** Request tracker. */ 
    protected RequestTracker cqlRequestTracker;
    
    /**
     * By default, Cql session is not created, you can enable the flag or using any
     * withCql* operations to enable.
     *
     * @return
     *      reference of current object
     */
    public StargateClientBuilder enableCql() {
        this.enabledCql = true;
        return this;
    }
    
    /**
     * Provide your own CqlSession skipping settings in the builder.
     *
     * @param cql
     *            existing session
     * @return
     *      self reference
     */
    public StargateClientBuilder withCqlSession(CqlSession cql) {
        Assert.notNull(cql, "cqlSession");
        checkNoCqlSession();
        enableCql();
        this.cqlSession = cql;
        return this;
    }
    
    /**
     * You want to initialize CqlSession with a configuration file
     * 
     * @param configFile
     *            a configuration file
     * @return the current reference
     */
    public StargateClientBuilder withCqlDriverConfigurationFile(File configFile) {
        Assert.notNull(configFile, "configFile");
        Assert.isTrue(configFile.exists(), "Config file should exists");
        return withCqlSession(CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(configFile)).build());
    }
    
    /**
     * Provide the proper programmatic config loader builder. It will be orverriden when 
     * used with Spring.
     * 
     * @param pdclb
     *      builder
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlDriverConfigLoaderBuilder(ProgrammaticDriverConfigLoaderBuilder pdclb) {
        this.driverConfig = pdclb;
        return this;
    }
    
    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionBoolean(TypedDriverOption<Boolean> option, Boolean du) {
        driverConfig.withBoolean(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionBooleanDC(String dc, TypedDriverOption<Boolean> option, Boolean du) {
        driverConfig.startProfile(dc).withBoolean(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }
    
    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionBooleanList(TypedDriverOption<List<Boolean>> option, List<Boolean> du) {
        driverConfig.withBooleanList(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionBooleanListDC(String dc, TypedDriverOption<List<Boolean>> option, List<Boolean> du) {
        driverConfig.startProfile(dc).withBooleanList(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }
    
    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionClass(TypedDriverOption<Class<?>> option, Class<?> du) {
        driverConfig.withClass(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionClassDC(String dc, TypedDriverOption<Class<?>> option, Class<?> du) {
        driverConfig.startProfile(dc).withClass(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }

    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionClassList(TypedDriverOption<List<Class<?>>> option, List<Class<?>> du) {
        driverConfig.withClassList(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionClassListDC(String dc, TypedDriverOption<List<Class<?>>> option, List<Class<?>> du) {
        driverConfig.startProfile(dc).withClassList(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }

    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionDouble(TypedDriverOption<Double> option, Double du) {
        driverConfig.withDouble(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionDoubleDC(String dc, TypedDriverOption<Double> option, Double du) {
        driverConfig.startProfile(dc).withDouble(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }
    
    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionDoubleList(TypedDriverOption<List<Double>> option, List<Double> du) {
        driverConfig.withDoubleList(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionDoubleListDC(String dc, TypedDriverOption<List<Double>> option, List<Double> du) {
        driverConfig.startProfile(dc).withDoubleList(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }
    
    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionDuration(TypedDriverOption<Duration> option, Duration du) {
        driverConfig.withDuration(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionDurationDC(String dc, TypedDriverOption<Duration> option, Duration du) {
        driverConfig.startProfile(dc).withDuration(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }
    
    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionDurationList(TypedDriverOption<List<Duration>> option, List<Duration> du) {
        driverConfig.withDurationList(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionDurationListDC(String dc, TypedDriverOption<List<Duration>> option, List<Duration> du) {
        driverConfig.startProfile(dc).withDurationList(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }
    
    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionInteger(TypedDriverOption<Integer> option, Integer du) {
        driverConfig.withInt(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionIntegerDC(String dc, TypedDriverOption<Integer> option, Integer du) {
        driverConfig.startProfile(dc).withInt(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }
    
    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionIntegerList(TypedDriverOption<List<Integer>> option, List<Integer> du) {
        driverConfig.withIntList(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionIntegerListDC(String dc, TypedDriverOption<List<Integer>> option, List<Integer> du) {
        driverConfig.startProfile(dc).withIntList(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }
    
    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionLong(TypedDriverOption<Long> option, Long du) {
        driverConfig.withLong(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionLongDC(String dc, TypedDriverOption<Long> option, Long du) {
        driverConfig.startProfile(dc).withLong(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }
    
    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionLongList(TypedDriverOption<List<Long>> option, List<Long> du) {
        driverConfig.withLongList(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionLongListDC(String dc, TypedDriverOption<List<Long>> option, List<Long> du) {
        driverConfig.startProfile(dc).withLongList(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }
    
    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionString(TypedDriverOption<String> option, String du) {
        driverConfig.withString(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionStringDC(String dc, TypedDriverOption<String> option, String du) {
        driverConfig.startProfile(dc).withString(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }
    
    /**
     * Add a property to the Cql Context.
     *
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionStringList(TypedDriverOption<List<String>> option, List<String> du) {
        driverConfig.withStringList(option.getRawOption(), du);
        cqlOptions.put(option, du);
        return this;
    }
    
    /**
     * Add a propery to the Cql Context.
     *
     * @param dc
     *      targate datacenter
     * @param option
     *      current option
     * @param du
     *      option value
     * @return
     *      current reference
     */
    public StargateClientBuilder withCqlOptionStringListDC(String dc, TypedDriverOption<List<String>> option, List<String> du) {
        driverConfig.startProfile(dc).withStringList(option.getRawOption(), du).endProfile();
        cqlOptions.put(dc, option, du);
        return this;
    }
    
    /**
     * Metrics registry.
     *
     * @param registry
     *      target metrics registry
     * @return
     *      client config
     */
    public StargateClientBuilder withCqlMetricsRegistry(Object registry) {
        Assert.notNull(registry, "registry");
        checkNoCqlSession();
        cqlMetricsRegistry = registry;
        return this;
    }
    
    /**
     * CqlRequest Tracker registry.
     *
     * @param cqlReqTracker
     *      cql tracker
     * @return
     *      client config
     */
    public StargateClientBuilder withCqlRequestTracker(RequestTracker cqlReqTracker) {
        Assert.notNull(cqlReqTracker, "RequestTracker");
        checkNoCqlSession();
        this.cqlRequestTracker = cqlReqTracker;
        return this;
    }
    
    /**
     * Set the consistency level
     * 
     * @param cl
     *            current consitency level
     * @return self reference
     */
    public StargateClientBuilder withCqlConsistencyLevel(ConsistencyLevel cl) {
        Assert.notNull(cl, "consistency level");
        driverConfig.withString(DefaultDriverOption.REQUEST_CONSISTENCY,  cl.name());
        // To be reused when dc failover
        cqlOptions.put(this.localDatacenter, TypedDriverOption.REQUEST_CONSISTENCY, cl.name());
        return this;
    }
    /**
     * Define consistency level for a DC.
     *
     * @param dc
     *            datacenter name
     * @param cl
     *            consistency level
     * @return self reference
     */
    public StargateClientBuilder withCqlConsistencyLevelDC(String dc, ConsistencyLevel cl) {
        Assert.hasLength(dc, "dc, datacenter name");
        Assert.notNull(cl, "consistency level");
        driverConfig.startProfile(dc).withString(DefaultDriverOption.REQUEST_CONSISTENCY, cl.name()).endProfile();
        // To be reused when dc failover
        cqlOptions.put(dc, TypedDriverOption.REQUEST_CONSISTENCY, cl.name());
        return this;
    }

    /**
     * Fill contact points.
     *
     * @param contactPoints
     *           contact points
     * @return current reference
     */
    public StargateClientBuilder withCqlContactPoints(String... contactPoints) {
        Assert.notNull(contactPoints, "contactPoints");
        Assert.isTrue(contactPoints.length>0, "contactPoints should not be null");
        Assert.hasLength(contactPoints[0], "one contact point");
        driverConfig.withStringList(DefaultDriverOption.CONTACT_POINTS, Arrays.asList(contactPoints));
        // To be reused when dc failover
        cqlOptions.put(this.localDatacenter, TypedDriverOption.CONTACT_POINTS, Arrays.asList(contactPoints));
        return this;
    }

    /**
     * Fill contact points.
     *
     * @param dc
     *            datacenter name
     * @param contactPoints
     *           contact points
     * @return current reference
     */
    public StargateClientBuilder withCqlContactPointsDC(String dc, String... contactPoints) {
        Assert.notNull(contactPoints, "contactPoints");
        Assert.isTrue(contactPoints.length>0, "contactPoints should not be null");
        Assert.hasLength(contactPoints[0], "one contact point");
        Assert.hasLength(dc, "dc, datacenter name");
        driverConfig.startProfile(dc).withStringList(DefaultDriverOption.CONTACT_POINTS, Arrays.asList(contactPoints)).endProfile();
        // To be reused when dc failover
        cqlOptions.put(dc, TypedDriverOption.CONTACT_POINTS, Arrays.asList(contactPoints));
        return this;
    }

    /**
     * Fill Keyspaces.
     *
     * @param keyspace
     *            keyspace name
     * @return current reference
     */
    public StargateClientBuilder withCqlKeyspace(String keyspace) {
        Assert.hasLength(keyspace, "keyspace");
        driverConfig.withString(DefaultDriverOption.SESSION_KEYSPACE, keyspace);
        // To be reused when dc failover
        cqlOptions.put(TypedDriverOption.SESSION_KEYSPACE, keyspace);
        return this;
    }

    /**
     * Providing SCB. (note it is one per Region).
     *
     * @param cloudConfigUrl
     *            configuration
     * @return current reference
     */
    public StargateClientBuilder withCqlCloudSecureConnectBundle(String cloudConfigUrl) {
        Assert.hasLength(cloudConfigUrl, "cloudConfigUrl");
        driverConfig.withString(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, cloudConfigUrl);
        cqlOptions.put(this.localDatacenter, TypedDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, cloudConfigUrl);
        return this;
    }
    
    /**
     * Providing SCB. (note it is one per Region).
     *
     * @param dc
     *          datacenter name
     * @param cloudConfigUrl
     *          configuration
     * @return current reference
     */
    public StargateClientBuilder withCqlCloudSecureConnectBundleDC(String dc, String cloudConfigUrl) {
        Assert.hasLength(cloudConfigUrl, "cloudConfigUrl");
        driverConfig.withString(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, cloudConfigUrl);
        cqlOptions.put(dc, TypedDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, cloudConfigUrl);
        return this;
    }
    
    /**
     * Accessor to driver config builder.
     *
     * @return
     *      driver config builder
     */
    public ProgrammaticDriverConfigLoaderBuilder getCqlDriverConfigLoaderBuilder() {
        return driverConfig;
    }

    /**
     * When working with builder you do no want the Cqlsession provided ad hoc.
     */
    private void checkNoCqlSession() {
        if (cqlSession != null) {
            throw new IllegalArgumentException("You cannot provide CqlOptions if a external CqlSession is used.");
        }
    }
   
    // ------------------------------------------------
    // -----            Grpc Session          ---------
    // ------------------------------------------------
    
    /** If the flag is enabled the Grpc Session will not be created even if parameters are ok. */
    protected boolean enabledGrpc = false;
    
    /**
     * By default Cqlsession is not created, you can enable the flag or using any
     * withCql* operations to enableit.
     *
     * @return
     *      reference of current object
     */
    public StargateClientBuilder enableGrpc() {
        this.enabledGrpc = true;
        return this;
    }

    // ------------------------------------------------
    // ------------- HTTP Client ----------------------
    // ------------------------------------------------


    /** Observers. */
    protected HttpClientOptions httpClientOptions = HttpClientOptions.builder().build();

    /**
     * Enable fine Grained configuration of the HTTP Client.
     *
     * @param options
     *            http client options
     * @return self reference
     */
    public StargateClientBuilder withTttpClientOptions(HttpClientOptions options) {
        Assert.notNull(options, "options");
        this.httpClientOptions = options;
        return this;
    }
    
    // ------------------------------------------------
    // ------------------  MAIN -----------------------
    // ------------------------------------------------
    
    /**
     * Load defaults from Emvironment variables
     */
    public StargateClientBuilder() {
    }
    
    /**
     * Building a StargateClient from the values in the BUILDER
     *
     * @return
     *     instance of the client
     */
    public StargateClient build() {
        return new StargateClient(this);
    }
}
