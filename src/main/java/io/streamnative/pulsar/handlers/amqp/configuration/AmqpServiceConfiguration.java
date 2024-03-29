package io.streamnative.pulsar.handlers.amqp.configuration;


import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;

/**
 * Amqp on Pulsar service configuration object.
 */
@Getter
@Setter
public class AmqpServiceConfiguration extends ServiceConfiguration {

    @Category
    private static final String CATEGORY_AMQP = "AMQP on Pulsar";
    @Category
    private static final String CATEGORY_AMQP_PROXY = "AMQP Proxy";
    @Category
    private static final String CATEGORY_AMQP_SSL = "AMQP Proxy SSL";

    //
    // --- AMQP on Pulsar Broker configuration ---
    //

    @FieldContext(
        category = CATEGORY_AMQP,
        required = true,
        doc = "Amqp on Pulsar Broker tenant"
    )
    private String amqpTenant = "public";

    @FieldContext(
        category = CATEGORY_AMQP,
        required = true,
        doc = "The tenant used for storing Amqp metadata topics"
    )
    private String amqpMetadataTenant = "public";

    @FieldContext(
        category = CATEGORY_AMQP,
        required = true,
        doc = "The namespace used for storing Amqp metadata topics"
    )
    private String amqpMetadataNamespace = "__amqp";

    @FieldContext(
        category = CATEGORY_AMQP,
        required = false,
        doc = "The namespace used for storing Amqp metadata topics"
    )
    private String amqpListeners;

    @FieldContext(
        category = CATEGORY_AMQP,
        required = true,
        doc = "The maximum number of channels which can exist concurrently on a connection."
    )
    private int amqpMaxNoOfChannels = 64;

    @FieldContext(
        category = CATEGORY_AMQP,
        required = true,
        doc = "The maximum frame size on a connection."
    )
    private int amqpMaxFrameSize = 4 * 1024 * 1024;

    @FieldContext(
        category = CATEGORY_AMQP,
        required = true,
        doc = "The default heartbeat timeout on broker"
    )
    private int amqpHeartBeat = 60 * 1000;

    @FieldContext(
        category = CATEGORY_AMQP_PROXY,
        required = false,
        doc = "The amqp proxy port"
    )
    private int amqpProxyPort = 5682;

    @FieldContext(
        category = CATEGORY_AMQP_PROXY,
        required = false,
        doc = "Whether start amqp protocol handler with proxy"
    )
    private boolean amqpProxyEnable = false;

    @FieldContext(
        category = CATEGORY_AMQP,
        required = true,
        doc = "The aop admin service port"
    )
    private int amqpAdminPort = 15673;

    @FieldContext(
        category = CATEGORY_AMQP,
        required = true,
        doc = "The aop metrics service port"
    )
    private int amqpMetricsPort = 15692;

    @FieldContext(
        category = CATEGORY_AMQP,
        required = false
    )
    private int amqpExplicitFlushAfterFlushes = 1000;

    @FieldContext(
        category = CATEGORY_AMQP,
        required = false,
        doc = "Exchange route queue size."
    )
    private int amqpExchangeRouteQueueSize = 200;

    @FieldContext(
        category = CATEGORY_AMQP,
        required = false,
        doc = "Threads count for route exchange messages."
    )
    private int amqpExchangeRouteExecutorThreads = Runtime.getRuntime().availableProcessors();

    @FieldContext(
        category = CATEGORY_AMQP,
        doc = "is the amqp authentication open"
    )
    private boolean amqpAuthenticationEnabled = false;

    @FieldContext(
        category = CATEGORY_AMQP,
        doc = "is the amqp authorization open"
    )
    private boolean amqpAuthorizationEnabled = false;

    @FieldContext(
        category = CATEGORY_AMQP_SSL,
        doc = "is the amqp proxy ssl open"
    )
    private boolean amqpTlsEnabled = false;

    @FieldContext(
        category = CATEGORY_AMQP_SSL,
        doc = "amqp ssl configuration"
    )
    private String amqpSslProtocol = "TLSv1.2";

    @FieldContext(
        category = CATEGORY_AMQP_SSL,
        doc = "amqp ssl configuration"
    )
    private String amqpSslKeystoreType = "PKCS12";

    @FieldContext(
        category = CATEGORY_AMQP_SSL,
        doc = "amqp ssl configuration"
    )
    private String amqpSslKeystoreLocation;

    @FieldContext(
        category = CATEGORY_AMQP_SSL,
        doc = "amqp ssl configuration"
    )
    private String amqpSslKeystorePassword;

    @FieldContext(
        category = CATEGORY_AMQP_SSL,
        doc = "amqp ssl configuration"
    )
    private String amqpSslTruststoreType = "JKS";

    @FieldContext(
        category = CATEGORY_AMQP_SSL,
        doc = "amqp ssl configuration"
    )
    private String amqpSslTruststoreLocation;

    @FieldContext(
        category = CATEGORY_AMQP_SSL,
        doc = "amqp ssl configuration"
    )
    private String amqpSslTruststorePassword;

    @FieldContext(
        category = CATEGORY_AMQP_SSL,
        doc = "amqp ssl configuration"
    )
    private String amqpSslKeymanagerAlgorithm = "SunX509";

    @FieldContext(
        category = CATEGORY_AMQP_SSL,
        doc = "amqp ssl configuration"
    )
    private String amqpSslTrustmanagerAlgorithm = "SunX509";

    @FieldContext(
        category = CATEGORY_AMQP_SSL,
        doc = "amqp TLS peer or not"
    )
    private boolean amqpSslClientAuth = false;

    @FieldContext(
        category = CATEGORY_AMQP,
        doc = "Mechanisms supported"
    )
    private String amqpAllowedMechanisms = "PLAIN token";

    @FieldContext(
        category = CATEGORY_AMQP,
        doc = "Mechanism used"
    )
    private String amqpMechanism = "token";

    @FieldContext(
        category = CATEGORY_AMQP,
        doc = "Mechanism used"
    )
    private boolean amqpIpFilter = false;


    @FieldContext(
        category = CATEGORY_AMQP,
        doc = "AMQP Replication enable"
    )
    private boolean amqpReplicationEnabled = false;

    @FieldContext(
        category = CATEGORY_AMQP,
        doc = "AMQP Replication Host ip"
    )
    private String amqpReplicationHost;

    @FieldContext(
        category = CATEGORY_AMQP,
        doc = "AMQP Replication Port"
    )
    private Integer amqpReplicationPort = 6672;

    @FieldContext(
        category = CATEGORY_AMQP,
        doc = "AMQP Replication Admin Port"
    )
    private Integer amqpReplicationAdminPort = 15673;

    @FieldContext(
        category = CATEGORY_AMQP,
        doc = "AMQP Replication Username"
    )
    private String amqpReplicationUsername;

    @FieldContext(
        category = CATEGORY_AMQP,
        doc = "AMQP Replication Password"
    )
    private String amqpReplicationPassword;

    @FieldContext(
        category = CATEGORY_AMQP,
        doc = "AMQP Replication Password"
    )
    private Integer amqpReplicationOffsetPeriod = 120;
}

