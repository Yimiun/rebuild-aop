package io.streamnative.pulsar.handlers.amqp.netty;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.amqp.broker.AmqpBrokerService;
import io.streamnative.pulsar.handlers.amqp.broker.EventManager;
import io.streamnative.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.streamnative.pulsar.handlers.amqp.configuration.ProxyConfiguration;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyService;
import io.streamnative.pulsar.handlers.amqp.utils.ConfigurationUtils;
import io.streamnative.pulsar.handlers.amqp.utils.ipUtils.IpFilter;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@Slf4j
public class AmqpProtocolHandler implements ProtocolHandler {

    public static final String PROTOCOL_NAME = "amqp";
    public static final String SSL_PREFIX = "amqp+ssl://";
    public static final String PLAINTEXT_PREFIX = "amqp://";
    public static final String LISTENER_DEL = ",";
    public static final String LISTENER_PATTEN = "^(amqp)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-0-9+]";

    @Getter
    private AmqpServiceConfiguration amqpConfig;
    @Getter
    private BrokerService brokerService;
    @Getter
    private String bindAddress;
    @Getter
    private AmqpBrokerService amqpBrokerService;
    private Server webServer;
    @Getter
    private EventManager eventManager;

    @Override
    public String protocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return PROTOCOL_NAME.equalsIgnoreCase(protocol);
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        // init config
        if (conf instanceof AmqpServiceConfiguration) {
            // in unit test, passed in conf will be AmqpServiceConfiguration
            amqpConfig = (AmqpServiceConfiguration) conf;
        } else {
            // when loaded with PulsarService as NAR, `conf` will be type of ServiceConfiguration
            amqpConfig = ConfigurationUtils.create(conf.getProperties(), AmqpServiceConfiguration.class);
        }
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(amqpConfig.getBindAddress());
        IpFilter.addDefaultIp(conf.getManagerUrl().trim().split("://")[1].split(":")[0]);
    }

    @Override
    public String getProtocolDataToAdvertise() {
        if (log.isDebugEnabled()) {
            log.debug("Get configured listeners: {}", getAppliedAmqpListeners(amqpConfig));
        }
        return getAppliedAmqpListeners(amqpConfig);
    }

    @Override
    public void start(BrokerService service) {
        brokerService = service;
        eventManager = new EventManager(brokerService.getPulsar().getLocalMetadataStore(), amqpConfig);
        eventManager.start();
        amqpBrokerService = new AmqpBrokerService(service.getPulsar(), amqpConfig);
        if (amqpConfig.isAmqpProxyEnable()) {
            ProxyConfiguration proxyConfig = new ProxyConfiguration();
            proxyConfig.setAmqpAllowedMechanisms(amqpConfig.getAmqpAllowedMechanisms());
            proxyConfig.setAmqpTenant(amqpConfig.getAmqpTenant());
            proxyConfig.setAmqpMaxNoOfChannels(amqpConfig.getAmqpMaxNoOfChannels());
            proxyConfig.setAmqpMaxFrameSize(amqpConfig.getAmqpMaxFrameSize());
            proxyConfig.setAmqpHeartBeat(amqpConfig.getAmqpHeartBeat());
            proxyConfig.setAmqpProxyPort(amqpConfig.getAmqpProxyPort());
            proxyConfig.setAmqpExplicitFlushAfterFlushes(amqpConfig.getAmqpExplicitFlushAfterFlushes());
            // TSL config convert
            proxyConfig.setAmqpTlsEnabled(amqpConfig.isAmqpTlsEnabled());
            proxyConfig.setAmqpSslProtocol(amqpConfig.getAmqpSslProtocol());
            proxyConfig.setAmqpSslKeystoreType(amqpConfig.getAmqpSslKeystoreType());
            proxyConfig.setAmqpSslKeystoreLocation(amqpConfig.getAmqpSslKeystoreLocation());
            proxyConfig.setAmqpSslKeystorePassword(amqpConfig.getAmqpSslKeystorePassword());
            proxyConfig.setAmqpSslTruststoreType(amqpConfig.getAmqpSslTruststoreType());
            proxyConfig.setAmqpSslTruststoreLocation(amqpConfig.getAmqpSslTruststoreLocation());
            proxyConfig.setAmqpSslTruststorePassword(amqpConfig.getAmqpSslTruststorePassword());
            proxyConfig.setAmqpSslKeymanagerAlgorithm(amqpConfig.getAmqpSslKeymanagerAlgorithm());
            proxyConfig.setAmqpSslTrustmanagerAlgorithm(amqpConfig.getAmqpSslTrustmanagerAlgorithm());
            proxyConfig.setAmqpSslClientAuth(amqpConfig.isAmqpSslClientAuth());

            proxyConfig.setAmqpReplicationEnabled(amqpConfig.isAmqpReplicationEnabled());
            proxyConfig.setAmqpReplicationHost(amqpConfig.getAmqpReplicationHost());
            proxyConfig.setAmqpReplicationPort(amqpConfig.getAmqpReplicationPort());
//            RabbitMqClient.resetLeader(amqpConfig.getAmqpReplicationHost());
//
            AdvertisedListener internalListener = ServiceConfigurationUtils.getInternalListener(amqpConfig, "pulsar");
            checkArgument(internalListener.getBrokerServiceUrl() != null,
                "plaintext must be configured on internal listener");
            proxyConfig.setBrokerServiceURL(internalListener.getBrokerServiceUrl().toString());

            try {
                ProxyService proxyService = new ProxyService(proxyConfig, service.getPulsar(), eventManager);
                proxyService.start();
                log.info("Start amqp proxy service at port: {}", proxyConfig.getAmqpProxyPort());
            } catch (Exception e) {
                log.error("Failed to start amqp proxy service.");
            }
        }
        startAdminResource(amqpConfig.getAmqpAdminPort());

        Configuration conf = new PropertiesConfiguration();
        conf.addProperty("cluster", amqpConfig.getClusterName());
        conf.addProperty("prometheusStatsLatencyRolloverSeconds",
            60);
    }

    private void startAdminResource(int port) {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/api");
        context.setAttribute("aop", this);

        webServer = new Server(port);
        webServer.setHandler(context);

        ServletHolder jerseyServlet = context.addServlet(ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);

        jerseyServlet.setInitParameter(
            "jersey.config.server.provider.packages", "io.streamnative.pulsar.handlers.amqp.admin");

        try {
            webServer.start();
        } catch (Exception e) {
            log.error("Failed to start web service for aop", e);
        }
    }

    // this is called after initialize, and with amqpConfig, brokerService all set.
    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkState(amqpConfig != null);
        checkState(getAppliedAmqpListeners(amqpConfig) != null);
        checkState(brokerService != null);

        String listeners = getAppliedAmqpListeners(amqpConfig);
        String[] parts = listeners.split(LISTENER_DEL);

        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                ImmutableMap.<InetSocketAddress, ChannelInitializer<SocketChannel>>builder();

            for (String listener: parts) {
                if (listener.startsWith(PLAINTEXT_PREFIX)) {
                    builder.put(
                        new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                        new AmqpChannelInitializer(amqpConfig, amqpBrokerService));
                } else {
                    log.error("Amqp listener {} not supported. supports {} and {}",
                        listener, PLAINTEXT_PREFIX, SSL_PREFIX);
                }
            }

            return builder.build();
        } catch (Exception e){
            log.error("AmqpProtocolHandler newChannelInitializers failed with", e);
            return null;
        }
    }

    @Override
    public void close() {
        try {
            webServer.stop();
            eventManager.close();
        } catch (Exception e) {
            log.error("Failed to stop web server for aop", e);
        }
    }

    public static int getListenerPort(String listener) {
        checkState(listener.matches(LISTENER_PATTEN), "listener not match patten");

        int lastIndex = listener.lastIndexOf(':');
        return Integer.parseInt(listener.substring(lastIndex + 1));
    }

    public static String getAppliedAmqpListeners(AmqpServiceConfiguration configuration) {
        String amqpListeners = configuration.getAmqpListeners();
        if (amqpListeners == null) {
            String fullyHostName = ServiceConfigurationUtils.unsafeLocalhostResolve();
            return amqpUrl(fullyHostName, 5672);
        }
        return amqpListeners;
    }

    public static String amqpUrl(String host, int port) {
        return String.format("amqp://%s:%d", host, port);
    }

    class AopVersion{

    }
}
