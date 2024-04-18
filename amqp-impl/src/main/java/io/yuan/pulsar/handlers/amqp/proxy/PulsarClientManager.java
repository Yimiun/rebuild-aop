package io.yuan.pulsar.handlers.amqp.proxy;

import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class PulsarClientManager {

    private final PulsarService pulsarService;

    private final Map<String, AmqpPulsarClient> clientMap = new ConcurrentHashMap<>();

    private final MetadataService metadataService;

    private PulsarClientManager(PulsarService pulsarService, AmqpServiceConfiguration config,
                                    MetadataService metadataService) {
        this.pulsarService = pulsarService;
        this.metadataService = metadataService;
        metadataService.registerListener(this::handleBrokerNotification);
    }

    private void handleBrokerNotification(Notification notice) {
        if (notice.getPath().startsWith(LoadManager.LOADBALANCE_BROKERS_ROOT)) {
            String path = notice.getPath();
            if (notice.getType().equals(NotificationType.Created))
                metadataService.getMetadata(LocalBrokerData.class, notice.getPath(), true)
                    .thenAccept(localBrokerDataOps -> {
                        if (localBrokerDataOps.isEmpty()) {
                            log.error("Created node:{} a few milliseconds ago, but disappeared! check the cluster stats",
                                path.substring(LoadManager.LOADBALANCE_BROKERS_ROOT.length()));
                            return;
                        }
                        String pulsarServiceUrl = localBrokerDataOps.get().getPulsarServiceUrl();
                        if (StringUtils.isNotBlank(pulsarServiceUrl)) {
                            clientMap.putIfAbsent(path, new AmqpPulsarClient());
                        }
                    });
            else if (notice.getType().equals(NotificationType.Deleted)) {
                AmqpPulsarClient removeFuture = clientMap.remove(path);
                if (removeFuture != null) {
                    removeFuture.close();
                }
            }
        }
    }

//    private PulsarClientImpl getClient(AmqpServiceConfiguration proxyConfig) {
//        ClientConfigurationData conf = new ClientConfigurationData();
//        conf.setServiceUrl(proxyConfig.isTlsEnabled()
//            ? pulsarService.getBrokerServiceUrlTls() : pulsarService.getBrokerServiceUrl());
//        conf.setTlsAllowInsecureConnection(proxyConfig.isTlsAllowInsecureConnection());
//        conf.setTlsTrustCertsFilePath(proxyConfig.getTlsCertificateFilePath());
//
//        if (proxyConfig.isBrokerClientTlsEnabled()) {
//            if (proxyConfig.isBrokerClientTlsEnabledWithKeyStore()) {
//                conf.setUseKeyStoreTls(true);
//                conf.setTlsTrustStoreType(proxyConfig.getBrokerClientTlsTrustStoreType());
//                conf.setTlsTrustStorePath(proxyConfig.getBrokerClientTlsTrustStore());
//                conf.setTlsTrustStorePassword(proxyConfig.getBrokerClientTlsTrustStorePassword());
//            } else {
//                conf.setTlsTrustCertsFilePath(
//                    StringUtils.isNotBlank(proxyConfig.getBrokerClientTrustCertsFilePath())
//                        ? proxyConfig.getBrokerClientTrustCertsFilePath()
//                        : proxyConfig.getTlsCertificateFilePath());
//            }
//        }
//
//        try {
//            if (StringUtils.isNotBlank(proxyConfig.getBrokerClientAuthenticationPlugin())) {
//                conf.setAuthPluginClassName(proxyConfig.getBrokerClientAuthenticationPlugin());
//                conf.setAuthParams(proxyConfig.getBrokerClientAuthenticationParameters());
//                conf.setAuthParamMap(null);
//                conf.setAuthentication(AuthenticationFactory.create(
//                    proxyConfig.getBrokerClientAuthenticationPlugin(),
//                    proxyConfig.getBrokerClientAuthenticationParameters()));
//            }
//            return new PulsarClientImpl(conf);
//        } catch (PulsarClientException e) {
//            log.error("Failed to create PulsarClient", e);
//            throw new IllegalArgumentException(e);
//        }
//    }
}
