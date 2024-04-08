//package io.yuan.pulsar.handlers.amqp.proxy.lookup;
//
//import io.yuan.pulsar.handlers.amqp.amqp.service.TopicService;
//import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
//import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
//import io.yuan.pulsar.handlers.amqp.AmqpProtocolHandler;
//import io.yuan.pulsar.handlers.amqp.utils.ConfigurationUtils;
//import lombok.Builder;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.pulsar.broker.PulsarServerException;
//import org.apache.pulsar.broker.PulsarService;
//import org.apache.pulsar.broker.loadbalance.LoadManager;
//import org.apache.pulsar.broker.service.BrokerServiceException;
//import org.apache.pulsar.client.api.AuthenticationFactory;
//import org.apache.pulsar.client.api.PulsarClient;
//import org.apache.pulsar.client.api.PulsarClientException;
//import org.apache.pulsar.client.impl.Backoff;
//import org.apache.pulsar.client.impl.BackoffBuilder;
//import org.apache.pulsar.client.impl.PulsarClientImpl;
//import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
//import org.apache.pulsar.client.util.ExecutorProvider;
//import org.apache.pulsar.common.naming.TopicName;
//import org.apache.pulsar.common.util.FutureUtil;
//import org.apache.pulsar.metadata.api.MetadataCache;
//import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
//
//import javax.ws.rs.NotFoundException;
//import java.net.InetSocketAddress;
//import java.util.*;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.ScheduledThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.stream.Collectors;
//
//import static org.apache.commons.lang3.StringUtils.isNotBlank;
//
//@Slf4j
//public class AmqpLookupHandlerImpl implements AmqpLookupHandler {
//
//    private final String protocolHandlerName = "amqp";
//
//    private final PulsarService pulsarService;
//
//    private final AmqpServiceConfiguration config;
//
//    private final MetadataCache<LocalBrokerData> localBrokerDataCache;
//
//    private final TopicService topicService;
//
//    private final MetadataService metadataService;
//
//    private final PulsarClientImpl pulsarClient;
//
//    private final ScheduledThreadPoolExecutor executorService;
//
//    public AmqpLookupHandlerImpl(PulsarService pulsarService, AmqpServiceConfiguration config,
//                                 MetadataService metadataService, TopicService topicService) {
//        this.pulsarService = pulsarService;
//        this.pulsarClient = getClient(config);
//        this.config = config;
//        this.metadataService = metadataService;
//        this.localBrokerDataCache = metadataService.createOrGetMetadataCache(LocalBrokerData.class);
//        this.executorService = new ScheduledThreadPoolExecutor(1);
//        this.topicService = topicService;
//    }
//
//    @Override
//    // copy from mqtt :>
//    public CompletableFuture<InetSocketAddress> findBroker(TopicName topicName) {
//        CompletableFuture<InetSocketAddress> completableFuture = new CompletableFuture<>();
//        AtomicLong opTimeoutMs = new AtomicLong(config.getLookupOperationTimeoutMs());
//        Backoff backoff = new BackoffBuilder()
//            .setInitialTime(100, TimeUnit.MILLISECONDS)
//            .setMandatoryStop(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
//            .setMax(config.getMaxLookupIntervalMs(), TimeUnit.MILLISECONDS)
//            .create();
//        findBroker(topicName, backoff, opTimeoutMs, completableFuture);
//        return completableFuture;
//    }
//
//    @Override
//    public CompletableFuture<Map<TopicName, InetSocketAddress>> findBroker(List<TopicName> topicName) {
//        return null;
//    }
//
//    private void findBroker(TopicName topicName,
//                            Backoff backoff,
//                            AtomicLong remainingTime,
//                            CompletableFuture<InetSocketAddress> future) {
//        pulsarClient.getLookup().getBroker(topicName)
//            .thenCompose(lookupPair -> {
//                return localBrokerDataCache.getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT)
//                    .thenCompose(brokers -> {
//                        List<CompletableFuture<Optional<LocalBrokerData>>> brokerDataFutures =
//                            brokers.stream()
//                                .map(brokerPath -> String.format("%s/%s",
//                                    LoadManager.LOADBALANCE_BROKERS_ROOT, brokerPath))
//                                .map(localBrokerDataCache::get).collect(Collectors.toUnmodifiableList());
//                        return FutureUtil.waitForAll(brokerDataFutures)
//                            .thenCompose(__ -> {
//                                Optional<LocalBrokerData> specificBrokerData =
//                                    brokerDataFutures.stream().map(CompletableFuture::join)
//                                        .filter(brokerData -> brokerData.isPresent()
//                                            && isLookupAMQPBroker(lookupPair, brokerData.get()))
//                                        .map(Optional::get)
//                                        .findAny();
//                                if (specificBrokerData.isEmpty()) {
//                                    return FutureUtil.failedFuture(new BrokerServiceException(
//                                        "The broker does not enabled the amqp protocol handler."));
//                                }
//
//                                Optional<String> protocol = specificBrokerData.get()
//                                    .getProtocol(protocolHandlerName);
//
//                                assert protocol.isPresent();
//                                String amqpBrokerUrls = protocol.get();
//
//                                String[] brokerUrls = amqpBrokerUrls.split(ConfigurationUtils.LISTENER_DEL);
//                                // Get url random
//                                Optional<String> brokerUrl = Arrays.stream(brokerUrls)
//                                    .filter(url -> url.startsWith(ConfigurationUtils.PLAINTEXT_PREFIX))
//                                    .findAny();
//                                if (brokerUrl.isEmpty()) {
//                                    return FutureUtil.failedFuture(new BrokerServiceException(
//                                        "The broker does not enabled the amqp protocol handler."));
//                                }
//                                String[] splits = brokerUrl.get().split(ConfigurationUtils.COLON);
//                                String port = splits[splits.length - 1];
//                                int amqpBrokerPort = Integer.parseInt(port);
//                                return CompletableFuture.completedFuture(new InetSocketAddress(lookupPair
//                                    .getLeft().getHostName(), amqpBrokerPort));
//                            });
//                    });
//            })
//            .thenAccept(future::complete)
//            .exceptionally(e -> {
//                long nextDelay = Math.min(backoff.next(), remainingTime.get());
//                boolean isLookupThrottling = !PulsarClientException.isRetriableError(e.getCause())
//                    || e.getCause() instanceof PulsarClientException.TooManyRequestsException
//                    || e.getCause() instanceof PulsarClientException.AuthenticationException;
//                if (nextDelay <= 0 || isLookupThrottling) {
//                    future.completeExceptionally(e);
//                    return null;
//                }
//
//                executorService.schedule(() -> {
//                    log.warn("[topic: {}] Could not get topic lookup result -- Will try again in {} ms",
//                        topicName, nextDelay);
//                    remainingTime.addAndGet(-nextDelay);
//                    findBroker(topicName, backoff, remainingTime, future);
//                }, nextDelay, TimeUnit.MILLISECONDS);
//                return null;
//            });
//    }
//
//    // same as use pulsar look-up
////    public CompletableFuture<InetSocketAddress> findBroker(TopicName topicName, List<InetSocketAddress> hasVisitedBrokers) {
////        CompletableFuture<InetSocketAddress> lookupResult = new CompletableFuture<>();
////        pulsarService.getNamespaceService().checkTopicExists(topicName)
////            .thenAccept(exist -> {
////                if (!exist) {
////                    lookupResult.completeExceptionally(new NotFoundException("No topic found named" + topicName.getLocalName()));
////                    return;
////                }
////                if (topicService.ownedTopic(topicName)) {
////                    lookupResult.complete(null);
////                    return;
////                }
////                localBrokerDataCache.getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT)
////                    .thenCompose(brokers -> {
////                        List<CompletableFuture<Optional<LocalBrokerData>>> brokerDataFutures =
////                            brokers.stream()
////                                .map(brokerPath -> String.format("%s/%s",
////                                    LoadManager.LOADBALANCE_BROKERS_ROOT, brokerPath))
////                                .map(localBrokerDataCache::get).collect(Collectors.toUnmodifiableList());
////                        return FutureUtil.waitForAll(brokerDataFutures)
////                            .thenCompose(brokersData -> {
////                                List<InetSocketAddress> amqpHttpBrokers =
////                                    brokerDataFutures.stream().map(CompletableFuture::join)
////                                        .filter(brokerData -> brokerData.isPresent()
////                                            && brokerData.get().getProtocol(AmqpProtocolHandler.PROTOCOL_NAME).isPresent()
////                                            && !brokerData.get().getPulsarServiceUrl().contains(pulsarService.getAdvertisedAddress()))
////                                        .map(Optional::get)
////                                        .map(data -> new InetSocketAddress(data.getPulsarServiceUrl(), config.getAmqpAdminPort()))
////                                        .filter(leftBroker -> !hasVisitedBrokers.contains(leftBroker))
////                                        .collect(Collectors.toUnmodifiableList());
////
////                            })
////                    });
////            });
////        return lookupResult;
////    }
//
//    @Override
//    public void close() {
//
//    }
//
//    private boolean isLookupAMQPBroker(Pair<InetSocketAddress, InetSocketAddress> pair,
//                                       LocalBrokerData localBrokerData) {
//
//        String plain = String.format("pulsar://%s:%s", pair.getLeft().getHostName(), pair.getLeft().getPort());
//        String ssl = String.format("pulsar+ssl://%s:%s", pair.getLeft().getHostName(), pair.getLeft().getPort());
//        return localBrokerData.getProtocol(protocolHandlerName).isPresent()
//            && (plain.equals(localBrokerData.getPulsarServiceUrl())
//            || ssl.equals(localBrokerData.getPulsarServiceUrlTls()));
//    }
//
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
//                    isNotBlank(proxyConfig.getBrokerClientTrustCertsFilePath())
//                        ? proxyConfig.getBrokerClientTrustCertsFilePath()
//                        : proxyConfig.getTlsCertificateFilePath());
//            }
//        }
//
//        try {
//            if (isNotBlank(proxyConfig.getBrokerClientAuthenticationPlugin())) {
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
//}
