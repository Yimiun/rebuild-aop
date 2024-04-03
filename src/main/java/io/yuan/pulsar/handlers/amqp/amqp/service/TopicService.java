package io.yuan.pulsar.handlers.amqp.amqp.service;

import com.google.common.base.Splitter;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.util.FutureUtil;

import java.net.URLEncoder;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * 提供Topic相关服务，包括元数据更新，元数据获取，根据Name返回Topic等功能
 * */
@Slf4j
public class TopicService {

    public static final String PERSISTENT_DOMAIN = TopicDomain.persistent.value() + "://";
    public static final String NON_PERSISTENT_DOMAIN = TopicDomain.non_persistent.value() + "://";

    private MetadataService metadataService;

    private final PulsarService pulsarService;

    public TopicService(MetadataService metadataService, PulsarService pulsarService) {
        this.metadataService = metadataService;
        this.pulsarService = pulsarService;
    }

    public CompletableFuture<Optional<Topic>> getTopicReference(String topicName,
                                                                       String tenantName, String namespaceName,
                                                                       boolean encodeTopicName,
                                                                       boolean isPersistent) {
        return generateTopicName(topicName, tenantName, namespaceName, encodeTopicName, isPersistent)
            .thenCompose(topic -> pulsarService.getPulsarResources().getNamespaceResources()
                .getPoliciesAsync(topic.getNamespaceObject())
                .thenApply(policies -> {
                    if (policies.isEmpty()) {
                        return pulsarService.getConfig().isAllowAutoTopicCreation();
                    }
                    AutoTopicCreationOverride autoTopicCreationOverride =
                        policies.get().autoTopicCreationOverride;
                    if (autoTopicCreationOverride == null) {
                        return pulsarService.getConfig().isAllowAutoTopicCreation();
                    } else {
                        return autoTopicCreationOverride.isAllowAutoTopicCreation();
                    }
                }).thenCompose(isAllowTopicCreation ->
                    getTopicReference(topic, isAllowTopicCreation)));
    }

    public CompletableFuture<Optional<Topic>> getTopicReference(String topicName,
                                                                String tenantName,
                                                                String namespaceName,
                                                                boolean encodeTopicName,
                                                                String defaultTopicDomain,
                                                                Boolean createIfMissing) {
        return getTopicName(topicName, tenantName, namespaceName, encodeTopicName, defaultTopicDomain)
            .thenCompose(topic -> getTopicReference(topic, createIfMissing));
    }

    public CompletableFuture<Optional<Topic>> getTopicReference(TopicName topic,
                                                                       Boolean createIfMissing) {
        return pulsarService.getPulsarResources().getNamespaceResources().getPoliciesAsync(topic.getNamespaceObject())
            .thenCompose(policies -> {
                if (policies.isEmpty()) {
                    log.error("Get or create topic failed, namespace not found:{}", topic.getNamespace());
                    return FutureUtil.failedFuture(new NamespaceNotFoundException(topic.getNamespace()));
                }
                return pulsarService.getNamespaceService().getBrokerServiceUrlAsync(topic,
                        LookupOptions.builder().authoritative(false).loadTopicsInBundle(false).build())
                    .thenCompose(lookupOp -> pulsarService.getBrokerService().getTopic(topic.toString(), createIfMissing));
            });
    }

    public static CompletableFuture<TopicName> getTopicName(String topicName, String tenantName,
                                                            String namespaceName, boolean encodeTopicName,
                                                            String defaultTopicDomain) {
        try {
            return CompletableFuture.completedFuture(
                TopicName.get(
                    getPulsarTopicName(topicName, tenantName, namespaceName, encodeTopicName,
                        TopicDomain.getEnum(defaultTopicDomain))));
        } catch (Exception e) {
            return FutureUtil.failedFuture(e);
        }
    }

    public static CompletableFuture<TopicName> generateTopicName(String shortName, String tenantName,
                                                                 String namespaceName, boolean encode,
                                                                 boolean isPersistent) {
        try {
            return CompletableFuture.completedFuture(
                TopicName.get(isPersistent ? PERSISTENT_DOMAIN : NON_PERSISTENT_DOMAIN,
                    tenantName, namespaceName, shortName)
            );
        } catch (Exception e) {
            log.error("Wrong format of topic", e);
            return FutureUtil.failedFuture(e);
        }
    }

    public static String getPulsarTopicName(String amqpTopicName, String defaultTenant, String defaultNamespace,
                                            boolean urlEncoded, TopicDomain topicDomain) {
        if (amqpTopicName.startsWith(PERSISTENT_DOMAIN)
            || amqpTopicName.startsWith(NON_PERSISTENT_DOMAIN)) {
            List<String> parts = Splitter.on("://").limit(2).splitToList(amqpTopicName);
            if (parts.size() < 2) {
                throw new IllegalArgumentException("Invalid topic name: " + amqpTopicName);
            }
            String domain = parts.get(0);
            String rest = parts.get(1);
            parts = Splitter.on("/").limit(3).splitToList(rest);
            if (parts.size() < 3) {
                throw new IllegalArgumentException("Invalid topic name: " + amqpTopicName);
            }
            String tenant = parts.get(0);
            String namespace = parts.get(1);
            String localName = parts.get(2);
            return TopicName.get(domain, tenant, namespace,
                urlEncoded ? URLEncoder.encode(localName) : localName).toString();
        } else {
            return TopicName.get(topicDomain.value(), defaultTenant, defaultNamespace,
                URLEncoder.encode(amqpTopicName)).toString();
        }
    }
}
