package io.yuan.pulsar.handlers.amqp.proxy;

import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Slf4j
public class BundleListener implements NamespaceBundleOwnershipListener {

    private final NamespaceService namespaceService;


    private final List<TopicOwnershipListener> topicOwnershipListeners = new CopyOnWriteArrayList<>();

    public BundleListener(NamespaceService namespaceService) {
        this.namespaceService = namespaceService;
    }

    public void addTopicOwnershipListener(final TopicOwnershipListener listener) {
        topicOwnershipListeners.add(listener);
    }

    @Override
    public void onLoad(NamespaceBundle bundle) {
        log.info("Broker load bundle: {}, progress topics immigration", bundle);
        getOwnedTopicList(bundle)
            .thenAccept(topics -> {
                topicOwnershipListeners.forEach(listener -> {
                    log.info("Handling topics :{} immigration", topics);
                    topics.forEach(topic -> {
                        listener.whenLoad(TopicName.get(topic));
                    });
                });
            });
    }

    @Override
    public void unLoad(NamespaceBundle bundle) {
        // 应当包含使queueName->channel关联的map数据失效
    }

    @Override
    public boolean test(NamespaceBundle namespaceBundle) {
        return true;
    }

    private CompletableFuture<List<String>> getOwnedTopicList(final NamespaceBundle bundle) {
        final NamespaceName namespaceName = bundle.getNamespaceObject();
        return namespaceService.getListOfTopics(namespaceName, CommandGetTopicsOfNamespace.Mode.ALL)
            .thenApply(topics -> topics.stream().
                        filter(topic -> bundle.includes(TopicName.get(topic)))
                        .collect(Collectors.toList()));
    }
}
