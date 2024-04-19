package io.yuan.pulsar.handlers.amqp.amqp.service;

import io.yuan.pulsar.handlers.amqp.amqp.component.queue.PersistentQueue;
import io.yuan.pulsar.handlers.amqp.amqp.component.queue.Queue;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.QueueData;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import io.yuan.pulsar.handlers.amqp.proxy.ProxyLookupException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class QueueServiceImpl implements QueueService {

    public static final String PERSISTENT_DOMAIN = TopicDomain.persistent.value();
    public static final String NON_PERSISTENT_DOMAIN = TopicDomain.non_persistent.value();

    private final MetadataService metadataService;

    private final AmqpServiceConfiguration config;

    private final PulsarService pulsarService;

    private final Object lock = new Object();

    private final Map<TopicName, CompletableFuture<Optional<Queue>>> queueMap = new ConcurrentHashMap<>();

    private final Map<TopicName, CompletableFuture<Void>> exclusiveMap = new ConcurrentHashMap<>();

    private final Map<TopicName, CompletableFuture<Void>> autoDeleteMap = new ConcurrentHashMap<>();

    public QueueServiceImpl(MetadataService metadataService,
                            AmqpServiceConfiguration config,
                            PulsarService pulsarService) {
        this.metadataService = metadataService;
        this.config = config;
        this.pulsarService = pulsarService;
    }

    @Override
    public CompletableFuture<Optional<Queue>> getQueue(String queueName, String tenantName, String namespaceName) {
        return getQueue();
    }

    public CompletableFuture<Optional<Queue>> getQueue(String queueName, String tenantName, String namespaceName,
                                                       boolean query) {
        TopicName tpName = TopicName.get(PERSISTENT_DOMAIN, tenantName, namespaceName, queueName);
        if (queueMap.containsKey(tpName)) {
            CompletableFuture<Optional<Queue>> res = queueMap.get(tpName);
            return res == null ? CompletableFuture.completedFuture(Optional.empty()) : res;
        }
        synchronized (lock) {
            return pulsarService.getNamespaceService().getBrokerServiceUrlAsync(tpName,
                LookupOptions.builder().authoritative(true).build())
                .thenCompose(lookupResult -> {
                   if (lookupResult.isEmpty()) {
                       return FutureUtil.failedFuture(new ProxyLookupException("No look up result"));
                   }
                   return pulsarService.getBrokerService().getTopic(tpName, false, null);
                })
                .thenCompose(topicOps -> {
                    if (topicOps.isEmpty()) {
                        return FutureUtil.failedFuture(new BrokerServiceException.TopicNotFoundException("No topic found"));
                    }
                    PersistentTopic tp = (PersistentTopic) topicOps.get();
                    String managedPath = tp.getManagedLedger().getName();
                    return metadataService.getMetadata(QueueData.class, managedPath, false)
                        .thenCompose(queueDataOps -> {
                            if (queueDataOps.isEmpty()) {
                                return FutureUtil.failedFuture(new NotFoundException("Metadata is empty"));
                            }
                            // write in method witch call this method
//                            if (queueDataOps.get().isExclusive()) {
//                                Map<String, String> properties = queueDataOps.get().getArguments();
//                                assert properties != null;
//                                assert properties.get(Queue.EXCLUSIVE_PROPERTY) != null;
//
//                                return FutureUtil.failedFuture(new QueueException.ExclusiveQueueException(
//                                    "The queue can't exist here, may be broker shutdown exceptionally before, so delete it"
//                                ));
//                            }
                            CompletableFuture<Optional<Queue>> queueFuture = CompletableFuture.completedFuture(
                                Optional.of(recoveryFromMetadata(tp, queueDataOps.get())));
                            if (!query) {
                                queueMap.put(tpName, queueFuture);
                            }
                            return queueFuture;
                        });
                });
        }
    }


    @Override
    public CompletableFuture<Optional<Queue>> createQueue(String queueName, String tenantName, String namespaceName,
                                                          boolean durable, boolean autoDelete, boolean internal,
                                                          boolean exclusive, Map<String, String> arguments,
                                                          List<BindData> bindData, int maxSize, int maxPriority) {
        CompletableFuture<Optional<Queue>> queueFuture = new CompletableFuture<>();
        final TopicName topicName = TopicName.get(PERSISTENT_DOMAIN, tenantName, namespaceName, queueName);
        pulsarService.getNamespaceService().getBrokerServiceUrlAsync(topicName,
                LookupOptions.builder().authoritative(true).build())
            .thenCompose(lookupResult -> {
                if (lookupResult.isEmpty()) {
                    queueFuture.completeExceptionally(new BrokerServiceException("No look up result"));
                    return null;
                }
                return pulsarService.getBrokerService().getTopic(topicName, true, arguments);
            })
            .thenCompose(topicOps -> {
                CompletableFuture<Optional<Queue>> res = new CompletableFuture<>();

                return res;
            });
        return queueFuture;
    }

    private PersistentQueue recoveryFromMetadata(PersistentTopic topic, QueueData queueData) {
        return new PersistentQueue(
            topic,
            queueData.getName(),
            NamespaceName.get(queueData.getTenant(), queueData.getVhost()),
            true,
            queueData.isAutoDelete(),
            queueData.isInternal(),
            queueData.isExclusive(),
            queueData.getBindsData(),
            queueData.getArguments()
        );
    }

    @Override
    public CompletableFuture<Void> updateBindings(List<BindData> newData) {
        return null;
    }

    @Override
    public CompletableFuture<Void> removeQueue(String queueName, String tenantName, String namespaceName) {
        return null;
    }
}
