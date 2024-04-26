package io.yuan.pulsar.handlers.amqp.amqp.service.impl;

import io.yuan.pulsar.handlers.amqp.amqp.component.queue.PersistentQueue;
import io.yuan.pulsar.handlers.amqp.amqp.component.queue.Queue;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.QueueData;
import io.yuan.pulsar.handlers.amqp.amqp.service.BindService;
import io.yuan.pulsar.handlers.amqp.amqp.service.QueueService;
import io.yuan.pulsar.handlers.amqp.broker.AmqpBrokerService;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.yuan.pulsar.handlers.amqp.exception.AmqpQueueException;
import io.yuan.pulsar.handlers.amqp.exception.NotFoundException;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import io.yuan.pulsar.handlers.amqp.proxy.ProxyLookupException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.Notification;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class QueueServiceImpl implements QueueService {

    public static final String PERSISTENT_DOMAIN = TopicDomain.persistent.value();
    public static final String NON_PERSISTENT_DOMAIN = TopicDomain.non_persistent.value();
    public static final String QUEUE_PREFIX = "/amqp/queue/";

    private final MetadataService metadataService;

    private final AmqpServiceConfiguration config;

    private final PulsarService pulsarService;

    private final BindService bindService;

    // all the operation of it must be serialized! or will dirty-reading, use ResourceLock
    private final Map<String, CompletableFuture<Optional<Queue>>> queueMap = new ConcurrentHashMap<>();

    private final Map<TopicName, CompletableFuture<Void>> autoDeleteMap = new ConcurrentHashMap<>();

    public QueueServiceImpl(AmqpBrokerService brokerService) {
        this.metadataService = brokerService.getMetadataService();
        this.config = brokerService.getAmqpServiceConfiguration();
        this.pulsarService = brokerService.pulsarService;
        this.bindService = brokerService.getBindService();
        metadataService.registerListener(this::handleNotification);
    }

    @Override
    public CompletableFuture<Optional<Queue>> getQueue(String queueName, String tenantName, String namespaceName) {
        TopicName tpName = TopicName.get(PERSISTENT_DOMAIN, tenantName, namespaceName, queueName);
        String path = generateQueuePath(tenantName, namespaceName, queueName);
        if (queueMap.containsKey(path)) {
            CompletableFuture<Optional<Queue>> res = queueMap.get(path);
            return res == null ? CompletableFuture.completedFuture(Optional.empty()) : res;
        }
        ResourceLockServiceImpl.acquireResourceLock(path);
        if (queueMap.containsKey(path)) {
            ResourceLockServiceImpl.releaseResourceLock(path);
            return queueMap.get(path);
        }
        return pulsarService.getNamespaceService().getBrokerServiceUrlAsync(tpName, LookupOptions.builder().authoritative(true).build())
            .thenCompose(lookupResult -> {
                if (lookupResult.isEmpty()) {
                    return FutureUtil.failedFuture(new ProxyLookupException("No look up result"));
                }
                return pulsarService.getBrokerService().getTopic(tpName.toString(), false);
            })
            .thenCompose(topicOpt -> {
                if (topicOpt.isEmpty()) {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
                PersistentTopic tp = (PersistentTopic) topicOpt.get();
                return metadataService.getMetadata(QueueData.class, path, false)
                    .thenCompose(queueDataOpt -> {
                        if (queueDataOpt.isEmpty()) {
                            return CompletableFuture.completedFuture(Optional.empty());
                        }
                        boolean isExclusive = queueDataOpt.get().isExclusive();
                        if (isExclusive) {
                            log.error("Attempt to access an exclusive queue:{}", queueDataOpt.get());
                            throw new AmqpQueueException.ExclusiveQueueException();
                        }
                        CompletableFuture<Optional<Queue>> queueFuture = CompletableFuture.completedFuture(
                            Optional.of(recoveryFromMetadata(tp, queueDataOpt.get())));
                        queueMap.put(path, queueFuture);
                        return queueFuture;
                    });
            })
            .whenComplete((__, ex) -> {
                ResourceLockServiceImpl.releaseResourceLock(path);
            });
    }

    @Override
    public CompletableFuture<Optional<Queue>> createQueue(String queueName, String tenantName, String namespaceName,
                                                          boolean durable, boolean autoDelete, boolean exclusive,
                                                          Map<String, Object> arguments, int maxSize, int maxPriority) {
        String path = generateQueuePath(tenantName, namespaceName, queueName);
        return getQueue(queueName, tenantName, namespaceName)
            .thenCompose(queueOpt -> {
                if (queueOpt.isPresent()) {
                    return CompletableFuture.completedFuture(queueOpt);
                }

                if (StringUtils.isBlank(queueName) || StringUtils.isBlank(tenantName) || StringUtils.isBlank(namespaceName)) {
                    log.error("empty arguments when declaring the exchange: {}", queueName);
                    return FutureUtil.failedFuture(new NullPointerException("empty arguments when declaring the exchange"));
                }
                final TopicName topicName = TopicName.get(PERSISTENT_DOMAIN, tenantName, namespaceName, queueName);
                return pulsarService.getNamespaceService().getBrokerServiceUrlAsync(topicName,
                        LookupOptions.builder().authoritative(true).build())
                    .thenCompose(lookupResult -> {
                        if (lookupResult.isEmpty()) {
                            log.error("No topic :{} look-up result, redo look-up", topicName);
                            return FutureUtil.failedFuture(new ProxyLookupException("No look up result"));
                        }
                        return pulsarService.getBrokerService().getTopic(topicName.toString(), true)
                            .whenComplete((__, ex) -> {
                                if (ex != null) {
                                    log.error("Exception", ex);
                                }
                            });
                    })
                    .thenCompose(topicOpt -> {
                        if (topicOpt.isEmpty()) {
                            return CompletableFuture.completedFuture(Optional.empty());
                        }
                        ResourceLockServiceImpl.acquireResourceLock(path);
                        if (queueMap.containsKey(path)) {
                            ResourceLockServiceImpl.releaseResourceLock(path);
                            return queueMap.get(path);
                        }
                        PersistentTopic topic = (PersistentTopic) topicOpt.get();
                        QueueData queueData = generateQueueData(path, tenantName, namespaceName,
                                durable, autoDelete, exclusive, new HashSet<>(), arguments);
                        return metadataService.createMetadata(QueueData.class, queueData,
                                path, false)
                            .thenCompose(__ -> {
                                CompletableFuture<Optional<Queue>> queueFuture =
                                    CompletableFuture.completedFuture(Optional.of(recoveryFromMetadata(topic, queueData)));
                                queueMap.put(path, queueFuture);
                                return queueFuture;
                            })
                            .whenComplete((__, ex) -> {
                                ResourceLockServiceImpl.releaseResourceLock(path);
                            });
                    });
            });
    }

    @Override
    public CompletableFuture<Void> removeQueue(String queueName, String tenantName, String namespaceName) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        getQueue(queueName, tenantName, namespaceName)
            .whenComplete((queueOpt, ex) -> {
                if (ex != null) {
                    log.error("Failed to get topic from queue service:", ex);
                    future.completeExceptionally(ex.getCause());
                    return;
                }
                if (queueOpt.isEmpty()) {
                    log.error("Queue:{} metadata is empty, delete and create it again", queueName);
                    future.completeExceptionally(new NotFoundException.MetadataNotFoundException("Queue metadata is empty"));
                    return;
                }
                Queue queue = queueOpt.get();
                Topic topic = queue.getTopic();
                try {
                    topic.getProducers().values().forEach(topic::removeProducer);
                } catch (RuntimeException e) {
                    future.completeExceptionally(e);
                    return;
                }
                queue.getTopic().delete().whenComplete((ignore, e) -> {
                    if (e != null) {
                        future.completeExceptionally(e.getCause());
                        return;
                    }
                    bindService.unbindAllFromExchange(queue.getBindData())
                        .whenComplete((__, unbindEx) -> {
                            if (unbindEx != null) {
                                future.completeExceptionally(unbindEx.getCause());
                                return;
                            }
                            String path = generateQueuePath(tenantName, namespaceName, queueName);
                            ResourceLockServiceImpl.acquireResourceLock(path);
                            metadataService.deleteMetadata(path)
                                .whenComplete((ignore2, deleteEx) -> {
                                    ResourceLockServiceImpl.releaseResourceLock(path, true);
                                    CompletableFuture<Optional<Queue>> queueFuture = queueMap.remove(path);
                                    if (queueFuture != null && !queueFuture.isDone()) {
                                        queueFuture.cancel(true);
                                    }
                                    if (deleteEx != null) {
                                        future.completeExceptionally(deleteEx.getCause());
                                    } else {
                                        future.complete(null);
                                    }
                                });
                        });
                });
            });
        return null;
    }

    private void handleNotification(Notification notification) {
        String path = notification.getPath();
        if (path.startsWith(QUEUE_PREFIX) && path.split("/").length == 6) {
            if (!queueMap.containsKey(path)) {
                if (log.isDebugEnabled()) {
                    log.warn("An Queue request is processed globally, " +
                        "but queue {} is not initialized on this broker", path.substring(QUEUE_PREFIX.length()));
                }
                return;
            }
            switch (notification.getType()) {
                case Deleted:
                    handleRemoveQueue(path);
                    log.warn("A Delete request is processed globally, so delete the queue {} on this broker",
                        path.substring(QUEUE_PREFIX.length()));
                    break;
                case Modified:
                    break;
            }
        } else if (NamespaceResources.pathIsFromNamespace(path)) {
            if (path.split("/").length == 5) {
                NamespaceName namespaceName = NamespaceResources.namespaceFromPath(path);
                switch (notification.getType()) {
                    case Deleted:
                        handleRemoveAllQueues(namespaceName.getTenant(), namespaceName.getLocalName());
                        break;
                    case Created:
                    case Modified:
                    case ChildrenChanged:
                        break;
                }
            }
        }
    }

    private void handleRemoveQueue(String path) {
        if (log.isDebugEnabled()) {
            log.debug("Queue:{} has been removed from cache", path);
        }
        CompletableFuture<Optional<Queue>> removeFuture = queueMap.remove(path);
        if (removeFuture != null) {
            // @todo close queue
            removeFuture.thenAccept(opt -> opt.ifPresent(Queue::clearQueue));
        }
        metadataService.invalidPath(QueueData.class, path);
    }

    private void handleRemoveAllQueues(String tenantName, String namespaceName) {
        String path = generateQueuePath(tenantName, namespaceName);
        ResourceLockServiceImpl.acquireResourceLock(path);
        metadataService.deleteMetadataRecursive(path)
            .whenComplete((__, ex) -> {
                ResourceLockServiceImpl.releaseResourceLock(path, true);
                if (ex != null) {
                    log.error("Remove queue metadata wrong", ex);
                    return;
                }
                log.info("Successfully removed namespace path:{}", path);
            });
    }

    private PersistentQueue recoveryFromMetadata(PersistentTopic topic, QueueData queueData) {
        return new PersistentQueue(topic, queueData);
    }

    private QueueData generateQueueData(String name, String tenant, String vhost, boolean durable,
                                        boolean autoDelete, boolean exclusive, Set<BindData> bindData,
                                        Map<String, Object> arguments) {
        QueueData queueData = new QueueData();
        queueData.setName(name);
        queueData.setTenant(tenant);
        queueData.setVhost(vhost);
        queueData.setDurable(durable);
        queueData.setAutoDelete(autoDelete);
        queueData.setExclusive(exclusive);
        queueData.setBindsData(bindData);
        queueData.setArguments(arguments);
        return queueData;
    }

    public static String generateQueuePath(String... paras) {
        StringJoiner stringJoiner = new StringJoiner("/", QUEUE_PREFIX, "");
        for(String para : paras) {
            stringJoiner.add(para);
        }
        return stringJoiner.toString();
    }
}
