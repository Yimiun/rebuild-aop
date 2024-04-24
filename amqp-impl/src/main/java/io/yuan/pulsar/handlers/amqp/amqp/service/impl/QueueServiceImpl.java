package io.yuan.pulsar.handlers.amqp.amqp.service.impl;

import io.yuan.pulsar.handlers.amqp.amqp.component.queue.PersistentQueue;
import io.yuan.pulsar.handlers.amqp.amqp.component.queue.Queue;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.QueueData;
import io.yuan.pulsar.handlers.amqp.amqp.service.QueueService;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import io.yuan.pulsar.handlers.amqp.proxy.ProxyLookupException;
import io.yuan.pulsar.handlers.amqp.utils.FutureExceptionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class QueueServiceImpl implements QueueService {

    public static final String PERSISTENT_DOMAIN = TopicDomain.persistent.value();
    public static final String NON_PERSISTENT_DOMAIN = TopicDomain.non_persistent.value();

    private final MetadataService metadataService;

    private final AmqpServiceConfiguration config;

    private final PulsarService pulsarService;

    private final Map<TopicName, CompletableFuture<Optional<Queue>>> queueMap = new ConcurrentHashMap<>();

    private final Map<TopicName, CompletableFuture<Void>> exclusiveMap = new ConcurrentHashMap<>();

    private final Map<TopicName, CompletableFuture<Void>> autoDeleteMap = new ConcurrentHashMap<>();

    public QueueServiceImpl(MetadataService metadataService,
                            AmqpServiceConfiguration config,
                            PulsarService pulsarService) {
        this.metadataService = metadataService;
        this.config = config;
        this.pulsarService = pulsarService;
        metadataService.registerListener(this::handleNotification);
    }

    @Override
    public CompletableFuture<Optional<Queue>> getQueue(String queueName, String tenantName, String namespaceName) {
        TopicName tpName = TopicName.get(PERSISTENT_DOMAIN, tenantName, namespaceName, queueName);
        if (queueMap.containsKey(tpName)) {
            CompletableFuture<Optional<Queue>> res = queueMap.get(tpName);
            return res == null ? CompletableFuture.completedFuture(Optional.empty()) : res;
        }
        ResourceLockServiceImpl.acquireResourceLock(tpName.getLookupName());
        if (queueMap.containsKey(tpName)) {
            ResourceLockServiceImpl.releaseResourceLock(tpName.getLookupName());
            return queueMap.get(tpName);
        }
        return pulsarService.getNamespaceService().getBrokerServiceUrlAsync(tpName, LookupOptions.builder().authoritative(true).build())
            .thenCompose(lookupResult -> {
                if (lookupResult.isEmpty()) {
                    return FutureUtil.failedFuture(new ProxyLookupException("No look up result"));
                }
                return pulsarService.getBrokerService().getTopic(tpName.toString(), false, null);
            })
            .thenCompose(topicOps -> {
                if (topicOps.isEmpty()) {
                    log.error("Queue:{} topic is empty, delete and create it again", queueName);
                    return FutureUtil.failedFuture(new BrokerServiceException.TopicNotFoundException("No topic found"));
                }
                PersistentTopic tp = (PersistentTopic) topicOps.get();
                String managedPath = getManagedLedgerPath(tp.getManagedLedger().getName());
                return metadataService.getMetadata(QueueData.class, managedPath, false)
                    .thenCompose(queueDataOps -> {
                        if (queueDataOps.isEmpty()) {
                            log.error("Queue:{} metadata is empty, delete and create it again", queueName);
                            return CompletableFuture.completedFuture(Optional.empty());
                        }
                        CompletableFuture<Optional<Queue>> queueFuture = CompletableFuture.completedFuture(
                            Optional.of(recoveryFromMetadata(tp, queueDataOps.get())));
                        queueMap.put(tpName, queueFuture);
                        return queueFuture;
                    });
            })
            .whenComplete((__, ex) -> {
                ResourceLockServiceImpl.releaseResourceLock(tpName.getLookupName());
            });
    }


    @Override
    public CompletableFuture<Optional<Queue>> createQueue(String name, String tenantName, String namespaceName,
                                                          boolean durable, boolean autoDelete, boolean internal,
                                                          boolean exclusive, Map<String, String> arguments,
                                                          int maxSize, int maxPriority) {
        return getQueue(name, tenantName, namespaceName)
            .thenCompose(queueOps -> {
                if (queueOps.isPresent()) {
                    return CompletableFuture.completedFuture(queueOps);
                }

                if (StringUtils.isBlank(name) || StringUtils.isBlank(tenantName) || StringUtils.isBlank(namespaceName)) {
                    log.error("empty arguments when declaring the exchange: {}", name);
                    return FutureUtil.failedFuture(new NullPointerException("empty arguments when declaring the exchange"));
                }
                final TopicName topicName = TopicName.get(PERSISTENT_DOMAIN, tenantName, namespaceName, name);
                return pulsarService.getNamespaceService().getBrokerServiceUrlAsync(topicName,
                        LookupOptions.builder().authoritative(true).build())
                    .thenCompose(lookupResult -> {
                        if (lookupResult.isEmpty()) {
                            log.error("No topic :{} look-up result, redo look-up", topicName);
                            return FutureUtil.failedFuture(new BrokerServiceException("No look up result"));
                        }
                        return pulsarService.getBrokerService().getTopic(topicName, true, null);
                    })
                    .thenCompose(topicOps -> {
                        if (topicOps.isEmpty()) {
                            return FutureUtil.failedFuture(new PulsarServerException.NotFoundException("Queue not found"));
                        }
                        PersistentTopic topic = (PersistentTopic) topicOps.get();
                        QueueData queueData = generateQueueData(name, tenantName, namespaceName, internal,
                                durable, autoDelete, exclusive, new HashSet<>(), arguments);
                        return metadataService.createMetadata(QueueData.class, queueData,
                                    getManagedLedgerPath(topic.getManagedLedger().getName()), false)
                            .exceptionally(ex -> {
                                Throwable e = FutureExceptionUtils.decodeFuture(ex);
                                if (e instanceof MetadataStoreException.AlreadyExistsException) {
                                    log.warn("Create Queue:{} with data:{} failed, another creation request" +
                                        " accepted by another node has already been created", name, queueData);
                                    return null;
                                } else {
                                    throw (RuntimeException) e;
                                }
                            })
                            .thenCompose(__ -> {
                                return getQueue(name, tenantName, namespaceName);
                            });
                    });
            });
    }

    private void handleNotification(Notification notification) {

    }

    private PersistentQueue recoveryFromMetadata(PersistentTopic topic, QueueData queueData) {
        return new PersistentQueue(topic, queueData);
    }

    private QueueData generateQueueData(String name, String tenant, String vhost, boolean internal, boolean durable,
                                        boolean autoDelete, boolean exclusive, Set<BindData> bindData,
                                        Map<String, String> arguments) {
        QueueData queueData = new QueueData();
        queueData.setName(name);
        queueData.setTenant(tenant);
        queueData.setVhost(vhost);
        queueData.setInternal(internal);
        queueData.setDurable(durable);
        queueData.setAutoDelete(autoDelete);
        queueData.setExclusive(exclusive);
        queueData.setBindsData(bindData);
        queueData.setArguments(arguments);
        return queueData;
    }

    @Override
    public CompletableFuture<Void> bind(Queue queue, BindData newData) {
        PersistentTopic topic = (PersistentTopic) queue.getTopic();
        String path = getManagedLedgerPath(topic.getManagedLedger().getName());
        return queue.addBindData(newData).thenCompose(__ -> {
            return metadataService.modifyUpdateMetadata(QueueData.class, path, data -> {
                return queue.getQueueData();
            });
        });
    }

    @Override
    public CompletableFuture<Void> unbind(Queue queue, BindData newData) {
        PersistentTopic topic = (PersistentTopic) queue.getTopic();
        String path = getManagedLedgerPath(topic.getManagedLedger().getName());
        return queue.removeBindData(newData).thenCompose(__ -> {
            return metadataService.modifyUpdateMetadata(QueueData.class, path, data -> {
                return queue.getQueueData();
            });
        });
    }

    @Override
    public CompletableFuture<Void> removeQueue(String queueName, String tenantName, String namespaceName) {
        return null;
    }

    public String getManagedLedgerPath(String ledgerName) {
        return "/managed-ledgers/" + ledgerName;
    }
}
