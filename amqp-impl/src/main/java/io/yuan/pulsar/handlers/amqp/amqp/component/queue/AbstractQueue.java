package io.yuan.pulsar.handlers.amqp.amqp.component.queue;

import io.netty.buffer.ByteBuf;
import io.yuan.pulsar.handlers.amqp.amqp.component.consumer.AmqpConsumer;
import io.yuan.pulsar.handlers.amqp.amqp.component.message.AmqpMessage;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.QueueData;
import io.yuan.pulsar.handlers.amqp.exception.NotFoundException;
import io.yuan.pulsar.handlers.amqp.exception.ServiceRuntimeException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractQueue implements Queue {

    protected final QueueData queueData;

    protected final String tenant;

    protected final String namespace;

    protected Topic topic;

    protected String name;

    protected boolean durable;

    protected boolean autoDelete;

    protected boolean exclusive;

    protected Set<BindData> bindData;

    protected final ReentrantReadWriteLock bindLock = new ReentrantReadWriteLock();

    protected final Map<String, Set<BindData>> deadLetterMap = new ConcurrentHashMap<>();

    protected final Map<String, Object> arguments;

    public AbstractQueue(Topic topic, QueueData queueData) {
        this.queueData = queueData;
        this.topic = topic;
        this.name = queueData.getName();
        this.tenant = queueData.getTenant();
        this.namespace = queueData.getVhost();
        this.durable = queueData.isDurable();
        this.autoDelete = queueData.isAutoDelete();
        this.exclusive = queueData.isExclusive();
        this.bindData = queueData.getBindsData();
        this.arguments = queueData.getArguments();
    }

    @Override
    public CompletableFuture<Void> route(TopicName from, ByteBuf buf, String routingKey) {
        return null;
    }

    @Override
    public CompletableFuture<Void> addBindData(BindData bindData) {
        try {
            bindLock.writeLock().lock();
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (this.bindData.add(bindData)) {
                future.complete(null);
            } else {
                future.completeExceptionally(new ServiceRuntimeException.DuplicateBindException("Duplicate bind data"));
            }
            return future;
        }  finally {
            bindLock.writeLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Void> removeBindData(BindData bindData) {
        try {
            bindLock.writeLock().lock();
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (this.bindData.remove(bindData)) {
                future.complete(null);
            } else {
                future.completeExceptionally(new NotFoundException.BindNotFoundException("Bind data not found"));
            }
            return null;
        } finally {
            bindLock.writeLock().unlock();
        }
    }

    @Override
    public QueueData getQueueData() {
        return this.queueData;
    }

    @Override
    public int getConsumerCount() {
        return 0;
    }

    @Override
    public boolean isOverFlow() {
        return false;
    }

    @Override
    public List<AmqpConsumer> getConsumers() {
        return null;
    }

    @Override
    public long clearQueue() {
        return 0;
    }

    @Override
    public void addMessage(AmqpMessage message) {

    }

    @Override
    public AmqpMessage readMessage() {
        return null;
    }

    @Override
    public long getCapacity() {
        return 0;
    }

    @Override
    public void ack() {

    }

    @Override
    public Set<BindData> getBindData() {
        return bindData;
    }

    @Override
    public Map<String, Object> getArguments() {
        return arguments;
    }

    @Override
    public int getMaxPriority() {
        return 0;
    }

    @Override
    public int getMaxLength() {
        return 0;
    }

    @Override
    public String getVhost() {
        return this.namespace;
    }

    @Override
    public String getTenant() {
        return this.tenant;
    }

    @Override
    public Topic getTopic() {
        return topic;
    }

    @Override
    public String getName() {
        return name;
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public boolean isExclusive() {
        return exclusive;
    }
}
