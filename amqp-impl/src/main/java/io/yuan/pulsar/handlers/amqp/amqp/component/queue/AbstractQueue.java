package io.yuan.pulsar.handlers.amqp.amqp.component.queue;

import io.netty.buffer.ByteBuf;
import io.yuan.pulsar.handlers.amqp.amqp.component.consumer.AmqpConsumer;
import io.yuan.pulsar.handlers.amqp.amqp.component.message.AmqpMessage;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractQueue implements Queue {

    private final NamespaceName namespaceName;

    private Topic topic;

    private String name;

    private boolean durable;

    private boolean autoDelete;

    private boolean internal;

    private boolean exclusive;

    private List<BindData> bindData;

    private final Map<String, String> arguments;

    public AbstractQueue(Topic topic, String name, NamespaceName namespaceName,
                         boolean durable, boolean autoDelete,
                         boolean internal, boolean exclusive, List<BindData> bindData,
                         Map<String, String> arguments) {
        this.topic = topic;
        this.name = name;
        this.namespaceName = namespaceName;
        this.durable = durable;
        this.autoDelete = autoDelete;
        this.internal = internal;
        this.exclusive = exclusive;
        this.bindData = bindData;
        this.arguments = arguments;
    }

    @Override
    public CompletableFuture<Void> route(TopicName from, ByteBuf buf, String routingKey) {
        return null;
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
    public List<BindData> getBindData() {
        return bindData;
    }

    @Override
    public Map<String, Object> getArguments() {
        return null;
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
        return namespaceName.getLocalName();
    }

    @Override
    public String getTenant() {
        return namespaceName.getTenant();
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

    public boolean isInternal() {
        return internal;
    }

    public boolean isExclusive() {
        return exclusive;
    }
}
