package io.yuan.pulsar.handlers.amqp.amqp.component.queue;

import io.yuan.pulsar.handlers.amqp.amqp.component.Router;
import io.yuan.pulsar.handlers.amqp.amqp.component.consumer.AmqpConsumer;
import io.yuan.pulsar.handlers.amqp.amqp.component.message.AmqpMessage;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.QueueData;
import org.apache.pulsar.broker.service.Topic;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface Queue extends Router {

    static final String EXCLUSIVE_PROPERTY = "exclusive_connection";

    QueueData getQueueData();

    String getName();

    boolean isDurable();

    Set<BindData> getBindData();

    Map<String, Object> getArguments();

    boolean isAutoDelete();

    boolean isInternal();

    boolean isExclusive();

    int getMaxPriority();

    int getConsumerCount();

    int getMaxLength();

    boolean isOverFlow();

    List<AmqpConsumer> getConsumers();

    long clearQueue();

    Topic getTopic();

    CompletableFuture<Void> addBindData(BindData bindData);

    CompletableFuture<Void> removeBindData(BindData bindData);

    void addMessage(AmqpMessage message);

    AmqpMessage readMessage();

    String getVhost();

    String getTenant();

    long getCapacity();

    void ack();
}
