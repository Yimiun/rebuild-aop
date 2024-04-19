package io.yuan.pulsar.handlers.amqp.amqp.component.queue;

import io.yuan.pulsar.handlers.amqp.amqp.component.Router;
import io.yuan.pulsar.handlers.amqp.amqp.component.consumer.AmqpConsumer;
import io.yuan.pulsar.handlers.amqp.amqp.component.message.AmqpMessage;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import org.apache.pulsar.broker.service.Topic;

import java.util.List;
import java.util.Map;

public interface Queue extends Router {

    static final String EXCLUSIVE_PROPERTY = "exclusive_connection";

    String getName();

    boolean isDurable();

    List<BindData> getBindData();

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

    void addMessage(AmqpMessage message);

    AmqpMessage readMessage();

    String getVhost();

    String getTenant();

    long getCapacity();

    void ack();
}
