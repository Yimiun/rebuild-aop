package io.yuan.pulsar.handlers.amqp.amqp.component.queue;

import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import io.yuan.pulsar.handlers.amqp.amqp.pojo.QueueData;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.naming.NamespaceName;

import java.util.Map;
import java.util.Set;

public class PersistentQueue extends AbstractQueue {

    public PersistentQueue(Topic topic, QueueData queueData) {
        super(topic, queueData);
    }
}
