package io.yuan.pulsar.handlers.amqp.amqp.component.queue;

import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.naming.NamespaceName;

import java.util.List;
import java.util.Map;

public class PersistentQueue extends AbstractQueue {

    public PersistentQueue(Topic topic, String name, NamespaceName namespaceName,
                           boolean durable, boolean autoDelete, boolean internal,
                           boolean exclusive, List<BindData> bindData, Map<String, String> arguments) {
        super(topic, name, namespaceName, durable, autoDelete, internal, exclusive, bindData, arguments);
    }
}
