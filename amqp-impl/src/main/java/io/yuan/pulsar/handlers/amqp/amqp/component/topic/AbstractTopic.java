package io.yuan.pulsar.handlers.amqp.amqp.component.topic;

import org.apache.pulsar.broker.service.Topic;

public abstract class AbstractTopic {

    Topic topic;

    AbstractTopic(Topic topic) {
        this.topic = topic;
    }

}
