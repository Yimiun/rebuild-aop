package io.yuan.pulsar.handlers.amqp.amqp.component;

import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import org.apache.pulsar.broker.service.Topic;

public abstract class AbstractTopic {

    Topic topic;

    AbstractTopic(Topic topic) {
        this.topic = topic;
    }

}
