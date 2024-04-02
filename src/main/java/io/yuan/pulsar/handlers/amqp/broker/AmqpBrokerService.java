package io.yuan.pulsar.handlers.amqp.broker;

import io.yuan.pulsar.handlers.amqp.amqp.service.TopicService;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;

public class AmqpBrokerService {

    @Getter
    public PulsarService pulsarService;
    @Getter
    AmqpServiceConfiguration amqpServiceConfiguration;
    @Getter
    TopicService topicService;

    public AmqpBrokerService(PulsarService pulsar, AmqpServiceConfiguration amqpConfig, TopicService topicService) {
        this.pulsarService = pulsar;
        this.amqpServiceConfiguration = amqpConfig;
    }
}
