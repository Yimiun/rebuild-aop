package io.streamnative.pulsar.handlers.amqp.broker;

import io.streamnative.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;

public class AmqpBrokerService {

    @Getter
    public PulsarService pulsarService;

    public AmqpBrokerService(PulsarService pulsar, AmqpServiceConfiguration amqpConfig) {
    }
}
