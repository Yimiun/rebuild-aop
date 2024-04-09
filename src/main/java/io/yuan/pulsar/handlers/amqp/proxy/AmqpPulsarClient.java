package io.yuan.pulsar.handlers.amqp.proxy;

import org.apache.pulsar.client.impl.PulsarClientImpl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * save some pulsarClient to the same broker
 *
 * provide a
 * */
public class AmqpPulsarClient {

    List<PulsarClientImpl> pulsarClients = new CopyOnWriteArrayList<>();

    public AmqpPulsarClient() {

    }

    public void close() {

    }
}
