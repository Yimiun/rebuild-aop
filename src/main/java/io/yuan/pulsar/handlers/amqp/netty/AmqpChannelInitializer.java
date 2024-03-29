package io.yuan.pulsar.handlers.amqp.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.yuan.pulsar.handlers.amqp.amqp.AmqpConnection;
import io.yuan.pulsar.handlers.amqp.broker.AmqpBrokerService;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
//import io.streamnative.pulsar.handlers.amqp.encode.AmqpEncoder;
import io.yuan.pulsar.handlers.amqp.utils.ipUtils.IpFilter;

import java.io.IOException;

public class AmqpChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final AmqpServiceConfiguration amqpConfig;
    private final AmqpBrokerService amqpBrokerService;

    public AmqpChannelInitializer(AmqpServiceConfiguration amqpConfig, AmqpBrokerService amqpBrokerService) {
        super();
        this.amqpConfig = amqpConfig;
        this.amqpBrokerService = amqpBrokerService;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (IpFilter.isMatched(ch.remoteAddress().getAddress().toString())) {
            ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(
                amqpConfig.getAmqpExplicitFlushAfterFlushes(), true));
//            ch.pipeline().addLast("frameEncoder", new AmqpEncoder());
            ch.pipeline().addLast("handler", new AmqpConnection(amqpConfig, amqpBrokerService));
        } else {
            throw new IOException("Illegal ip address, which in black ip list or not in white ip list");
        }
    }
}
