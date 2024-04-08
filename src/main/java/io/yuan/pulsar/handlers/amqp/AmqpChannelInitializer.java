package io.yuan.pulsar.handlers.amqp;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.yuan.pulsar.handlers.amqp.amqp.AmqpConnection;
import io.yuan.pulsar.handlers.amqp.broker.AmqpBrokerService;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;

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
        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(
            amqpConfig.getAmqpExplicitFlushAfterFlushes(), true));
//            ch.pipeline().addLast("frameEncoder", new AmqpEncoder());
        ch.pipeline().addLast("handler", new AmqpConnection(amqpConfig, amqpBrokerService));
    }
}
