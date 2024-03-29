package io.streamnative.pulsar.handlers.amqp.amqp;

import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.amqp.broker.AmqpBrokerService;
import io.streamnative.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.streamnative.pulsar.handlers.amqp.encode.AmqpBrokerDecoder;
import io.streamnative.pulsar.handlers.amqp.encode.AmqpCommandDecoder;
import org.apache.pulsar.broker.PulsarService;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;

import java.util.concurrent.atomic.AtomicBoolean;

public class AmqpConnection extends AmqpCommandDecoder implements ServerMethodProcessor<ServerChannelMethodProcessor> {

    public AmqpConnection(AmqpServiceConfiguration amqpConfig, AmqpBrokerService amqpBrokerService) {
        super(amqpBrokerService.getPulsarService(), amqpConfig);
    }

    @Override
    public AtomicBoolean getIsActive() {
        return super.getIsActive();
    }

    protected AmqpConnection(PulsarService pulsarService, AmqpServiceConfiguration amqpConfig) {
        super(pulsarService, amqpConfig);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
    }

    @Override
    public AmqpBrokerDecoder getBrokerDecoder() {
        return super.getBrokerDecoder();
    }

    @Override
    public PulsarService getPulsarService() {
        return super.getPulsarService();
    }

    @Override
    public ChannelHandlerContext getCtx() {
        return super.getCtx();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
    }

    @Override
    protected void close() {

    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void receiveConnectionStartOk(FieldTable fieldTable, AMQShortString amqShortString, byte[] bytes, AMQShortString amqShortString1) {

    }

    @Override
    public void receiveConnectionSecureOk(byte[] bytes) {

    }

    @Override
    public void receiveConnectionTuneOk(int i, long l, int i1) {

    }

    @Override
    public void receiveConnectionOpen(AMQShortString amqShortString, AMQShortString amqShortString1, boolean b) {

    }

    @Override
    public void receiveChannelOpen(int i) {

    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return null;
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(int i) {
        return null;
    }

    @Override
    public void receiveConnectionClose(int i, AMQShortString amqShortString, int i1, int i2) {

    }

    @Override
    public void receiveConnectionCloseOk() {

    }

    @Override
    public void receiveHeartbeat() {

    }

    @Override
    public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {

    }

    @Override
    public void setCurrentMethod(int i, int i1) {

    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

}
