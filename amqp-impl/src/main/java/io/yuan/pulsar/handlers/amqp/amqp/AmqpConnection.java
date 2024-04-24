package io.yuan.pulsar.handlers.amqp.amqp;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.yuan.pulsar.handlers.amqp.broker.AmqpBrokerService;
import io.yuan.pulsar.handlers.amqp.configuration.AmqpServiceConfiguration;
import io.yuan.pulsar.handlers.amqp.encode.AmqpBrokerDecoder;
import io.yuan.pulsar.handlers.amqp.encode.AmqpCommandDecoder;
import io.yuan.pulsar.handlers.amqp.utils.DateFormatUtils;
import io.yuan.pulsar.handlers.amqp.utils.FutureExceptionUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQDecoder;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.*;

import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.US_ASCII;

@Slf4j
public class AmqpConnection extends AmqpCommandDecoder implements ServerMethodProcessor<ServerChannelMethodProcessor> {

    enum ConnectionState {
        INIT,
        AWAIT_START_OK,
        AWAIT_SECURE_OK,
        AWAIT_TUNE_OK,
        AWAIT_OPEN,
        OPEN
    }

    private ChannelHandlerContext ctx;

    private SocketAddress remoteAddress;

    private String clientAddress;

    private final ProtocolVersion protocolVersion = ProtocolVersion.v0_91;

    private String mechanism = "PLAIN";

    private final AmqpBrokerService amqpBrokerService;

    private AmqpBrokerDecoder brokerDecoder;

    @Getter
    private final MethodRegistry methodRegistry;

    private Long connectTime;

    @Getter
    private NamespaceName namespaceName;

    private volatile ConnectionState state = ConnectionState.INIT;

    private final ConcurrentLongHashMap<AmqpChannel> channels;

    private final ConcurrentLongLongHashMap closingChannelsList;

    private final Object channelAddRemoveLock = new Object();

    private final AtomicBoolean alreadyClosed;

    private volatile int currentClassId;

    private volatile int currentMethodId;

    private volatile int frameSize;

    private volatile int channelSize;

    private final AmqpServiceConfiguration config;

    private String defaultTenant;

    public AmqpConnection(AmqpServiceConfiguration amqpConfig, AmqpBrokerService amqpBrokerService) {
        super(amqpBrokerService.getPulsarService(), amqpConfig);
        this.amqpBrokerService = amqpBrokerService;
        this.methodRegistry = new MethodRegistry(ProtocolVersion.v0_91);
        this.channels = new ConcurrentLongHashMap<>();
        this.closingChannelsList = new ConcurrentLongLongHashMap();
        this.alreadyClosed = new AtomicBoolean(false);
        this.config = amqpConfig;
        this.mechanism = amqpConfig.getAmqpMechanism();
    }

    @VisibleForTesting
    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        if (remoteAddress instanceof InetSocketAddress) {
            this.clientAddress = ((InetSocketAddress) remoteAddress).getHostString();
        } else {
            log.error("From an exception client which does not use SOCKET");
            close();
        }
        this.connectTime = System.currentTimeMillis();
        this.brokerDecoder = new AmqpBrokerDecoder(this);
        this.defaultTenant = config.getAmqpTenant();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        completeAndCloseAllChannels();
        amqpBrokerService.getAmqpConnectionService().removeConnection(namespaceName, this);
        if (this.brokerDecoder != null) {
            this.brokerDecoder.close();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        try {
            brokerDecoder.decodeBuffer(QpidByteBuffer.wrap(buf.nioBuffer()));
        } catch (Exception e) {
            close();
        } finally {
            buf.release();
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Throwable c = FutureExceptionUtils.decodeFuture(cause);
        log.error("[{}] Got exception: {}", remoteAddress, c);
        close();
    }

    @Override
    public void receiveConnectionStartOk(FieldTable fieldTable, AMQShortString mechanism, byte[] response,
                                         AMQShortString locale) {
        if (log.isDebugEnabled()) {
            log.debug("RECV ConnectionStartOk[clientProperties: {}, mechanism: {}, locale: {}]",
                fieldTable, mechanism, locale);
        }
        assertState(ConnectionState.AWAIT_START_OK);
        ConnectionTuneBody tuneBody =
            methodRegistry.createConnectionTuneBody(config.getAmqpMaxNoOfChannels(),
                config.getAmqpMaxFrameSize(),
                config.getAmqpHeartBeat());

        writeFrame(tuneBody.generateFrame(0));
        state = ConnectionState.AWAIT_TUNE_OK;
    }

    @Override
    public void receiveConnectionSecureOk(byte[] bytes) {
        assertState(ConnectionState.AWAIT_SECURE_OK);
        ConnectionTuneBody tuneBody =
            methodRegistry.createConnectionTuneBody(config.getAmqpMaxNoOfChannels(),
                config.getAmqpMaxFrameSize(),
                config.getAmqpHeartBeat());
        writeFrame(tuneBody.generateFrame(0));
        state = ConnectionState.AWAIT_TUNE_OK;
    }

    @Override
    public void receiveConnectionTuneOk(int channelMax, long frameMax, int heartbeat) {
        if (log.isDebugEnabled()) {
            log.debug("RECV ConnectionTuneOk[ channelMax: {} frameMax: {} heartbeat: {} ]",
                channelMax, frameMax, heartbeat);
        }
        assertState(ConnectionState.AWAIT_TUNE_OK);

        if (heartbeat > 0) {
            long writerIdle = 1000L * heartbeat;
            long readerIdle = 1000L * 2 * heartbeat;
            this.ctx.pipeline().addFirst("idleStateHandler", new IdleStateHandler(readerIdle, writerIdle, 0,
                TimeUnit.MILLISECONDS));
            this.ctx.pipeline().addLast("connectionIdleHandler", new ConnectionIdleHandler());
        }

        if (frameMax > (long) config.getAmqpMaxFrameSize()) {
            sendConnectionClose(ErrorCodes.SYNTAX_ERROR,
                "Attempt to set max frame size to " + frameMax
                    + " greater than the broker will allow: "
                    + config.getAmqpMaxFrameSize(), 0);
        } else if (frameMax > 0 && frameMax < AMQDecoder.FRAME_MIN_SIZE) {
            sendConnectionClose(ErrorCodes.SYNTAX_ERROR,
                "Attempt to set max frame size to " + frameMax
                    + " which is smaller than the specification defined minimum: "
                    + AMQFrame.getFrameOverhead(), 0);
        } else {
            int frameSize = frameMax == 0 ? config.getAmqpMaxFrameSize() : (int) frameMax;
            this.frameSize = frameSize;
            brokerDecoder.setMaxFrameSize(frameSize);

            this.channelSize = ((channelMax == 0) || (channelMax > 0xFFFF))
                ? 0xFFFF
                : channelMax;
        }
        state = ConnectionState.AWAIT_OPEN;
    }

    @Override
    public void receiveConnectionOpen(AMQShortString virtualHost, AMQShortString capabilities, boolean insist) {
        if (log.isDebugEnabled()) {
            log.debug("RECV ConnectionOpen[virtualHost: {} capabilities: {} insist: {} ]",
                virtualHost, capabilities, insist);
        }
        assertState(ConnectionState.AWAIT_OPEN);

        String vhostName = AMQShortString.toString(virtualHost);
        try {
            NamespaceName ns = NamespaceName.get(config.getAmqpTenant(), vhostName);
            amqpBrokerService.getPulsarService().getPulsarResources().getNamespaceResources()
                .getPoliciesAsync(ns)
                .thenAccept(policies -> {
                    if (policies.isEmpty()) {
                        sendConnectionClose(ErrorCodes.NOT_FOUND, String.format(
                            "The virtualHost [%s] not found, create it first",
                            vhostName), 0);
                        return;
                    }
                    this.namespaceName = ns;
                });
        } catch (RuntimeException e) {
            log.error("Namespace :{} not found", vhostName, e);
            sendConnectionClose(ErrorCodes.NOT_ALLOWED, String.format(
                "The virtualHost [%s] configuration is incorrect. For example: namespace",
                vhostName), 0);
            return;
        }

        AMQMethodBody responseBody = methodRegistry.createConnectionOpenOkBody(virtualHost);
        writeFrame(responseBody.generateFrame(0));
        state = ConnectionState.OPEN;
        amqpBrokerService.getAmqpConnectionService().addConnection(namespaceName, this);
    }

    @Override
    public void receiveChannelOpen(int channelId) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[" + channelId + "] ChannelOpen");
        }
        assertState(ConnectionState.OPEN);

        if (this.namespaceName == null) {
            sendConnectionClose(ErrorCodes.COMMAND_INVALID,
                "Virtualhost has not yet been set. ConnectionOpen has not been called.", channelId);
        } else if (channels.get(channelId) != null || channelAwaitingClosure(channelId)) {
            sendConnectionClose(ErrorCodes.CHANNEL_ERROR, "Channel " + channelId + " already exists", channelId);
        } else if (channelId > channelSize) {
            sendConnectionClose(ErrorCodes.CHANNEL_ERROR,
                "Channel " + channelId + " cannot be created as the max allowed channel id is "
                    + channelSize,
                channelId);
        } else {
            log.debug("Connecting to: {}", namespaceName.getLocalName());
            final AmqpChannel channel = new AmqpChannel(channelId, this, amqpBrokerService);
            channels.put(channelId, channel);
            ChannelOpenOkBody response = methodRegistry.createChannelOpenOkBody();
            writeFrame(response.generateFrame(channelId));
        }
    }

    private boolean channelAwaitingClosure(int channelId) {
        return alreadyClosed.get() || (!closingChannelsList.isEmpty() && closingChannelsList.containsKey(channelId));
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return null;
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
        assertState(ConnectionState.OPEN);
        ServerChannelMethodProcessor channelMethodProcessor = getChannel(channelId);
        if (channelMethodProcessor == null) {
            channelMethodProcessor =
                (ServerChannelMethodProcessor) Proxy.newProxyInstance(ServerMethodDispatcher.class.getClassLoader(),
                    new Class[] {ServerChannelMethodProcessor.class}, (proxy, method, args) -> {
                        if (method.getName().equals("receiveChannelCloseOk") && channelAwaitingClosure(channelId)) {
                            closeChannelOk(channelId);
                        } else if (method.getName().startsWith("receive")) {
                            sendConnectionClose(ErrorCodes.CHANNEL_ERROR,
                                "Unknown channel id: " + channelId, channelId);
                        } else if (method.getName().equals("ignoreAllButCloseOk")) {
                            return channelAwaitingClosure(channelId);
                        }
                        return null;
                    });
        }
        return channelMethodProcessor;
    }

    @Override
    public void receiveConnectionClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        try {
            if (alreadyClosed.compareAndSet(false, true)) {
                completeAndCloseAllChannels();
            }
            MethodRegistry methodRegistry = this.methodRegistry;
            ConnectionCloseOkBody responseBody = methodRegistry.createConnectionCloseOkBody();
            writeFrame(responseBody.generateFrame(0));
        } catch (Exception e) {
            log.error("Error closing connection for " + this.remoteAddress.toString(), e);
        } finally {
            close();
        }
    }

    @Override
    public void receiveConnectionCloseOk() {
        if (log.isDebugEnabled()) {
            log.debug("RECV ConnectionCloseOk");
        }
        close();
    }

    @Override
    public void receiveHeartbeat() {

    }

    @Override
    public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
        if (log.isDebugEnabled()) {
            log.debug("RECV Protocol Header [{}]", protocolInitiation);
        }
        brokerDecoder.setExpectProtocolInitiation(false);

        try {
            ProtocolVersion pv = protocolInitiation.checkVersion(); // Fails if not correct
            // TODO serverProperties
            Map<String, Object> serverProperties = new HashMap<>();
            serverProperties.put("cluster_name", amqpBrokerService.getPulsarService().getConfig().getClusterName());
            serverProperties.put("owner", "yuan");
            serverProperties.put("version", "0.0.1");
            serverProperties.put("platform", "Linux");
            AMQMethodBody responseBody = this.methodRegistry.createConnectionStartBody(
                (short) protocolVersion.getMajorVersion(),
                (short) pv.getActualMinorVersion(),
                FieldTable.convertToFieldTable(serverProperties)/*null*/,
                // TODO temporary modification
                mechanism.getBytes(US_ASCII),
                "en_US".getBytes(US_ASCII));
            writeFrame(responseBody.generateFrame(0));
            state = ConnectionState.AWAIT_START_OK;
        } catch (Exception e) {
            log.error("Received unsupported protocol initiation for protocol version: {} ", getProtocolVersion(), e);
            writeFrame(new ProtocolInitiation(ProtocolVersion.v0_91));
            throw new RuntimeException(e);
        }
    }

    public AmqpChannel getChannel(int channelId) {
        final AmqpChannel channel = channels.get(channelId);
        if ((channel == null) || alreadyClosed.get() ||
                channel.getClosing().get() || closingChannelsList.containsKey(channelId)) {
            return null;
        } else {
            return channel;
        }
    }

    public void closeChannel(AmqpChannel amqpChannel) {
        closeChannel(amqpChannel, false);
        writeFrame(new AMQFrame(amqpChannel.getChannelId(), methodRegistry.createChannelCloseOkBody()));
    }

    public void closeChannelAndWriteFrame(AmqpChannel channel, int cause, String message) {
        writeFrame(new AMQFrame(channel.getChannelId(),
            methodRegistry.createChannelCloseBody(cause,
                AMQShortString.validValueOf(message),
                currentClassId,
                currentMethodId)));
        closeChannel(channel, true);
    }

    public void closeChannelOk(int channelId) {
        closingChannelsList.remove(channelId);
    }

    void closeChannel(AmqpChannel channel, boolean mark) {
        int channelId = channel.getChannelId();
        try {
            channel.close();
            if (mark) {
                markChannelAwaitingCloseOk(channelId);
            }
        } finally {
            channels.remove(channelId);
        }
    }

    @Override
    public void setCurrentMethod(int classId, int methodId) {
        this.currentClassId = classId;
        this.currentMethodId = methodId;
    }

    void assertState(final ConnectionState requiredState) {
        if (state != requiredState) {
            String replyText = "Command Invalid, expected " + requiredState + " but was " + state;
            sendConnectionClose(ErrorCodes.COMMAND_INVALID, replyText, 0);
            throw new RuntimeException(replyText);
        }
    }


    public void sendConnectionClose(int errorCode, String message, int channelId) {
        if (alreadyClosed.compareAndSet(false, true)) {
            try {
                markChannelAwaitingCloseOk(channelId);
                completeAndCloseAllChannels();

            } finally {
                writeFrame(new AMQFrame(0, new ConnectionCloseBody(getProtocolVersion(),
                    errorCode, AMQShortString.validValueOf(message), currentClassId, currentMethodId)));
            }
        }
    }

    private void markChannelAwaitingCloseOk(int channelId) {
        closingChannelsList.put(channelId, System.currentTimeMillis());
    }

    // for transaction stats change in qpid
    private void completeAndCloseAllChannels() {
        try {
            receivedCompleteAllChannels();
        } finally {
            closeAllChannels();
        }
    }

    private void receivedCompleteAllChannels() {
        RuntimeException exception = null;

        for (AmqpChannel channel : channels.values()) {
            try {
                channel.receivedComplete();
            } catch (RuntimeException exceptionForThisChannel) {
                if (exception == null) {
                    exception = exceptionForThisChannel;
                }
                log.error("error informing channel that receiving is complete. Channel: " + channel,
                    exceptionForThisChannel);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    private void closeAllChannels() {
        RuntimeException exception = null;
        try {
            for (AmqpChannel channel : channels.values()) {
                try {
                    channel.close();
                } catch (RuntimeException exceptionForThisChannel) {
                    if (exception == null) {
                        exception = exceptionForThisChannel;
                    }
                    log.error("error informing channel that receiving is complete. Channel: " + channel,
                        exceptionForThisChannel);
                }
            }
            if (exception != null) {
                throw exception;
            }
        } finally {
            synchronized (channelAddRemoveLock) {
                channels.clear();
            }
        }
    }

    @Override
    protected void close() {
        log.info("[{}] Client {} closed, connect time {}",
            clientAddress, ctx.channel(), DateFormatUtils.formatDate(DateFormatUtils.PATTERN_DEFAULT_ON_SECOND, connectTime));
        this.ctx.close();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AmqpConnection another = (AmqpConnection) obj;
        if (another.namespaceName == null || another.remoteAddress == null) {
            return false;
        }
        return another.remoteAddress.equals(this.remoteAddress) && another.namespaceName.equals(this.namespaceName);
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    public void writeFrame(AMQDataBlock frame) {
        if (log.isDebugEnabled()) {
            log.debug("send: " + frame);
        }
        getCtx().writeAndFlush(frame);
    }

    class ConnectionIdleHandler extends ChannelDuplexHandler {

        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.READER_IDLE)) {
                    log.error("heartbeat timeout close remoteSocketAddress [{}]",
                        AmqpConnection.this.remoteAddress.toString());
                    AmqpConnection.this.close();
                } else if (event.state().equals(IdleState.WRITER_IDLE)) {
                    log.warn("heartbeat write  idle [{}]", AmqpConnection.this.remoteAddress.toString());
                    writeFrame(HeartbeatBody.FRAME);
                }
            }

            super.userEventTriggered(ctx, evt);
        }

    }
}
