package io.yuan.pulsar.handlers.amqp.amqp;

import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeService;
import io.yuan.pulsar.handlers.amqp.broker.AmqpBrokerService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;

import javax.validation.constraints.NotNull;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.qpid.server.protocol.ErrorCodes.INTERNAL_ERROR;

@Slf4j
public class AmqpChannel implements ServerChannelMethodProcessor {

    @Getter
    private final int channelId;

    private final AmqpConnection connection;

    private final AmqpBrokerService brokerService;
    @Getter
    private final AtomicBoolean closing = new AtomicBoolean(false);

    private final ExchangeService exchangeService;

    private final NamespaceName namespaceName;

    public AmqpChannel(int channelId, AmqpConnection connection, AmqpBrokerService amqpBrokerService) {
        this.channelId = channelId;
        this.connection = connection;
        this.brokerService = amqpBrokerService;
        this.exchangeService = brokerService.getExchangeService();
        this.namespaceName = connection.getNamespaceName();
    }

    public void receivedComplete() {
    }

    public void close() {
    }

    @Override
    public void receiveAccessRequest(AMQShortString amqShortString, boolean b, boolean b1, boolean b2, boolean b3, boolean b4) {

    }

    @Override
    public void receiveExchangeDeclare(@NotNull final AMQShortString exchangeName,
                                       final AMQShortString type,
                                       final boolean passive,
                                       final boolean durable,
                                       final boolean autoDelete,
                                       final boolean internal,
                                       final boolean nowait,
                                       final FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeDeclare[ exchange: {},"
                    + " type: {}, passive: {}, durable: {}, autoDelete: {}, internal: {}, "
                    + "nowait: {}, arguments: {} ]", channelId, exchangeName,
                type, passive, durable, autoDelete, internal, nowait, arguments);
        }
        if (exchangeName == null || AMQShortString.toString(exchangeName).isEmpty()) {
            log.error("[{}][{}] ExchangeType should be set when createIfMissing is true.", namespaceName, exchangeName);
            String sb = "ExchangeName is empty";
            handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.NOT_FOUND, sb);
        }
        if (type == null || AMQShortString.toString(type).isEmpty()) {
            String sb = "ExchangeType should be set";
            handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.NOT_FOUND, sb);
        }
        String name = AMQShortString.toString(exchangeName);
        String tp = AMQShortString.toString(type);
        if (passive) {
            exchangeService.getExchange(name, namespaceName.getTenant(), namespaceName.getLocalName())
                .thenAccept(ops -> {
                    if (ops.isEmpty()) {
                        log.error("[{}][{}] ExchangeType should be set when createIfMissing is true.", namespaceName, exchangeName);
                        String sb = "There is no exchange named:" + exchangeName;
                        handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.NOT_FOUND, sb);
                    } else {
                        connection.writeFrame(
                                connection.getMethodRegistry().createExchangeDeclareOkBody().generateFrame(channelId));
                    }
                });
            return;
        }
        exchangeService.createExchange(name, namespaceName.getTenant(), namespaceName.getLocalName(), tp,
            durable, autoDelete, internal, FieldTable.convertToMap(arguments))
                .thenAccept(ops -> {
                    if (ops.isEmpty()) {
                        log.error("[{}][{}] ExchangeType should be set when createIfMissing is true.", namespaceName, exchangeName);
                        String sb = "There is no exchange named:" + exchangeName;
                        handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.NOT_FOUND, sb);
                    } else {
                        connection.writeFrame(
                            connection.getMethodRegistry().createExchangeDeclareOkBody().generateFrame(channelId));
                    }
                });
    }



    @Override
    public void receiveExchangeDelete(AMQShortString exchangeName, boolean ifUnused, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeDelete[ exchange: {}, ifUnused: {}, nowait:{} ]", channelId, exchangeName,
                ifUnused, nowait);
        }
        if (exchangeName == null || AMQShortString.toString(exchangeName).isEmpty()) {
            log.error("[{}][{}] ExchangeType should be set when createIfMissing is true.", namespaceName, exchangeName);
            String sb = "ExchangeName is empty";
            handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.NOT_FOUND, sb);
        }
        String exchange = AMQShortString.toString(exchangeName);
        exchangeService.removeExchange(exchange, namespaceName.getTenant(), namespaceName.getLocalName())
            .whenComplete((__, ex) -> {
                ExchangeDeleteOkBody responseBody = connection.getMethodRegistry().createExchangeDeleteOkBody();
                connection.writeFrame(responseBody.generateFrame(channelId));
            });
    }

    @Override
    public void receiveExchangeBound(AMQShortString amqShortString, AMQShortString amqShortString1, AMQShortString amqShortString2) {

    }

    @Override
    public void receiveQueueDeclare(AMQShortString amqShortString, boolean b, boolean b1, boolean b2, boolean b3, boolean b4, FieldTable fieldTable) {

    }

    @Override
    public void receiveQueueBind(AMQShortString amqShortString, AMQShortString amqShortString1, AMQShortString amqShortString2, boolean b, FieldTable fieldTable) {

    }

    @Override
    public void receiveQueuePurge(AMQShortString amqShortString, boolean b) {

    }

    @Override
    public void receiveQueueDelete(AMQShortString amqShortString, boolean b, boolean b1, boolean b2) {

    }

    @Override
    public void receiveQueueUnbind(AMQShortString amqShortString, AMQShortString amqShortString1, AMQShortString amqShortString2, FieldTable fieldTable) {

    }

    @Override
    public void receiveBasicRecover(boolean b, boolean b1) {

    }

    @Override
    public void receiveBasicQos(long l, int i, boolean b) {

    }

    @Override
    public void receiveBasicConsume(AMQShortString amqShortString, AMQShortString amqShortString1, boolean b, boolean b1, boolean b2, boolean b3, FieldTable fieldTable) {

    }

    @Override
    public void receiveBasicCancel(AMQShortString amqShortString, boolean b) {

    }

    @Override
    public void receiveBasicPublish(AMQShortString amqShortString, AMQShortString amqShortString1, boolean b, boolean b1) {

    }

    @Override
    public void receiveBasicGet(AMQShortString amqShortString, boolean b) {

    }

    @Override
    public void receiveChannelFlow(boolean b) {

    }

    @Override
    public void receiveChannelFlowOk(boolean b) {

    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        connection.closeChannel(this);
    }

    @Override
    public void receiveChannelCloseOk() {
        if (log.isDebugEnabled()) {
            log.debug("RECV[ {} ] ChannelCloseOk", channelId);
        }

        connection.closeChannelOk(channelId);
    }

    @Override
    public void receiveMessageContent(QpidByteBuffer qpidByteBuffer) {

    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties basicContentHeaderProperties, long l) {

    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    @Override
    public void receiveBasicNack(long l, boolean b, boolean b1) {

    }

    @Override
    public void receiveBasicAck(long l, boolean b) {

    }

    @Override
    public void receiveBasicReject(long l, boolean b) {

    }

    @Override
    public void receiveTxSelect() {

    }

    @Override
    public void receiveTxCommit() {

    }

    @Override
    public void receiveTxRollback() {

    }

    @Override
    public void receiveConfirmSelect(boolean b) {

    }

    private void closeChannel(int cause, final String message) {
        connection.closeChannelAndWriteFrame(this, cause, message);
    }

    protected void handleAoPException(ExceptionType e, int errorCodes, String msg) {
        switch (e) {
            case INTERNAL_ERROR:
                connection.sendConnectionClose(INTERNAL_ERROR, msg, channelId);
                break;
            case CLOSING_CHANNEL:
                closeChannel(errorCodes, msg);
                break;
            case CLOSING_CONNECTION:
                connection.sendConnectionClose(errorCodes, msg, channelId);
                break;
        }
    }

    enum ExceptionType {
        INTERNAL_ERROR,
        CLOSING_CHANNEL,
        CLOSING_CONNECTION
    }

}
