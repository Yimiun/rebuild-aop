package io.yuan.pulsar.handlers.amqp.amqp;

import io.yuan.pulsar.handlers.amqp.amqp.service.BindService;
import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeService;
import io.yuan.pulsar.handlers.amqp.amqp.service.QueueService;
import io.yuan.pulsar.handlers.amqp.broker.AmqpBrokerService;
import io.yuan.pulsar.handlers.amqp.exception.NotFoundException;
import io.yuan.pulsar.handlers.amqp.utils.FutureExceptionUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.*;

import javax.validation.constraints.NotNull;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.yuan.pulsar.handlers.amqp.amqp.service.impl.BindServiceImpl.EXCHANGE_TYPE;
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

    private final QueueService queueService;

    private final BindService bindService;

    private final NamespaceName namespaceName;

    public AmqpChannel(int channelId, AmqpConnection connection, AmqpBrokerService amqpBrokerService) {
        this.channelId = channelId;
        this.connection = connection;
        this.brokerService = amqpBrokerService;
        this.exchangeService = brokerService.getExchangeService();
        this.queueService = brokerService.getQueueService();
        this.bindService = brokerService.getBindService();
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
            exchangeService.getExchangeAsync(name, namespaceName.getTenant(), namespaceName.getLocalName())
                .whenComplete((ops, ex) -> {
                    if (ops.isEmpty() || ex != null &&
                        FutureExceptionUtils.decodeFuture(ex) instanceof NotFoundException.ExchangeNotFoundException) {
                            log.error("[{}][{}] ExchangeType should be set when createIfMissing is true.", namespaceName, exchangeName);
                            String sb = "There is no exchange named:" + exchangeName;
                            handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.NOT_FOUND, sb);
                    } else if (ex != null) {
                        log.error("[{}][{}] ExchangeType should be set when createIfMissing is true.", namespaceName, exchangeName);
                        String sb = "Error when fetching exchange metadata:" + FutureExceptionUtils.decodeFuture(ex);
                        handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.NOT_FOUND, sb);
                    } else {
                        connection.writeFrame(
                                connection.getMethodRegistry().createExchangeDeclareOkBody().generateFrame(channelId));
                    }
                });
            return;
        }
        exchangeService.createExchangeAsync(name, namespaceName.getTenant(), namespaceName.getLocalName(), tp,
            durable, autoDelete, internal, FieldTable.convertToMap(arguments))
                .whenComplete((ops, ex) -> {
                    if (ops.isEmpty()) {
                        log.error("[{}][{}] Create exchange failed, metadata empty.", namespaceName, exchangeName);
                        String sb = "Create exchange:" + exchangeName + "failed, metadata empty!";
                        handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.RESOURCE_ERROR, sb);
                    } else if (ex != null) {
                        log.error("[{}][{}] Exception when creating exchange.", namespaceName, exchangeName, ex);
                        String sb = "Exception when creating exchange:" + exchangeName + "," + ex;
                        handleAoPException(ExceptionType.CLOSING_CHANNEL, INTERNAL_ERROR, sb);
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
        exchangeService.removeExchangeAsync(exchange, namespaceName.getTenant(), namespaceName.getLocalName())
            .whenComplete((__, ex) -> {
                if (ex != null) {
                    log.error("[{}][{}] Delete exchange failed, metadata service wrong.", namespaceName, exchangeName);
                    String sb = "Delete exchange:" + exchangeName + "failed, metadata service wrong!" + ex.getMessage();
                    handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.RESOURCE_ERROR, sb);
                } else {
                    ExchangeDeleteOkBody responseBody = connection.getMethodRegistry().createExchangeDeleteOkBody();
                    connection.writeFrame(responseBody.generateFrame(channelId));
                }
            });
    }

    @Override
    public void receiveExchangeBound(AMQShortString exchange, AMQShortString routingKey, AMQShortString queue) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeBound[ exchange: {}, routingKey: {}, queue:{} ]", channelId, exchange,
                routingKey, queue);
        }
        String exchangeName = AMQShortString.toString(exchange);
        String queueName = AMQShortString.toString(queue);
        String rk = AMQShortString.toString(routingKey);
        bindService.checkExchangeBound(namespaceName.getTenant(), namespaceName.getLocalName(), exchangeName, queueName, rk)
            .whenComplete((code, ex) -> {
                if (ex != null) {
                    log.error("Failed to bound queue {} to exchange {} with routingKey {} in vhost {}",
                        queueName, exchangeName, routingKey, connection.getNamespaceName(), ex.getCause());
                    handleAoPException(ExceptionType.CLOSING_CHANNEL, INTERNAL_ERROR, ex.getCause().getMessage());
                    return;
                }
                String reply = null;
                switch (code) {
                    case ExchangeBoundOkBody.EXCHANGE_NOT_FOUND:
                        reply = "Exchange '" + exchangeName + "' not found in vhost "
                            + connection.getNamespaceName();
                        break;
                    case ExchangeBoundOkBody.QUEUE_NOT_FOUND:
                        reply = "Queue '" + queueName + "' not found in vhost "
                            + connection.getNamespaceName();
                        break;
                    case ExchangeBoundOkBody.QUEUE_NOT_BOUND:
                        reply = "Queue '" + queueName + "' has no bound in vhost "
                            + connection.getNamespaceName();
                        break;
                    case ExchangeBoundOkBody.NO_QUEUE_BOUND_WITH_RK:
                        reply = "No queue bind with this routingKey " + routingKey;
                        break;
                    case ExchangeBoundOkBody.SPECIFIC_QUEUE_NOT_BOUND_WITH_RK:
                        reply = "Queue'" + queueName + "' is not bind with this routingKey " + routingKey;
                        break;
                    case ExchangeBoundOkBody.OK:
                        break;
                }
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                ExchangeBoundOkBody exchangeBoundOkBody = methodRegistry
                    .createExchangeBoundOkBody(code, AMQShortString.validValueOf(reply));
                connection.writeFrame(exchangeBoundOkBody.generateFrame(channelId));
            });
    }

    @Override
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive,
                                    boolean autoDelete, boolean nowait, FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueDeclare[ queue: {}, passive: {}, durable:{}, "
                    + "exclusive:{}, autoDelete:{}, nowait:{}, arguments:{} ]",
                channelId, queue, passive, durable, exclusive, autoDelete, nowait, arguments);
        }
        String queueName = queue != null ? queue.toString() : "";
        queueService.createQueue(queueName, namespaceName.getTenant(), namespaceName.getLocalName(), durable,
            autoDelete, exclusive, FieldTable.convertToMap(arguments), 1000, 10)
            .whenComplete((queueOpt, ex) -> {
                if (ex != null) {
                    Throwable real = FutureExceptionUtils.decodeFuture(ex);
                    log.error("Exception when declaring queue:{}", queueName);
                    handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.NOT_FOUND,
                            "Exception when declaring queue:" + real.getMessage());
                    return;
                }
                if (queueOpt.isEmpty()) {
                    log.error("Fetch empty queue metadata :{}", queueName);
                    handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.NOT_FOUND, "Fetch empty queue metadata");
                    return;
                }
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                QueueDeclareOkBody responseBody = methodRegistry.createQueueDeclareOkBody(
                    AMQShortString.createAMQShortString(queueOpt.get().getName()), 0, 0);
                connection.writeFrame(responseBody.generateFrame(channelId));
            });
    }

    @Override
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                                 boolean nowait, FieldTable argumentsTable) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueBind[ queue: {}, exchange: {}, bindingKey:{}, nowait:{}, arguments:{} ]",
                channelId, queue, exchange, bindingKey, nowait, argumentsTable);
        }
        String exchangeName = AMQShortString.toString(exchange);
        String routingKey = bindingKey != null ? AMQShortString.toString(bindingKey) : "";
        String queueName = AMQShortString.toString(queue);
        bindService.bind(namespaceName.getTenant(), namespaceName.getLocalName(), exchangeName, queueName,
                EXCHANGE_TYPE, routingKey, FieldTable.convertToMap(argumentsTable))
            .whenComplete((__, ex) -> {
                if (ex != null) {
                    Throwable real = FutureExceptionUtils.decodeFuture(ex);
                    log.error("Failed to bind queue {} to exchange {}, msg:{}", queue, exchange, ex);
                    if (real instanceof NotFoundException.QueueNotFoundException) {
                        String msg = String.format("Queue: %s not found", queueName);
                        handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.NOT_FOUND, msg);
                        return;
                    }
                    if (real instanceof NotFoundException.ExchangeNotFoundException) {
                        String msg = String.format("Exchange: %s not found", exchangeName);
                        handleAoPException(ExceptionType.CLOSING_CHANNEL, ErrorCodes.NOT_FOUND, msg);
                        return;
                    }
                    String msg = String.format("Internal exception:%s", real.getCause());
                    handleAoPException(ExceptionType.CLOSING_CONNECTION, INTERNAL_ERROR, msg);
                    return;
                }
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
                connection.writeFrame(responseBody.generateFrame(channelId));
            });
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
