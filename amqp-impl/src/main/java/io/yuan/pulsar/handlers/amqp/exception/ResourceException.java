package io.yuan.pulsar.handlers.amqp.exception;

import lombok.Getter;

public class ResourceException extends RuntimeException{

    @Getter
    private final int errorCode;
    @Getter
    private final boolean closeChannel;
    @Getter
    private final boolean closeConnection;

    public ResourceException(int errorCode, String msg, boolean closeChannel, boolean closeConnection) {
        super(msg);
        this.errorCode = errorCode;
        this.closeChannel = closeChannel;
        this.closeConnection = closeConnection;
    }
}
