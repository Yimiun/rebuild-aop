package io.yuan.pulsar.handlers.amqp.exception;

public class AmqpExchangeException extends RuntimeException {

    public AmqpExchangeException() {
        super();
    }

    public AmqpExchangeException(String msg) {
        super(msg);
    }

    public static class ExchangeAlreadyClosedException extends AmqpExchangeException {

        public ExchangeAlreadyClosedException() {
            super();
        }

        public ExchangeAlreadyClosedException(String msg) {
            super(msg);
        }
    }
}
