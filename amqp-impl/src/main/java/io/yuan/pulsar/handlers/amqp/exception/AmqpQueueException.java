package io.yuan.pulsar.handlers.amqp.exception;

public class AmqpQueueException extends RuntimeException {

    public AmqpQueueException() {
        super();
    }

    public AmqpQueueException(String msg) {
        super(msg);
    }

    public static class ExclusiveQueueException extends AmqpQueueException{

        public ExclusiveQueueException() {
            super();
        }

        public ExclusiveQueueException(String msg) {
            super(msg);
        }
    }
}
