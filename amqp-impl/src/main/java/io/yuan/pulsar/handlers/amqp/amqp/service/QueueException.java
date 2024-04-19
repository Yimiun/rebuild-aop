package io.yuan.pulsar.handlers.amqp.amqp.service;

public class QueueException extends Exception {

    public QueueException() {
        super();
    }

    public QueueException(String msg) {
        super(msg);
    }

    static class ExclusiveQueueException extends QueueException{

        public ExclusiveQueueException() {
            super();
        }

        public ExclusiveQueueException(String msg) {
            super(msg);
        }
    }
}
