package io.yuan.pulsar.handlers.amqp.amqp.service;

public class NotFoundException extends Exception {

    public NotFoundException() {
        super();
    }

    public NotFoundException(String msg) {
        super(msg);
    }

    public static class ExchangeNotFoundException extends NotFoundException {

        public ExchangeNotFoundException() {
            super();
        }

        public ExchangeNotFoundException(String msg) {
            super(msg);
        }
    }

    public static class QueueNotFoundException extends NotFoundException {

        public QueueNotFoundException() {
            super();
        }

        public QueueNotFoundException(String msg) {
            super(msg);
        }
    }
}
