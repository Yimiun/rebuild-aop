package io.yuan.pulsar.handlers.amqp.exception;

public class NotFoundException extends RuntimeException {

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

    public static class MetadataNotFoundException extends NotFoundException {

        public MetadataNotFoundException() {
            super();
        }

        public MetadataNotFoundException(String msg) {
            super(msg);
        }
    }

    public static class BindNotFoundException extends NotFoundException {

        public BindNotFoundException() {
            super();
        }

        public BindNotFoundException(String msg) {
            super(msg);
        }
    }
}
