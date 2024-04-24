package io.yuan.pulsar.handlers.amqp.exception;

public class ServiceRuntimeException extends RuntimeException {

    public ServiceRuntimeException() {
        super();
    }

    public ServiceRuntimeException(String msg) {
        super(msg);
    }

    public static class ExchangeParameterException extends ServiceRuntimeException {

        public ExchangeParameterException() {
            super();
        }

        public ExchangeParameterException(String msg) {
            super(msg);
        }
    }

    public static class QueueParameterException extends ServiceRuntimeException {

        public QueueParameterException() {
            super();
        }

        public QueueParameterException(String msg) {
            super(msg);
        }
    }

    public static class DuplicateBindException extends ServiceRuntimeException {

        public DuplicateBindException() {
            super();
        }

        public DuplicateBindException(String msg) {
            super(msg);
        }
    }

}
