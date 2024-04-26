package io.yuan.pulsar.handlers.amqp.proxy;

public class ProxyLookupException extends RuntimeException {

    public ProxyLookupException() {
        super();
    }

    public ProxyLookupException(String msg) {
        super(msg);
    }
}
