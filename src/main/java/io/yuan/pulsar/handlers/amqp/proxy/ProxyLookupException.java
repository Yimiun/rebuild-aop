package io.yuan.pulsar.handlers.amqp.proxy;

public class ProxyLookupException extends Exception{

    public ProxyLookupException() {
        super();
    }

    public ProxyLookupException(String msg) {
        super(msg);
    }
}
