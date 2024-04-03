package io.yuan.pulsar.handlers.amqp.utils;

import java.util.concurrent.CompletionException;

public class FutureExceptionUtils {

    public static Throwable DecodeFuture(Throwable e) {
        if (e instanceof CompletionException) {
            return DecodeFuture(e.getCause());
        }
        return e;
    }
}
