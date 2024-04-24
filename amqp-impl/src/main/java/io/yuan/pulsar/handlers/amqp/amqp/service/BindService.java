package io.yuan.pulsar.handlers.amqp.amqp.service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface BindService {

    CompletableFuture<Void> bind(String tenant, String namespaceName, String source, String destination, String type,
                                 String bindingKey, Map<String, Object> arguments);

    CompletableFuture<Void> unbind(String tenant, String namespaceName, String source, String destination,
                                   String bindingKey, Map<String, Object> arguments);
}
