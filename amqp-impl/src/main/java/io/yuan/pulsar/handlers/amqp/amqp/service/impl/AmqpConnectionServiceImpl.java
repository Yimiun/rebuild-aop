package io.yuan.pulsar.handlers.amqp.amqp.service.impl;

import io.vertx.core.impl.ConcurrentHashSet;
import io.yuan.pulsar.handlers.amqp.amqp.AmqpConnection;
import io.yuan.pulsar.handlers.amqp.metadata.MetadataService;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.qpid.server.protocol.ErrorCodes;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AmqpConnectionServiceImpl {

    private final Map<NamespaceName, Set<AmqpConnection>> connectionsMap = new HashMap<>();

    private final StatsProvider connectionStats;

    public AmqpConnectionServiceImpl(MetadataService metadataService) {
        this.connectionStats = new PrometheusMetricsProvider();
        metadataService.registerListener(this::handleNamespaceNotification);
    }

    private void handleNamespaceNotification(Notification notification) {
        String notifyPath = notification.getPath();
        if (!NamespaceResources.pathIsFromNamespace(notifyPath)) {
            return;
        }
        if (notifyPath.split("/").length == 5) {
            NamespaceName nsName = NamespaceResources.namespaceFromPath(notifyPath);
            switch (notification.getType()) {
                case Modified: {
                    break;
                }
                case Deleted: {
                    removeAndCloseConnection(nsName);
                    break;
                }
            }
        }
    }

    public void addConnection(NamespaceName namespaceName, AmqpConnection connection) {
        synchronized (connectionsMap) {
            connectionsMap.computeIfAbsent(namespaceName, key -> new ConcurrentHashSet<>()).add(connection);
        }
    }

    public void removeConnection(NamespaceName namespaceName, AmqpConnection connection) {
        synchronized (connectionsMap) {
            connectionsMap.get(namespaceName).remove(connection);
        }
    }

    public void removeAndCloseConnection(NamespaceName namespaceName) {
        if (connectionsMap.get(namespaceName) == null || connectionsMap.get(namespaceName).isEmpty()) {
            return;
        }
        synchronized (connectionsMap) {
            connectionsMap.get(namespaceName).forEach(e -> {
                e.sendConnectionClose(ErrorCodes.NOT_ALLOWED, String.format(
                    "The virtualHost [%s] has been deleted, so close the connection",
                    namespaceName), 0);
            });
            connectionsMap.remove(namespaceName);
        }
    }

}
