package io.yuan.pulsar.handlers.amqp.admin.view;

import io.yuan.pulsar.handlers.amqp.amqp.pojo.BindData;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
@AllArgsConstructor
public class ExchangeView {
    private String name;
    private String type;
    private String tenant;
    private String vhost;
    private boolean durable;
    private boolean internal;
    private boolean auto_delete;
    private Set<BindData> bind_data;
    private Map<String, Object> arguments;
}
