package io.yuan.pulsar.handlers.amqp.amqp.binding;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@JsonDeserialize(as = ExchangeData.class)
@Getter
@Setter
@AllArgsConstructor
public class ExchangeData {

    private String name;

    private String type;

    private String vhost;

    private boolean internal;

    private boolean durable;

    private boolean autoDelete;

    private BindData bindData;

    private Map<String, Object> arguments;
}
