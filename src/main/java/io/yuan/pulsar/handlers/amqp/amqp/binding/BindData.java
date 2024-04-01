package io.yuan.pulsar.handlers.amqp.amqp.binding;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Set;

@JsonDeserialize(as = BindData.class)
@Getter
@Setter
@AllArgsConstructor
public class BindData {

    private String vhost;

    private String fromName;

    private String toName;

    private Set<String> routingKeys;

    private String propertiesKey;

    private Map<String, Object> arguments;
}
