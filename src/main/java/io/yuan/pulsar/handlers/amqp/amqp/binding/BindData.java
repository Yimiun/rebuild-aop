package io.yuan.pulsar.handlers.amqp.amqp.binding;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;
import java.util.Set;

@JsonDeserialize(as = BindData.class)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class BindData {
    @JsonProperty
    private String vhost;
    @JsonProperty
    private String fromName;
    @JsonProperty
    private String toName;
    @JsonProperty
    private Set<String> routingKeys;
    @JsonProperty
    private String propertiesKey;
    @JsonProperty
    private Map<String, Object> arguments;
}
