package io.yuan.pulsar.handlers.amqp.amqp.binding;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@JsonDeserialize(as = ExchangeData.class)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ExchangeData {
    @JsonProperty
    private String name;
    @JsonProperty
    private String type;
    @JsonProperty
    private String vhost;//namespace
    @JsonProperty
    private boolean internal;
    @JsonProperty
    private boolean durable;
    @JsonProperty
    private boolean autoDelete;
    @JsonProperty
    private BindData bindData;
    @JsonProperty
    private Map<String, Object> arguments;
}
