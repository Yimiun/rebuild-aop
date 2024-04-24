package io.yuan.pulsar.handlers.amqp.amqp.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;
import java.util.Set;

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
    private String tenant;
    @JsonProperty
    private String vhost;//namespace
    @JsonProperty
    private boolean internal;
    @JsonProperty
    private boolean durable;
    @JsonProperty("auto_delete")
    private boolean autoDelete;
    @JsonProperty("bindings")
    private Set<BindData> bindsData;
    @JsonProperty
    private Map<String, Object> arguments;

}
