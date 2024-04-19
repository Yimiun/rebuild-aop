package io.yuan.pulsar.handlers.amqp.amqp.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@JsonDeserialize(as = QueueData.class)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class QueueData {

    @JsonProperty
    private String name;
    @JsonProperty
    private String tenant;//namespace
    @JsonProperty
    private String vhost;//namespace
    @JsonProperty
    private boolean internal;
    @JsonProperty
    private boolean durable;
    @JsonProperty
    private boolean autoDelete;
    @JsonProperty
    private boolean exclusive;
    @JsonProperty
    private List<BindData> bindsData;
    @JsonProperty
    private Map<String, String> arguments;
}
