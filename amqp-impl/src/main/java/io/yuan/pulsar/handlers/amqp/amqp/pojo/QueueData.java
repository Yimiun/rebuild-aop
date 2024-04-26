package io.yuan.pulsar.handlers.amqp.amqp.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.*;

import java.util.Map;
import java.util.Set;

@JsonDeserialize(as = QueueData.class)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueueData {

    @JsonProperty
    private String name;
    @JsonProperty
    private String tenant;
    @JsonProperty
    private String vhost;//namespace
    @JsonProperty
    private boolean durable;
    @JsonProperty("auto_delete")
    private boolean autoDelete;
    @JsonProperty
    private boolean exclusive;
    @JsonProperty("bind_data")
    private Set<BindData> bindsData;
    @JsonProperty
    private Map<String, Object> arguments;
}
