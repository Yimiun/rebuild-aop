package io.yuan.pulsar.handlers.amqp.amqp.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;
import java.util.Objects;

@JsonDeserialize(as = BindData.class)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class BindData {

    @JsonProperty
    private String source;

    @JsonProperty
    private String vhost;

    @JsonProperty
    private String destination;

    @JsonProperty("destination_type")
    private String destinationType;

    @JsonProperty("routing_key")
    private String routingKey;

    @JsonProperty
    private Map<String, Object> arguments;

    @JsonProperty("properties_key")
    private String propertiesKey;

    @Override
    public int hashCode() {
        return Objects.hash(destination, routingKey, arguments);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BindData bindData = (BindData) obj;
        return Objects.equals(destination, bindData.destination) &&
            Objects.equals(routingKey, bindData.routingKey) &&
            Objects.equals(arguments, bindData.arguments);
    }
}
