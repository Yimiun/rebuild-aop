package io.yuan.pulsar.handlers.amqp.amqp.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Objects;

@JsonDeserialize(as = BindData.class)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BindData {

    @JsonProperty
    private String source;

    @JsonProperty
    private String tenant;

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
        return Objects.hash(destination, routingKey, arguments, source, destinationType);
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
            Objects.equals(arguments, bindData.arguments) &&
            Objects.equals(source, bindData.source) &&
            Objects.equals(destinationType, bindData.destinationType);
    }

}
