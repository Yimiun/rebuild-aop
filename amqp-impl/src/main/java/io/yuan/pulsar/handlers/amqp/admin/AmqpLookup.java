//package io.yuan.pulsar.handlers.amqp.admin;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.pulsar.common.naming.TopicName;
//
//import javax.ws.rs.*;
//import javax.ws.rs.container.AsyncResponse;
//import javax.ws.rs.container.Suspended;
//import javax.ws.rs.core.MediaType;
//
//@Slf4j
//@Path("/lookup")
//@Produces(MediaType.APPLICATION_JSON)
//public class AmqpLookup extends BaseResource {
//
//    @POST
//    @Path("/{domain}/{tenant}/{vhost}/{queue}")
//    @Consumes(MediaType.APPLICATION_JSON)
//    public void lookup(@Suspended final AsyncResponse response,
//                       @PathParam("domain") String domain,
//                       @PathParam("tenant") String tenant,
//                       @PathParam("vhost") String vhost,
//                       @PathParam("queue") String queue,
//                       JsonNode jsonNode) {
//        getAmqpLookupHandler().findBroker(TopicName.get(domain, tenant, vhost, queue));
//    }
//}
