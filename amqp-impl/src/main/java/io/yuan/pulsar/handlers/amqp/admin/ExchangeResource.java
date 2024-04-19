package io.yuan.pulsar.handlers.amqp.admin;


import io.yuan.pulsar.handlers.amqp.admin.view.ExchangeView;
import io.yuan.pulsar.handlers.amqp.amqp.service.ExchangeServiceImpl;
import org.apache.pulsar.common.util.RestException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

@Path("/exchange")
@Produces(MediaType.APPLICATION_JSON)
public class ExchangeResource extends BaseResource {

    @GET
    @Path("/map")
    public void getExchangeMap(@Suspended final AsyncResponse response) {
        Map<String, Object> res = new HashMap<>();
        getExchangeService().getExchangeMap().forEach((key, value) -> {
            if (value.isDone()) {
                value.join().ifPresent(ex -> res.put(key.substring(ExchangeServiceImpl.prefix.length()), ex));
            } else {
                res.put(key.substring(ExchangeServiceImpl.prefix.length()), value);
            }
        });
        response.resume(res);
    }

    @GET
    @Path("/{tenant}/{vhost}/{exchange}")
    public void getExchange(@Suspended final AsyncResponse response,
                            @PathParam("tenant") String tenant,
                            @PathParam("vhost") String vhost,
                            @PathParam("exchange") String exchange) {
        getExchangeService().getExchange(exchange, tenant, vhost)
            .thenAccept(ops -> {
                ops.ifPresentOrElse(ex -> {
                    response.resume(
                        new ExchangeView(ex.getName(),
                                ex.getType().name(),
                                tenant,
                                vhost,
                                ex.getDurable(),
                                ex.getInternal(),
                                ex.getAutoDelete(),
                                ex.getBindData(),
                                ex.getArguments()));
                }, () -> {
                    response.resume(new RestException(Response.Status.NOT_FOUND, "404 NOT FOUND"));
                });
            });
    }
}
