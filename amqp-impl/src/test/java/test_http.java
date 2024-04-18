import com.fasterxml.jackson.core.JsonProcessingException;
import io.yuan.pulsar.handlers.amqp.utils.JsonUtil;
import okhttp3.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class test_http {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        OkHttpClient client = new OkHttpClient();
        RequestBody requestBody = null;
        try {
            Map<String,Object> map = new HashMap<>();
            map.put("queue1", 145435431310L);
            requestBody = RequestBody.create(JsonUtil.toString(map), MediaType.get("application/json; charset=utf-8"));
        } catch (JsonProcessingException e) {
            System.out.println(e);
        }
        Map<String, String> head = new HashMap<>();
        head.put("tenant", "amqp-data");
        Request request = new Request.Builder()
            .url("http://172.20.140.182:15673/api/queues/vhost07/cursors/reset")
            .headers(Headers.of(head))
            .post(requestBody)
            .build();

        CompletableFuture<Void> future = new CompletableFuture<>();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                if (!response.isSuccessful()) {
                    future.completeExceptionally(new IOException("Unexpected code " + response));
                    return;
                }
                future.complete(null);
            }
        });
        System.out.println(future.get());
    }
}
