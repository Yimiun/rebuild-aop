/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.pulsar.common.util.FutureUtil;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Slf4j
/**
 * HttpUtil.
 */
public class HttpUtil {

    private static final OkHttpClient client = new OkHttpClient();

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    public static CompletableFuture<Void> putAsync(String url, Map<String, Object> params) {
        return putAsync(url, params, Maps.newHashMap());
    }

    public static CompletableFuture<Void> putAsync(String url, Map<String, Object> params, Map<String, String> headers) {
        RequestBody requestBody;
        try {
            requestBody = RequestBody.create(JsonUtil.toString(params), JSON);
        } catch (JsonProcessingException e) {
            return FutureUtil/*CompletableFuture*/.failedFuture(e);
        }
        Request request = new Request.Builder()
                .url(url)
                .headers(Headers.of(headers))
                .put(requestBody)
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
        return future;
    }

    public static CompletableFuture<Void> postAsync(String url, Map<String, Object> params){
        return postAsync(url, params, Maps.newHashMap());
    }

    public static CompletableFuture<Void> postAsync(String url, Map<String, Object> params, Map<String, String> headers) {
        RequestBody requestBody;
        try {
            requestBody = RequestBody.create(JsonUtil.toString(params), JSON);
        } catch (JsonProcessingException e) {
            return FutureUtil/*CompletableFuture*/.failedFuture(e);
        }
        Request request = new Request.Builder()
                .url(url)
                .headers(Headers.of(headers))
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
        return future;
    }

    public static CompletableFuture<Void> deleteAsync(String url, Map<String, Object> params){
        return deleteAsync(url, params, Maps.newHashMap());
    }

    public static CompletableFuture<Void> deleteAsync(String url, Map<String, Object> params, Map<String, String> headers) {
        RequestBody requestBody;
        try {
            requestBody = RequestBody.create(JsonUtil.toString(params), JSON);
        } catch (JsonProcessingException e) {
            return FutureUtil/*CompletableFuture*/.failedFuture(e);
        }
        Request request = new Request.Builder()
                .url(url)
                .headers(Headers.of(headers))
                .delete(requestBody)
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
        return future;
    }

    public static CompletableFuture<Long> getAsync(String url) {
        Request request = new Request.Builder()
            .url(url)
            .get()
            .build();

        CompletableFuture<Long> future = new CompletableFuture<>();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                log.info("http get async failed, ", e);
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                if (!response.isSuccessful()) {
                    future.completeExceptionally(new IOException("Unexpected code " + response));
                    return;
                }
                String responseData = Objects.requireNonNull(response.body()).string();
                try {
                    long returnValue = Long.parseLong(responseData);
                    future.complete(returnValue);
                } catch (NumberFormatException e) {
                    future.completeExceptionally(new IOException("Invalid response data: " + responseData));
                }
            }
        });
        return future;
    }

}
