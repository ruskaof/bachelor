package ru.itmo.rusinov.consensus.common;


import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleEnvironmentClient implements EnvironmentClient {
    private final HttpClient client;
    private final Map<String, String> destinations;
    private final long requestTimeoutMillis;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public SimpleEnvironmentClient(Map<String, String> destinations, long requestTimeoutMillis) {
        this.destinations = destinations;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(25))
                .build();
    }

    @Override
    public void initialize() {
        // No initialization needed for HttpClient
    }

    @Override
    public CompletableFuture<byte[]> sendMessage(byte[] message, String serverId) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + destinations.get(serverId) + "/request"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(message))
                .timeout(Duration.ofMillis(requestTimeoutMillis))
                .build();

        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        Thread.sleep(25);
                        return client.send(request, HttpResponse.BodyHandlers.ofByteArray()).body();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                executor
        );
    }

    @Override
    public void close() {
        // No explicit cleanup needed for HttpClient
    }
}