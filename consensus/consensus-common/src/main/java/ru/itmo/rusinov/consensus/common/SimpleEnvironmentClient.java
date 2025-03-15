package ru.itmo.rusinov.consensus.common;


import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class SimpleEnvironmentClient implements EnvironmentClient {
    private final HttpClient client;
    private final Map<String, String> destinations;

    public SimpleEnvironmentClient(Map<String, String> destinations) {
        this.destinations = destinations;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(100))
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
                .build();

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(HttpResponse::body);
    }

    @Override
    public void close() {
        // No explicit cleanup needed for HttpClient
    }
}