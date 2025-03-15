package ru.itmo.rusinov.consensus.common;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class SimpleDistributedServer implements DistributedServer {
    private final int port;
    private HttpServer server;
    private Function<byte[], CompletableFuture<byte[]>> requestHandler;

    public SimpleDistributedServer(int port) {
        this.port = port;
    }

    @Override
    public void initialize() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/request", this::handleRequest);
            server.setExecutor(Executors.newFixedThreadPool(4));
            server.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start server", e);
        }
    }

    @Override
    public void setRequestHandler(Function<byte[], CompletableFuture<byte[]>> handler) {
        this.requestHandler = handler;
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        if (requestHandler == null) {
            exchange.sendResponseHeaders(500, -1);
            return;
        }

        byte[] requestBytes = exchange.getRequestBody().readAllBytes();
        requestHandler.apply(requestBytes).thenAccept(responseBytes -> {
            try {
                exchange.sendResponseHeaders(200, responseBytes.length);
                exchange.getResponseBody().write(responseBytes);
                exchange.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
        }
    }
}