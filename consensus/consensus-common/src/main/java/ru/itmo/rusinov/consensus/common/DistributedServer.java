package ru.itmo.rusinov.consensus.common;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface DistributedServer extends AutoCloseable {

    void initialize();
    void setRequestHandler(Function<byte[], CompletableFuture<byte[]>> handler);
}
