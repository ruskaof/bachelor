package ru.itmo.rusinov.consensus.kv.store;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import ru.itmo.rusinov.consensus.common.SimpleDistributedServer;
import ru.itmo.rusinov.consensus.common.SimpleEnvironmentClient;
import ru.itmo.rusinov.consensus.kv.store.paxos.KvStorePaxosStateMachine;
import ru.itmo.rusinov.consensus.kv.store.raft.KvStoreRaftStateMachine;
import ru.itmo.rusinov.consensus.kv.store.db.MapDbKvDatabase;
import ru.itmo.rusinov.consensus.paxos.core.MapDBDurableStateStore;
import ru.itmo.rusinov.consensus.paxos.core.PaxosServer;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.DefaultPaxosEnvironment;

import java.io.File;
import java.util.Arrays;
import java.util.stream.Collectors;

@Slf4j
public class KvStoreApplication {

    @SneakyThrows
    private static void runOnPaxos() {
        String id = System.getenv("KV_PEER_ID");
        int port = Integer.parseInt(System.getenv("KV_PORT"));
        var destinations = Arrays.stream(System.getenv("KV_PEERS").split(","))
                .collect(Collectors.toMap(
                        (p) -> p.split(":")[0],
                        (p) -> {
                            var addressParts = p.split(":");
                            return addressParts[1] + ":" + addressParts[2];
                        }
                ));
        var config = new Config(destinations.keySet());
        String storagePath = System.getenv("KV_STORAGE_PATH");
        var stateMachine = new KvStorePaxosStateMachine(new MapDbKvDatabase());

        var server = new PaxosServer(
                id,
                stateMachine,
                new DefaultPaxosEnvironment(new SimpleDistributedServer(port), new SimpleEnvironmentClient(destinations, 15000), id),
                config,
                new File(storagePath),
                new MapDBDurableStateStore()
        );

        server.start();

        while (true) {
            Thread.sleep(1000);
        }
    }

    public static void main(String[] args) {
        String protocol = System.getenv("KV_STORE_PROTOCOL");

        if (protocol.equals("paxos")) {
            runOnPaxos();
        } else {
            runOnRaft();
        }
    }

    @SneakyThrows
    private static void runOnRaft() {
        String id = System.getenv("KV_PEER_ID");
        int port = Integer.parseInt(System.getenv("KV_PORT"));
        var destinations = Arrays.stream(System.getenv("KV_PEERS").split(","))
                .collect(Collectors.toMap(
                        (p) -> p.split(":")[0],
                        (p) -> {
                            var addressParts = p.split(":");
                            return addressParts[1] + ":" + addressParts[2];
                        }
                ));
        String storagePath = System.getenv("KV_STORAGE_PATH");
        var stateMachine = new KvStoreRaftStateMachine(new MapDbKvDatabase());

        var server = new ru.itmo.rusinov.consensus.raft.core.RaftServer(
                new ru.itmo.rusinov.consensus.raft.core.MapDBDurableStateStore(),
                id,
                destinations.keySet(),
                new SimpleEnvironmentClient(destinations, 150),
                new SimpleDistributedServer(port),
                stateMachine,
                new File(storagePath)
        );

        server.start();

        while (true) {
            Thread.sleep(1000);
        }
    }
}
