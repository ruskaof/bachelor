package ru.itmo.rusinov.consensus.kv.store.paxos;

import lombok.SneakyThrows;
import ru.itmo.rusinov.consensus.common.SimpleDistributedServer;
import ru.itmo.rusinov.consensus.common.SimpleEnvironmentClient;
import ru.itmo.rusinov.consensus.raft.core.MapDBDurableStateStore;
import ru.itmo.rusinov.consensus.raft.core.RaftServer;

import java.io.File;
import java.util.Map;

public class RaftThreeReplicas {
    @SneakyThrows
    public static void main(String[] args) {
        var destinations = Map.of(
                "0", "localhost:8081",
                "1", "localhost:8082",
                "2", "localhost:8083"
        );
        var server1 = new RaftServer(
                new MapDBDurableStateStore(),
                "0",
                destinations.keySet(),
                new SimpleEnvironmentClient(destinations),
                new SimpleDistributedServer(8081),
                new RaftLoggingNopStateMachine(),
                new File("C:\\Users\\199-4\\Desktop\\tmpkvstore\\1")
        );
        var server2 = new RaftServer(
                new MapDBDurableStateStore(),
                "1",
                destinations.keySet(),
                new SimpleEnvironmentClient(destinations),
                new SimpleDistributedServer(8082),
                new RaftLoggingNopStateMachine(),
                new File("C:\\Users\\199-4\\Desktop\\tmpkvstore\\2")
        );
        var server3 = new RaftServer(
                new MapDBDurableStateStore(),
                "2",
                destinations.keySet(),
                new SimpleEnvironmentClient(destinations),
                new SimpleDistributedServer(8083),
                new RaftLoggingNopStateMachine(),
                new File("C:\\Users\\199-4\\Desktop\\tmpkvstore\\3")
        );

        server1.start();
        server2.start();
        server3.start();

        var client = new RaftClient(destinations.keySet().stream().toList(), new SimpleEnvironmentClient(destinations));
        Thread.sleep(10000);
        var result = client.getStringValue("test");
        System.out.println(result);

        while (true) {
            Thread.sleep(1000);
        }
    }
}
