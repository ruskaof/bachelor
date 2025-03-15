package ru.itmo.rusinov.consensus.kv.store.paxos;

import lombok.SneakyThrows;
import ru.itmo.rusinov.consensus.common.NettyDistributedServer;
import ru.itmo.rusinov.consensus.common.NettyEnvironmentClient;
import ru.itmo.rusinov.consensus.common.SimpleDistributedServer;
import ru.itmo.rusinov.consensus.common.SimpleEnvironmentClient;
import ru.itmo.rusinov.consensus.paxos.core.MapDBDurableStateStore;
import ru.itmo.rusinov.consensus.paxos.core.PaxosServer;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.DefaultPaxosEnvironment;

import java.io.File;
import java.util.Map;
import java.util.Set;

public class ThreeReplicas {
    @SneakyThrows
    public static void main(String[] args) {
        var localhost = "localhost";
        var destinations = Map.of(
                "0", "localhost:8081",
                "1", "localhost:8082",
                "2", "localhost:8083"
        );
        var config = new Config(Set.of(
                "0", "1", "2"
        ));

        var server1 = new PaxosServer("0",
                new LoggingNopStateMachine(),
                new DefaultPaxosEnvironment(new SimpleDistributedServer(8081), new SimpleEnvironmentClient(destinations)),
                config, new File("C:\\Users\\199-4\\Desktop\\tmpkvstore\\1"),
                new MapDBDurableStateStore()
        );
        var server2 = new PaxosServer("0",
                new LoggingNopStateMachine(),
                new DefaultPaxosEnvironment(new SimpleDistributedServer(8082), new SimpleEnvironmentClient(destinations)),
                config, new File("C:\\Users\\199-4\\Desktop\\tmpkvstore\\2"),
                new MapDBDurableStateStore()
        );
        var server3 = new PaxosServer("0",
                new LoggingNopStateMachine(),
                new DefaultPaxosEnvironment(new SimpleDistributedServer(8083), new SimpleEnvironmentClient(destinations)),
                config, new File("C:\\Users\\199-4\\Desktop\\tmpkvstore\\3"),
                new MapDBDurableStateStore()
        );

        server1.start();
        server2.start();
        server3.start();

        while (true) {
            Thread.sleep(1000);
        }
    }
}
