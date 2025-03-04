package ru.itmo.rusinov.consensus.kv.store.paxos;

import lombok.SneakyThrows;
import ru.itmo.rusinov.consensus.paxos.core.PaxosServer;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.TCPSocketEnvironment;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

public class ThreeReplicas {
    @SneakyThrows
    public static void main(String[] args) {
        var localhost = "localhost";
        var destinations = Map.of(
                "0", new InetSocketAddress(localhost, 8081),
                "1", new InetSocketAddress(localhost, 8082),
                "2", new InetSocketAddress(localhost, 8083)
        );
        var config = new Config(Set.of(
                "0", "1", "2"
        ));

        var server1 = new PaxosServer("0", new LoggingNopStateMachine(), new TCPSocketEnvironment(8081, destinations), config, new File("/tmp"));
        var server2 = new PaxosServer("1", new LoggingNopStateMachine(), new TCPSocketEnvironment(8082, destinations), config, new File("/tmp"));
        var server3 = new PaxosServer("2", new LoggingNopStateMachine(), new TCPSocketEnvironment(8083, destinations), config, new File("/tmp"));

        server1.start();
        server2.start();
        server3.start();

        while (true) {
            Thread.sleep(1000);
        }
    }
}
