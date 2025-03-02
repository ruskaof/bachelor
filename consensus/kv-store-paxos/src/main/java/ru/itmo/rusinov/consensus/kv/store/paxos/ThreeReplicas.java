package ru.itmo.rusinov.consensus.kv.store.paxos;

import lombok.SneakyThrows;
import ru.itmo.rusinov.consensus.paxos.core.PaxosServer;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.TCPSocketEnvironment;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ThreeReplicas {
    @SneakyThrows
    public static void main(String[] args) {
        var localhost = "localhost";
        var destinations = Map.of(
                "0", new InetSocketAddress(localhost, 7910),
                "1", new InetSocketAddress(localhost, 7911),
                "2", new InetSocketAddress(localhost, 7912)
        );
        var config = new Config(Set.of(
                "0", "1", "2"
        ));

        var server1 = new PaxosServer("0", new LoggingNopStateMachine(), new TCPSocketEnvironment(7910, destinations), config);
        var server2 = new PaxosServer("1", new LoggingNopStateMachine(), new TCPSocketEnvironment(7911, destinations), config);
        var server3 = new PaxosServer("2", new LoggingNopStateMachine(), new TCPSocketEnvironment(7912, destinations), config);

        server1.start();
        server2.start();
        server3.start();

        while (true) {
            Thread.sleep(1000);
        }
    }
}
