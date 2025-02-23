package ru.itmo.rusinov.consensus.kv.store.paxos;

import lombok.SneakyThrows;
import ru.itmo.rusinov.consensus.paxos.core.Acceptor;
import ru.itmo.rusinov.consensus.paxos.core.Leader;
import ru.itmo.rusinov.consensus.paxos.core.Replica;
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
                // leaders
                new UUID(0, 0), new InetSocketAddress(localhost, 7910),
                new UUID(0, 1), new InetSocketAddress(localhost, 7911),
                new UUID(0, 2), new InetSocketAddress(localhost, 7912),

                // replicas
                new UUID(0, 3), new InetSocketAddress(localhost, 7913),
                new UUID(0, 4), new InetSocketAddress(localhost, 7914),
                new UUID(0, 5), new InetSocketAddress(localhost, 7915),

                // acceptors
                new UUID(0, 6), new InetSocketAddress(localhost, 7916),
                new UUID(0, 7), new InetSocketAddress(localhost, 7917),
                new UUID(0, 8), new InetSocketAddress(localhost, 7918)
        );

        var config = new Config(
                Set.of(new UUID(0, 0), new UUID(0, 1), new UUID(0, 2)),
                Set.of(new UUID(0, 3), new UUID(0, 4), new UUID(0, 5)),
                Set.of(new UUID(0, 6), new UUID(0, 7), new UUID(0, 8))
        );

        var leader1 = new Leader(new UUID(0, 0), new TCPSocketEnvironment(7910, destinations), config);
        var leader2 = new Leader(new UUID(0, 1), new TCPSocketEnvironment(7911, destinations), config);
        var leader3 = new Leader(new UUID(0, 2), new TCPSocketEnvironment(7912, destinations), config);
        new Thread(leader1::run).start();
        new Thread(leader2::run).start();
        new Thread(leader3::run).start();

        var replica1 = new Replica(new TCPSocketEnvironment(7913, destinations), new LoggingNopStateMachine(), new UUID(0, 3), config);
        var replica2 = new Replica(new TCPSocketEnvironment(7914, destinations), new LoggingNopStateMachine(), new UUID(0, 4), config);
        var replica3 = new Replica(new TCPSocketEnvironment(7915, destinations), new LoggingNopStateMachine(), new UUID(0, 5), config);
        new Thread(replica1::run).start();
        new Thread(replica2::run).start();
        new Thread(replica3::run).start();

        var acceptor1 = new Acceptor(new TCPSocketEnvironment(7916, destinations), new UUID(0, 6));
        var acceptor2 = new Acceptor(new TCPSocketEnvironment(7917, destinations), new UUID(0, 7));
        var acceptor3 = new Acceptor(new TCPSocketEnvironment(7918, destinations), new UUID(0, 8));
        new Thread(acceptor1::run).start();
        new Thread(acceptor2::run).start();
        new Thread(acceptor3::run).start();

        while (true) {
            Thread.sleep(1000);
        }
    }
}
