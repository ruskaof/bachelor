package ru.itmo.rusinov.consensus.kv.store.paxos;

import ru.itmo.rusinov.consensus.paxos.core.Replica;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.TCPSocketEnvironment;

import java.util.Set;
import java.util.UUID;

public class ReplicaMain {
    public static void main(String[] args) {
        var leaderUuid = new UUID(0, 0);
        var acceptorUuid = new UUID(0, 1);
        var replicaUuid = new UUID(0, 2);

        var config = new Config(Set.of(leaderUuid), Set.of(replicaUuid), Set.of(acceptorUuid));
        var environment = new TCPSocketEnvironment(7912);

        environment.addDestination(leaderUuid, "localhost", 7910);
        environment.addDestination(acceptorUuid, "localhost", 7911);

        var replica = new Replica(environment, new LoggingNopStateMachine(), replicaUuid, config);

        replica.run();
    }
}
