package ru.itmo.rusinov.consensus.kv.store.paxos;

import ru.itmo.rusinov.consensus.paxos.core.Acceptor;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.TCPSocketEnvironment;

import java.util.Set;
import java.util.UUID;

public class AcceptorMain {
    public static void main(String[] args) {
        var leaderUuid = new UUID(0, 0);
        var acceptorUuid = new UUID(0, 1);
        var replicaUuid = new UUID(0, 2);

        var config = new Config(Set.of(leaderUuid), Set.of(replicaUuid), Set.of(acceptorUuid));
        var environment = new TCPSocketEnvironment(7911);

        environment.addDestination(leaderUuid, "localhost", 7910);
        environment.addDestination(replicaUuid, "localhost", 7912);

        var acceptor = new Acceptor(environment, acceptorUuid);

        acceptor.run();
    }
}
