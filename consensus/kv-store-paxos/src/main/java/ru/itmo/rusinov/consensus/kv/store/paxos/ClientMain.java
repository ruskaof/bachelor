package ru.itmo.rusinov.consensus.kv.store.paxos;

import ru.itmo.rusinov.consensus.paxos.core.command.Command;
import ru.itmo.rusinov.consensus.paxos.core.environment.TCPSocketEnvironment;
import ru.itmo.rusinov.consensus.paxos.core.message.RequestMessage;

import java.util.UUID;

public class ClientMain {
    public static void main(String[] args) {
        var leaderUuid = new UUID(0, 0);
        var acceptorUuid = new UUID(0, 1);
        var replicaUuid = new UUID(0, 2);

        var environment = new TCPSocketEnvironment(8080);

        environment.addDestination(leaderUuid, "localhost", 7910);
        environment.addDestination(acceptorUuid, "localhost", 7911);
        environment.addDestination(replicaUuid, "localhost", 7912);

        environment.sendMessage(replicaUuid, new RequestMessage(new UUID(1, 0), new TestCommand("test'test")));
    }

    record TestCommand(String message) implements Command {
    }
}
