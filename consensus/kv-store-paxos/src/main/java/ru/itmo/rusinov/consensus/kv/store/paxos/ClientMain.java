package ru.itmo.rusinov.consensus.kv.store.paxos;

import ru.itmo.rusinov.consensus.paxos.core.command.Command;
import ru.itmo.rusinov.consensus.paxos.core.environment.TCPSocketEnvironment;
import ru.itmo.rusinov.consensus.paxos.core.message.RequestMessage;

import java.util.UUID;

public class ClientMain {
    public static void main(String[] args) {
        var replicaUuid = new UUID(0, 3);

        var environment = new TCPSocketEnvironment(8080);

        environment.addDestination(replicaUuid, "localhost", 7913);

        environment.sendMessage(replicaUuid, new RequestMessage(new UUID(1, 0), new TestCommand("test'test")));
    }

    record TestCommand(String message) implements Command {
    }
}
