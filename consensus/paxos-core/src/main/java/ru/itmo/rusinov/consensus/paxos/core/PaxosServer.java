package ru.itmo.rusinov.consensus.paxos.core;

import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;
import java.util.UUID;

public class PaxosServer {

    private final Acceptor acceptor;
    private final Replica replica;
    private final Leader leader;

    public PaxosServer(UUID id, StateMachine stateMachine, Environment environment, Config config) {
        this.acceptor = new Acceptor(environment, id);
        this.replica = new Replica(environment, stateMachine, id, config);
        this.leader = new Leader(id, environment, config);
    }

    public void start() {
        new Thread(acceptor::run, "AcceptorThread").start();
        new Thread(replica::run, "ReplicaThread").start();
        new Thread(leader::run, "LeaderThread").start();
    }
}
