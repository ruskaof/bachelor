package ru.itmo.rusinov.consensus.paxos.core;

import lombok.extern.slf4j.Slf4j;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;
import java.util.UUID;

@Slf4j
public class PaxosServer {

    private final UUID id;
    private final Acceptor acceptor;
    private final Replica replica;
    private final Leader leader;

    public PaxosServer(UUID id, StateMachine stateMachine, Environment environment, Config config) {
        this.id = id;
        this.acceptor = new Acceptor(environment, id);
        this.replica = new Replica(environment, stateMachine, id, config);
        this.leader = new Leader(id, environment, config);
    }

    public void start() {
        log.info("Starting paxos server with id {}", id);

        new Thread(acceptor::run, "AcceptorThread").start();
        new Thread(replica::run, "ReplicaThread").start();
        new Thread(leader::run, "LeaderThread").start();
    }
}
