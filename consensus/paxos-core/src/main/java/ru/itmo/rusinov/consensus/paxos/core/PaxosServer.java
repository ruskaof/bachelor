package ru.itmo.rusinov.consensus.paxos.core;

import lombok.extern.slf4j.Slf4j;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;

import java.io.File;

@Slf4j
public class PaxosServer {

    private final String id;
    private final Acceptor acceptor;
    private final Replica replica;
    private final Leader leader;
    private final StateMachine stateMachine;
    private final File stateMachineDir;

    public PaxosServer(String id, StateMachine stateMachine, Environment environment, Config config, File stateMachineDir) {
        this.id = id;
        this.stateMachine = stateMachine;
        this.stateMachineDir = stateMachineDir;
        this.acceptor = new Acceptor(environment, id);
        this.replica = new Replica(environment, stateMachine, id, config);
        this.leader = new Leader(id, environment, config);
    }

    public void start() {
        log.info("Starting paxos server with id {}", id);

        stateMachine.initialize(stateMachineDir);

        new Thread(acceptor::run, "AcceptorThread").start();
        new Thread(replica::run, "ReplicaThread").start();
        new Thread(leader::run, "LeaderThread").start();
    }
}
