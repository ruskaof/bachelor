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
    private final File storagePath;
    private final DurableStateStore durableStateStore;

    public PaxosServer(String id, StateMachine stateMachine, Environment environment, Config config, File storagePath, DurableStateStore durableStateStore) {
        this.id = id;
        this.stateMachine = stateMachine;
        this.storagePath = storagePath;

        stateMachine.initialize(storagePath);
        durableStateStore.initialize(storagePath);

        this.acceptor = new Acceptor(environment, config, durableStateStore, id);
        this.replica = new Replica(environment, stateMachine, id, config, durableStateStore);
        this.leader = new Leader(id, environment, config, durableStateStore);
        this.durableStateStore = durableStateStore;
    }

    public void start() {
        log.info("Starting paxos server with id {}", id);



        new Thread(acceptor::run, "AcceptorThread").start();
        new Thread(replica::run, "ReplicaThread").start();
        new Thread(leader::run, "LeaderThread").start();
    }
}
