package ru.itmo.rusinov.consensus.paxos.core;

import lombok.extern.slf4j.Slf4j;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;
import ru.itmo.rusinov.consensus.paxos.core.message.AdoptedMessage;
import ru.itmo.rusinov.consensus.paxos.core.message.P1aMessage;
import ru.itmo.rusinov.consensus.paxos.core.message.P1bMessage;
import ru.itmo.rusinov.consensus.paxos.core.message.PreemptedMessage;

import java.util.HashSet;
import java.util.UUID;

@Slf4j
public class Scout implements Runnable {

    private final UUID replicaId;
    private final UUID id;
    private final Environment environment;
    private final Config config;
    private final BallotNumber ballotNumber;

    public Scout(UUID replicaId, UUID id, Environment environment, Config config, BallotNumber ballotNumber) {
        this.replicaId = replicaId;
        this.id = id;
        this.environment = environment;
        this.config = config;
        this.ballotNumber = ballotNumber;
    }

    @Override
    public void run() {
        var waitFor = new HashSet<UUID>();

        for (UUID a : config.replicas()) {
            environment.sendMessage(a, new P1aMessage(replicaId, id, ballotNumber));
            waitFor.add(a);
        }

        var pvalues = new HashSet<Pvalue>();
        while (true) {
            var msg = environment.getNextScoutMessage(id);

            if (msg instanceof P1bMessage p1b) {
                if (ballotNumber.equals(p1b.ballotNumber()) && waitFor.contains(p1b.src())) {
                    pvalues.addAll(p1b.accepted());
                    waitFor.remove(p1b.src());
                    if (waitFor.size() < (this.config.replicas().size() + 1) / 2) {
                        environment.sendMessage(replicaId, new AdoptedMessage(replicaId, this.ballotNumber, pvalues));
                        break;
                    }
                } else {
                    environment.sendMessage(replicaId, new PreemptedMessage(replicaId, p1b.ballotNumber()));
                    break;
                }
            }
        }
    }
}
