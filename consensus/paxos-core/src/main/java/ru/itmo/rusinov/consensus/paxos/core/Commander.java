package ru.itmo.rusinov.consensus.paxos.core;

import ru.itmo.rusinov.consensus.paxos.core.command.Command;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;
import ru.itmo.rusinov.consensus.paxos.core.message.DecisionMessage;
import ru.itmo.rusinov.consensus.paxos.core.message.P2aMessage;
import ru.itmo.rusinov.consensus.paxos.core.message.P2bMessage;
import ru.itmo.rusinov.consensus.paxos.core.message.PreemptedMessage;

import java.util.HashSet;
import java.util.UUID;

public class Commander implements Runnable {

    private final UUID replicaId;
    private final UUID id;
    private final Environment environment;
    private final Config config;
    private final BallotNumber ballotNumber;
    private final Command command;
    private final Long slotNumber;

    public Commander(UUID replicaId, UUID id, Environment environment, Config config, BallotNumber ballotNumber, Command command, Long slotNumber) {
        this.replicaId = replicaId;
        this.id = id;
        this.environment = environment;
        this.config = config;
        this.ballotNumber = ballotNumber;
        this.command = command;
        this.slotNumber = slotNumber;
    }

    @Override
    public void run() {
        var waitFor = new HashSet<UUID>();

        for (UUID a : this.config.replicas()) {
            this.environment.sendMessage(a, new P2aMessage(replicaId, ballotNumber, slotNumber, command, id));
            waitFor.add(a);
        }

        while (true) {
            var msg = environment.getNextCommanderMessage(id);
            if (msg instanceof P2bMessage p2b) {
                if (ballotNumber.equals(p2b.ballotNumber()) && waitFor.contains(p2b.src())) {
                    waitFor.remove(p2b.src());
                    if (waitFor.size() < (this.config.replicas().size() + 1) / 2) {
                        for (UUID r : this.config.replicas()) {
                            this.environment.sendMessage(r, new DecisionMessage(replicaId, slotNumber, command));
                        }
                        break;
                    }
                } else {
                    environment.sendMessage(replicaId, new PreemptedMessage(replicaId, p2b.ballotNumber()));
                    break;
                }
            }
        }
    }
}
