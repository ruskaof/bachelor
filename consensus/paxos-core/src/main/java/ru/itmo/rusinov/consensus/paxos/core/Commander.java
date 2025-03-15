package ru.itmo.rusinov.consensus.paxos.core;

import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import paxos.Paxos.*;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;

import java.util.HashSet;
import java.util.UUID;

@Slf4j
public class Commander implements Runnable {

    private final String replicaId;
    private final UUID id;
    private final Environment environment;
    private final Config config;
    private final Paxos.BallotNumber ballotNumber;
    private final Paxos.Command command;
    private final Long slotNumber;

    public Commander(String replicaId, UUID id, Environment environment, Config config, Paxos.BallotNumber ballotNumber, Paxos.Command command, Long slotNumber) {
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
        var waitFor = new HashSet<String>();

        for (String r : this.config.replicas()) {
            var p2a = P2aMessage.newBuilder()
                    .setBallotNumber(ballotNumber)
                    .setSlotNumber(slotNumber)
                    .setCommand(command)
                    .setCommanderId(id.toString());
            var p2aMessage = PaxosMessage.newBuilder()
                    .setSrc(this.replicaId)
                    .setP2A(p2a)
                    .build();
            this.environment.sendMessage(r, p2aMessage);
            waitFor.add(r);
        }

        while (true) {
            var request = environment.getNextCommanderMessage(id);
            var msg = request.message();
            log.info("Handling message {} from {}", msg.getMessageCase(), msg.getSrc());

            if (msg.getMessageCase() != PaxosMessage.MessageCase.P2B) {
                continue;
            }

            var p2b = msg.getP2B();

            if (ballotNumber.equals(p2b.getBallotNumber()) && waitFor.contains(msg.getSrc())) {
                waitFor.remove(msg.getSrc());
                if (waitFor.size() < (this.config.replicas().size() + 1) / 2) {
                    for (String r : this.config.replicas()) {
                        var dm = DecisionMessage.newBuilder()
                                .setSlotNumber(slotNumber)
                                .setCommand(command);
                        var dmMessage = PaxosMessage.newBuilder()
                                .setSrc(this.replicaId)
                                .setDecision(dm)
                                .build();
                        this.environment.sendMessage(r, dmMessage);
                    }
                    break;
                }
            } else {
                var pm = PreemptedMessage.newBuilder()
                        .setBallotNumber(p2b.getBallotNumber());
                var pmMessage = PaxosMessage.newBuilder()
                        .setSrc(replicaId)
                        .setPreempted(pm)
                        .build();

                environment.sendMessage(replicaId, pmMessage);
                break;
            }

        }
    }
}
