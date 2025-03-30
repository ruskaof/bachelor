package ru.itmo.rusinov.consensus.paxos.core;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import paxos.Paxos.*;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;

import java.util.HashSet;
import java.util.Objects;
import java.util.UUID;

@Slf4j
public class Scout implements Runnable {

    private final String replicaId;
    private final UUID id;
    private final Environment environment;
    private final Config config;
    private final BallotNumber ballotNumber;
    private final long lastAppliedSlot;
    private final String activeLeaderId;

    public Scout(String replicaId, UUID id, Environment environment, Config config, BallotNumber ballotNumber, long lastAppliedSlot, String activeLeaderId) {
        this.replicaId = replicaId;
        this.id = id;
        this.environment = environment;
        this.config = config;
        this.ballotNumber = ballotNumber;
        this.lastAppliedSlot = lastAppliedSlot;
        this.activeLeaderId = activeLeaderId;
    }

    @SneakyThrows
    @Override
    public void run() {
        log.info("Running scout with id {}", id);

        if (Objects.nonNull(activeLeaderId)) {
            log.info("Monitoring leader {}", activeLeaderId);
            while (true) {
                var ping = Paxos.PaxosMessage.newBuilder()
                        .setPing(Paxos.PingMessage.newBuilder().build())
                        .setSrc(replicaId)
                        .build();

                try {
                    environment.sendMessage(activeLeaderId, ping).join();
                } catch (Exception e) {
                    log.info("Detected leader failure");
                    break;
                }

                Thread.sleep(25);
            }
        }

        var waitFor = new HashSet<String>();

        for (String r : config.replicas()) {
            var p1a = P1aMessage.newBuilder()
                    .setScoutId(id.toString())
                    .setBallotNumber(ballotNumber)
                    .setLastAppliedSlot(lastAppliedSlot);
            var p1aMessage = PaxosMessage.newBuilder()
                    .setSrc(this.replicaId)
                    .setP1A(p1a)
                    .build();
            environment.sendMessage(r, p1aMessage);
            waitFor.add(r);
        }

        var pvalues = new HashSet<Pvalue>();
        while (true) {
            var request = environment.getNextScoutMessage(id);
            var msg = request.message();
            log.info("Handling message {} from {}", msg.getMessageCase(), msg.getSrc());

            if (!msg.getMessageCase().equals(PaxosMessage.MessageCase.P1B)) {
                continue;
            }

            var p1b = msg.getP1B();

            if (ballotNumber.equals(p1b.getBallotNumber()) && waitFor.contains(msg.getSrc())) {
                pvalues.addAll(p1b.getAcceptedList());
                waitFor.remove(msg.getSrc());
                if (waitFor.size() < (this.config.replicas().size() + 1) / 2) {
                    var am = AdoptedMessage.newBuilder()
                            .setBallotNumber(ballotNumber)
                            .addAllAccepted(pvalues);
                    var amMessage = PaxosMessage.newBuilder()
                            .setSrc(replicaId)
                            .setAdopted(am)
                            .build();

                    environment.sendMessage(replicaId, amMessage);
                    break;
                }
            } else {
                var pm = PreemptedMessage.newBuilder()
                        .setBallotNumber(p1b.getBallotNumber());
                var pmMessage = PaxosMessage.newBuilder()
                        .setSrc(replicaId)
                        .setPreempted(pm)
                        .build();

                environment.sendMessage(replicaId, pmMessage);
                break;
            }
        }
        log.info("Scout with id {} is done", id);
    }
}
