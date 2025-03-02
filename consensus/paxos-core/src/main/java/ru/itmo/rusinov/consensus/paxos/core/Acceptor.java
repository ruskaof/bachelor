package ru.itmo.rusinov.consensus.paxos.core;

import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import paxos.Paxos.BallotNumber;
import paxos.Paxos.Pvalue;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Slf4j
public class Acceptor {

    private final Environment environment;

    private BallotNumber ballotNumber = BallotNumberComparator.MIN;
    private BallotNumberComparator ballotNumberComparator = new BallotNumberComparator();
    private final Set<Pvalue> accepted = new HashSet<>();
    private final String id;

    public Acceptor(Environment environment, String id) {
        this.environment = environment;
        this.id = id;
    }

    public void run() {
        log.info("Starting acceptor {}", id);
        while (true) {
            var msg = environment.getNextAcceptorMessage();
            log.info("Handling message: {}", msg);
            switch (msg.getMessageCase()) {
                case P1A -> handleP1a(msg);
                case P2A -> handleP2a(msg);
                default -> log.error("Unexpected message: {}", msg);
            }
        }
    }

    private void handleP2a(Paxos.PaxosMessage msg) {
        var p2a = msg.getP2A();
        if (p2a.getBallotNumber().equals(this.ballotNumber)) {
            var pvalue = Paxos.Pvalue.newBuilder()
                    .setBallotNumber(p2a.getBallotNumber())
                    .setSlotNumber(p2a.getSlotNumber())
                    .setCommand(p2a.getCommand())
                    .build();
            this.accepted.add(pvalue);
        }
        var p2b = Paxos.P2bMessage.newBuilder()
                .setBallotNumber(this.ballotNumber)
                .setSlotNumber(p2a.getSlotNumber())
                .setCommanderId(p2a.getCommanderId());
        var p2bMessage = Paxos.PaxosMessage
                .newBuilder()
                .setSrc(this.id)
                .setP2B(p2b)
                .build();

        environment.sendMessage(msg.getSrc(), p2bMessage);
    }

    private void handleP1a(Paxos.PaxosMessage msg) {
        var p1a = msg.getP1A();
        if (ballotNumberComparator.compare(p1a.getBallotNumber(), this.ballotNumber) > 0) {
            this.ballotNumber = p1a.getBallotNumber();
        }
        var p1b = Paxos.P1bMessage.newBuilder()
                .setBallotNumber(this.ballotNumber)
                .addAllAccepted(this.accepted)
                .setScoutId(p1a.getScoutId());
        var p1bMessage = Paxos.PaxosMessage
                .newBuilder()
                .setSrc(this.id)
                .setP1B(p1b)
                .build();

        environment.sendMessage(msg.getSrc(), p1bMessage);
    }
}
