package ru.itmo.rusinov.consensus.paxos.core;

import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import paxos.Paxos.BallotNumber;
import paxos.Paxos.Pvalue;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class Acceptor {

    private final Environment environment;
    private final DurableStateStore stateStore;

    private BallotNumber ballotNumber;
    private final BallotNumberComparator ballotNumberComparator = new BallotNumberComparator();

    private final Map<Long, Pvalue> accepted;
    private final Map<String, Long> lastApplied;
    private final Config config;
    private final String id;

    public Acceptor(Environment environment, Config config, DurableStateStore stateStore, String id) {
        this.environment = environment;
        this.stateStore = stateStore;
        this.id = id;
        this.ballotNumber = stateStore.loadAcceptorBallotNumber().orElse(BallotNumberComparator.MIN);
        this.accepted = stateStore.loadAcceptedValues();
        this.lastApplied = new HashMap<>();
        this.config = config;
    }

    public void run() {
        log.info("Starting acceptor {}", id);
        while (true) {
            var msg = environment.getNextAcceptorMessage();
            log.info("Handling message {} from {}", msg.getMessageCase(), msg.getSrc());
            switch (msg.getMessageCase()) {
                case P1A -> handleP1a(msg);
                case P2A -> handleP2a(msg);
                default -> log.error("Unexpected message: {}", msg);
            }
        }
    }

    private void handleP2a(Paxos.PaxosMessage msg) {
        var p2a = msg.getP2A();
        if (ballotNumberComparator.compare(ballotNumber, p2a.getBallotNumber()) <= 0) {
            ballotNumber = p2a.getBallotNumber();  // Adopt the new ballot number
            stateStore.saveAcceptorBallotNumber(ballotNumber);

            var pvalue = Paxos.Pvalue.newBuilder()
                    .setBallotNumber(p2a.getBallotNumber())
                    .setSlotNumber(p2a.getSlotNumber())
                    .setCommand(p2a.getCommand())
                    .build();
            accepted.put(p2a.getSlotNumber(), pvalue);  // Store only the latest p-value per slot
            stateStore.saveAcceptedValue(p2a.getSlotNumber(), pvalue);

            log.info("{}", acceptedLog());
        } else {
            log.info("Cannot accept value because its ballot number ({}, {}) is less than ({}, {})",
                    p2a.getBallotNumber().getRound(), p2a.getBallotNumber().getLeaderId(),
                    ballotNumber.getRound(), ballotNumber.getLeaderId());
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
            stateStore.saveAcceptorBallotNumber(ballotNumber);
        }

        lastApplied.put(msg.getSrc(), p1a.getLastAppliedSlot());
        collectGarbage();

        var p1b = Paxos.P1bMessage.newBuilder()
                .setBallotNumber(this.ballotNumber)
                .addAllAccepted(getAcceptedFrom(p1a.getLastAppliedSlot() + 1))
                .setScoutId(p1a.getScoutId());

        var p1bMessage = Paxos.PaxosMessage
                .newBuilder()
                .setSrc(this.id)
                .setP1B(p1b)
                .build();

        environment.sendMessage(msg.getSrc(), p1bMessage);
    }

    private List<Pvalue> getAcceptedFrom(long slotNumber) {
        return this.accepted.entrySet()
                .stream()
                .filter((e) -> e.getKey() >= slotNumber)
                .map(Map.Entry::getValue)
                .toList();
    }

    private void collectGarbage() {
        if (this.lastApplied.size() < config.replicas().size()) {
            return;
        }

        var minApplied = lastApplied.values().stream().min(Long::compareTo);
        if (minApplied.isEmpty()) {
            return;
        }

        log.info("Collecting garbage with minApplied: {}", minApplied.get());
        this.accepted.entrySet().removeIf((e) -> e.getKey() <= minApplied.get());
        this.stateStore.clearAcceptedBelowSlot(minApplied.get() + 1);
    }

    private String acceptedLog() {
        var sb = new StringJoiner(" ");
        sb.add("Accepted:");
        for (var e : accepted.entrySet()) {
            sb.add("[" + e.getKey().toString() + "-" + LogUtils.ballotNumberStr(e.getValue().getBallotNumber()) + "|" + e.getValue().getCommand().getContent().toStringUtf8() + "]");
        }

        sb.add("LastApplied: " + lastApplied.toString());
        return sb.toString();
    }
}
