package ru.itmo.rusinov.consensus.paxos.core;

import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import paxos.Paxos.BallotNumber;
import paxos.Paxos.Pvalue;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;
import ru.itmo.rusinov.consensus.paxos.core.environment.PaxosRequest;

import java.util.*;

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
            var request = environment.getNextAcceptorMessage();
            log.info("Handling message {} from {}", request.message().getMessageCase(), request.message().getSrc());
            switch (request.message().getMessageCase()) {
                case P1A -> handleP1a(request);
                case P2A -> handleP2a(request);
                default -> log.error("Unexpected message: {}", request.message());
            }
        }
    }

    private void handleP2a(PaxosRequest request) {
        var msg = request.message();
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

    private void handleP1a(PaxosRequest request) {
        var msg = request.message();
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
}
