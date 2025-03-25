package ru.itmo.rusinov.consensus.paxos.core;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import paxos.Paxos.BallotNumber;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;
import ru.itmo.rusinov.consensus.paxos.core.environment.PaxosRequest;

@Slf4j
public class Leader {

    private final String id;
    private final Map<Long, Paxos.Command> proposals = new HashMap<>();
    private boolean active = true;
    private BallotNumber ballotNumber;
    private final BallotNumberComparator ballotNumberComparator = new BallotNumberComparator();

    private final Environment environment;
    private final Config config;

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final DurableStateStore durableStateStore;

    private String suggestedLeader = null;

    public Leader(String id, Environment environment, Config config, DurableStateStore durableStateStore) {
        this.id = id;
        this.environment = environment;
        this.config = config;
        this.ballotNumber = BallotNumber.newBuilder()
                .setLeaderId(id)
                .setRound(0)
                .build();
        this.durableStateStore = durableStateStore;
    }

    public void run() {
        log.info("Starting leader {}", id);
        executor.submit(new Scout(id, UUID.randomUUID(), environment, config, ballotNumber, durableStateStore.loadLastAppliedSlot().orElse(-1L), null));
        while (true) {
            handleMessage(this.environment.getNextLeaderMessage());
        }
    }

    private void handleMessage(PaxosRequest request) {
        var message = request.message();
        log.info("Handling message {} from {}", message.getMessageCase(), message.getSrc());

        switch (message.getMessageCase()) {
            case PROPOSE -> {
                var pm = message.getPropose();
                if (this.proposals.containsKey(pm.getSlotNumber())) {
                    return;
                }

                this.proposals.put(pm.getSlotNumber(), pm.getCommand());
                if (this.active) {
                    executor.submit(new Commander(id, UUID.randomUUID(), environment, config, ballotNumber, pm.getCommand(), pm.getSlotNumber()));
                } else {
                    log.info("Ignoring proposal because inactive");

                    if (Objects.nonNull(suggestedLeader)) {
                        var im = Paxos.InactiveMessage.newBuilder()
                                .setSuggestedLeader(suggestedLeader)
                                .build();
                        var inactiveMessage = Paxos.PaxosMessage.newBuilder()
                                .setInactive(im)
                                .build();
                        environment.sendMessageToColocatedReplica(inactiveMessage);
                    }
                }
            }
            case ADOPTED -> {
                var am = message.getAdopted();
                if (!this.ballotNumber.equals(am.getBallotNumber())) {
                    return;
                }

                var max = new HashMap<Long, BallotNumber>();

                for (Paxos.Pvalue pv : am.getAcceptedList()) {
                    var bn = max.get(pv.getSlotNumber());
                    if (Objects.isNull(bn) || ballotNumberComparator.compare(bn, pv.getBallotNumber()) < 0) {
                        max.put(pv.getSlotNumber(), pv.getBallotNumber());
                        this.proposals.put(pv.getSlotNumber(), pv.getCommand());
                    }

                }
                collectGarbage();
                this.proposals.forEach((sn, c) ->
                        executor.submit(new Commander(id, UUID.randomUUID(), environment, config, ballotNumber, c, sn)));
                this.active = true;
            }
            case PREEMPTED -> {
                var pm = message.getPreempted();
                if (ballotNumberComparator.compare(pm.getBallotNumber(), this.ballotNumber) > 0) {
                    this.ballotNumber = BallotNumber.newBuilder()
                            .setRound(pm.getBallotNumber().getRound() + 1)
                            .setLeaderId(this.id)
                            .build();
                    executor.submit(new Scout(id, UUID.randomUUID(), environment, config, ballotNumber, durableStateStore.loadLastAppliedSlot().orElse(-1L), pm.getBallotNumber().getLeaderId()));
                    this.active = false;
                } else {
                    log.info("Preempted is ignored because ({}, {}) less than ({}, {})", pm.getBallotNumber().getRound(), pm.getBallotNumber().getLeaderId(), ballotNumber.getRound(), ballotNumber.getLeaderId());
                }
            }
            case PING -> {
            }

            default -> log.error("Unknown message: {}", message);
        }
    }

    private void collectGarbage() {
        var minApplied = durableStateStore.loadLastAppliedSlot();
        if (minApplied.isEmpty()) return;

        proposals.entrySet().removeIf((e) -> e.getKey() <= minApplied.get());
    }
}
