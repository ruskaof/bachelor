package ru.itmo.rusinov.consensus.paxos.core;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import paxos.Paxos.BallotNumber;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;

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

    public Leader(String id, Environment environment, Config config) {
        this.id = id;
        this.environment = environment;
        this.config = config;
        this.ballotNumber = BallotNumber.newBuilder()
                .setLeaderId(id)
                .setRound(0)
                .build();
    }

    public void run() {
        log.info("Starting leader {}", id);
        executor.submit(new Scout(id, UUID.randomUUID(), environment, config, ballotNumber));
        while (true) {
            handleMessage(this.environment.getNextLeaderMessage());
        }
    }

    private void handleMessage(Paxos.PaxosMessage message) {
        log.info("Handling message {} from {}", message.getMessageCase(), message.getSrc());

        switch (message.getMessageCase()) {
            case PROPOSE -> {
                var pm = message.getPropose();
                this.proposals.putIfAbsent(pm.getSlotNumber(), pm.getCommand());
                if (this.active) {
                    executor.submit(new Commander(id, UUID.randomUUID(), environment, config, ballotNumber, pm.getCommand(), pm.getSlotNumber()));
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
                    executor.submit(new Scout(id, UUID.randomUUID(), environment, config, ballotNumber));
                    this.active = false;
                } else {
                    log.info("Preempted is ignored because ({}, {}) less than ({}, {})", pm.getBallotNumber().getRound(), pm.getBallotNumber().getLeaderId(), ballotNumber.getRound(), ballotNumber.getLeaderId());
                }
            }

            default -> log.error("Unknown message: {}", message);
        }
    }
}
