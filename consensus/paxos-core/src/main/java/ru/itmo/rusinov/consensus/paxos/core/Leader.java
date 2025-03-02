package ru.itmo.rusinov.consensus.paxos.core;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import lombok.extern.slf4j.Slf4j;
import ru.itmo.rusinov.consensus.paxos.core.command.Command;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;
import ru.itmo.rusinov.consensus.paxos.core.message.*;

@Slf4j
public class Leader {

    private final UUID id;
    private final Map<Long, Command> proposals = new HashMap<>();
    private boolean active = true;
    private BallotNumber ballotNumber;

    private final Environment environment;
    private final Config config;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    public Leader(UUID id, Environment environment, Config config) {
        this.id = id;
        this.environment = environment;
        this.config = config;
        this.ballotNumber = new BallotNumber(0, id);
    }

    public void run() {
        log.info("Starting leader {}", id);
        // fixme handle in environment
        executor.submit(new Scout(id, UUID.randomUUID(), environment, config, ballotNumber));
        while (true) {
            handleMessage(this.environment.getNextLeaderMessage());
        }
    }

    private void handleMessage(PaxosMessage message) {
        log.info("Handling message: {}", message);

        switch (message) {
            case ProposeMessage pm -> {
                this.proposals.putIfAbsent(pm.slotNumber(), pm.command());
                if (this.active) {
                    executor.submit(new Commander(id, UUID.randomUUID(), environment, config, ballotNumber, pm.command(), pm.slotNumber()));
                }
            }
            case AdoptedMessage am -> {
                if (this.ballotNumber.equals(am.ballotNumber())) {
                    var max = new HashMap<Long, BallotNumber>();

                    for (Pvalue pv : am.accepted()) {
                        var bn = max.get(pv.slotNumber());
                        if (Objects.isNull(bn) || bn.compareTo(pv.ballotNumber()) < 0) {
                            max.put(pv.slotNumber(), pv.ballotNumber());
                            this.proposals.put(pv.slotNumber(), pv.command());
                        }

                    }
                    this.proposals.forEach((sn, c) ->
                                    executor.submit(new Commander(id, UUID.randomUUID(), environment, config, ballotNumber, c, sn)));
                    this.active = true;
                }
            }
            case PreemptedMessage pm -> {
                if (pm.ballotNumber().compareTo(this.ballotNumber) > 0) {
                    this.ballotNumber = new BallotNumber(pm.ballotNumber().round() + 1, this.id);
                    executor.submit(new Scout(id, UUID.randomUUID(), environment, config, ballotNumber));
                }
                this.active = false;
            }

            default -> log.error("Unknown message: {}", message);
        }
    }
}
