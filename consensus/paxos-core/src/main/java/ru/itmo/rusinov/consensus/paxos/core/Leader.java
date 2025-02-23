package ru.itmo.rusinov.consensus.paxos.core;

import java.util.*;

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

    public Leader(UUID id, Environment environment, Config config) {
        this.id = id;
        this.environment = environment;
        this.config = config;
        this.ballotNumber = new BallotNumber(0, id);
    }

    public void run() {
        log.info("Starting leader {}", id);
        launchScout();
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
                    launchCommander(this.ballotNumber, pm.slotNumber(), pm.command());
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
                    this.proposals.forEach((sn, c) -> {
                        launchCommander(this.ballotNumber, sn, c);
                    });
                    this.active = true;
                }
            }
            case PreemptedMessage pm -> {
                if (pm.ballotNumber().compareTo(this.ballotNumber) > 0) {
                    this.ballotNumber = new BallotNumber(pm.ballotNumber().round() + 1, this.id);
                    launchScout();
                }
                this.active = false;
            }

            default -> log.error("Unknown message: {}", message);
        }
    }

    private void launchScout() {
        var waitFor = new HashSet<UUID>();

        for (UUID a : this.config.replicas()) {
            this.environment.sendMessage(a, new P1aMessage(this.id, this.ballotNumber));
            waitFor.add(a);
        }

        var pvalues = new HashSet<Pvalue>();
        while (true) {
            var msg = this.environment.getNextLeaderMessage();

            if (msg instanceof P1bMessage p1b) {
                if (this.ballotNumber.equals(p1b.ballotNumber()) && waitFor.contains(p1b.src())) {
                    pvalues.addAll(p1b.accepted());
                    waitFor.remove(p1b.src());
                    if (waitFor.size() < (this.config.replicas().size() + 1) / 2) {
                        this.handleMessage(new AdoptedMessage(this.id, this.ballotNumber, pvalues));
                        break;
                    }
                } else {
                    this.handleMessage(new PreemptedMessage(this.id, p1b.ballotNumber()));
                    break;
                }
            }
        }
    }

    private void launchCommander(BallotNumber ballotNumber, Long slotNumber, Command command) {
        var waitFor = new HashSet<UUID>();

        for (UUID a : this.config.replicas()) {
            this.environment.sendMessage(a, new P2aMessage(this.id, ballotNumber, slotNumber, command));
            waitFor.add(a);
        }

        while (true) {
            var msg = environment.getNextLeaderMessage();
            if (msg instanceof P2bMessage p2b) {
                if (ballotNumber.equals(p2b.ballotNumber()) && waitFor.contains(p2b.src())) {
                    waitFor.remove(p2b.src());
                    if (waitFor.size() < (this.config.replicas().size() + 1) / 2) {
                        for (UUID r : this.config.replicas()) {
                            this.environment.sendMessage(r, new DecisionMessage(this.id, slotNumber, command));
                        }
                        break;
                    }
                } else {
                    this.handleMessage(new PreemptedMessage(this.id, p2b.ballotNumber()));
                    break;
                }
            }
        }
    }
}
