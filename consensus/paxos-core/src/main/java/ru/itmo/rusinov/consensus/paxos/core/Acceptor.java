package ru.itmo.rusinov.consensus.paxos.core;

import lombok.extern.slf4j.Slf4j;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;
import ru.itmo.rusinov.consensus.paxos.core.message.P1aMessage;
import ru.itmo.rusinov.consensus.paxos.core.message.P1bMessage;
import ru.itmo.rusinov.consensus.paxos.core.message.P2aMessage;
import ru.itmo.rusinov.consensus.paxos.core.message.P2bMessage;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Slf4j
public class Acceptor {

    private final Environment environment;

    private BallotNumber ballotNumber = BallotNumber.MIN;
    private final Set<Pvalue> accepted = new HashSet<>();
    private final UUID id;

    public Acceptor(Environment environment, UUID id) {
        this.environment = environment;
        this.id = id;
    }

    public void run() {
        log.info("Starting acceptor {}", id);
        while (true) {
            var msg = environment.getNextAcceptorMessage();
            log.info("Handling message: {}", msg);
            switch (msg) {
                case P1aMessage p1a -> {
                    if (p1a.ballotNumber().compareTo(this.ballotNumber) > 0) {
                        this.ballotNumber = p1a.ballotNumber();
                    }
                    environment.sendMessage(p1a.src(), new P1bMessage(this.id, this.ballotNumber, this.accepted));
                }
                case P2aMessage p2a -> {
                    if (p2a.ballotNumber().equals(this.ballotNumber)) {
                        this.accepted.add(new Pvalue(p2a.ballotNumber(), p2a.slotNumber(), p2a.command()));
                    }
                    environment.sendMessage(p2a.src(), new P2bMessage(this.id, this.ballotNumber, p2a.slotNumber()));
                }
                default -> {
                    log.error("Unexpected message: {}", msg);
                }
            }
        }
    }
}
