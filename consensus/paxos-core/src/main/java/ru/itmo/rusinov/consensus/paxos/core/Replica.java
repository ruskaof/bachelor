package ru.itmo.rusinov.consensus.paxos.core;

import lombok.extern.slf4j.Slf4j;
import ru.itmo.rusinov.consensus.paxos.core.command.Command;
import ru.itmo.rusinov.consensus.paxos.core.command.ReconfigCommand;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;
import ru.itmo.rusinov.consensus.paxos.core.message.DecisionMessage;
import ru.itmo.rusinov.consensus.paxos.core.message.ProposeMessage;
import ru.itmo.rusinov.consensus.paxos.core.message.RequestMessage;

import java.util.*;

@Slf4j
public class Replica {

    private final long WINDOW = 5;

    private long slotIn = 1;
    private long slotOut = 1;

    private final Queue<Command> requests = new LinkedList<>();
    private final Map<Long, Command> proposals = new HashMap<>();
    private final Map<Long, Command> decisions = new HashMap<>();

    private final Environment environment;
    private final StateMachine stateMachine;
    private final UUID id;

    private Config config;

    public Replica(Environment environment, StateMachine stateMachine, UUID id, Config config) {
        this.environment = environment;
        this.stateMachine = stateMachine;
        this.id = id;
        this.config = config;
    }

    private void propose() {
        while (this.slotIn < this.slotOut + WINDOW && !this.requests.isEmpty()) {
            if (this.slotIn > WINDOW && this.decisions.containsKey(this.slotIn - WINDOW)) {
                if (this.decisions.get(this.slotIn - WINDOW) instanceof ReconfigCommand rc) {
                    this.config = rc.config();
                }
            }
            if (!decisions.containsKey(this.slotIn)) {
                var cmd = this.requests.poll();
                this.proposals.put(this.slotIn, cmd);
                for (UUID l : config.replicas()) {
                    this.environment.sendMessage(l, new ProposeMessage(this.id, this.slotIn, cmd));
                }
            }
            this.slotIn++;
        }
    }

    private void perform(Command cmd) {
        for (long s = 0; s < this.slotOut; s++) {
            if (this.decisions.get(s) == cmd) {
                this.slotOut++;
                return;
            }
        }
        if (cmd instanceof ReconfigCommand) {
            this.slotOut++;
            return;
        }
        this.stateMachine.applyCommand(cmd);
        this.slotOut++;
    }

    public void run() {
        log.info("Starting replica {}", id);
        while (true) {
            var msg = environment.getNextReplicaMessage();
            log.info("Handling message: {}", msg);
            if (msg instanceof RequestMessage rm) {
                this.requests.add(rm.command());
            } else if (msg instanceof DecisionMessage dm) {
                this.decisions.put(dm.slotNumber(), dm.command());
                while (this.decisions.containsKey(this.slotOut)) {
                    if (this.proposals.containsKey(this.slotOut)) {
                        if (!this.proposals.get(this.slotOut).equals(this.decisions.get(this.slotOut))) {
                            this.requests.add(this.proposals.get(this.slotOut));
                        }
                        this.proposals.remove(this.slotOut);
                    }
                    this.perform(this.decisions.get(this.slotOut));
                }
            }
            this.propose();
        }
    }
}
