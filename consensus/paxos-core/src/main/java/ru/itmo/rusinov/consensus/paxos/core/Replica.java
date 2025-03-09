package ru.itmo.rusinov.consensus.paxos.core;

import ch.qos.logback.core.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import paxos.Paxos.Command;
import ru.itmo.rusinov.consensus.paxos.core.config.Config;
import ru.itmo.rusinov.consensus.paxos.core.environment.Environment;

import java.util.*;

@Slf4j
public class Replica {

    private final long WINDOW = 5;
    private final DurableStateStore durableStateStore;

    private long slotIn = 1;
    private long slotOut = 1;

    private final Queue<Command> requests = new LinkedList<>();
    private final Map<Long, Command> proposals = new HashMap<>();
    private final Map<Long, Command> decisions = new HashMap<>();

    private final Environment environment;
    private final StateMachine stateMachine;
    private final String id;

    private Config config;

    public Replica(Environment environment, StateMachine stateMachine, String id, Config config, DurableStateStore durableStateStore) {
        this.environment = environment;
        this.stateMachine = stateMachine;
        this.id = id;
        this.config = config;
        this.durableStateStore = durableStateStore;
    }

    private void propose() {
        while (this.slotIn < this.slotOut + WINDOW && !this.requests.isEmpty()) {
            if (this.slotIn > WINDOW && this.decisions.containsKey(this.slotIn - WINDOW)) {
                // todo handle reconfig command if needed
            }
            if (!decisions.containsKey(this.slotIn)) {
                var cmd = this.requests.poll();
                this.proposals.put(this.slotIn, cmd);
                for (String l : config.replicas()) {
                    var pm = Paxos.ProposeMessage.newBuilder()
                            .setSlotNumber(slotIn)
                            .setCommand(cmd);
                    var pmMessage = Paxos.PaxosMessage.newBuilder()
                            .setSrc(this.id)
                            .setPropose(pm)
                            .build();

                    this.environment.sendMessage(l, pmMessage);
                }
            }
            this.slotIn++;
        }
    }

    private void perform(Command cmd, boolean sendResponse) {
        for (long s = 0; s < this.slotOut; s++) {
            if (this.decisions.get(s) == cmd) {
                this.slotOut++;
                return;
            }
        }
        // todo for reconfig command
        var result = this.stateMachine.applyCommand(cmd);
        if (sendResponse) {
            this.environment.sendResponse(cmd.getClientId(), result);
        }
        this.slotOut++;

        this.durableStateStore.saveLastAppliedSlot(this.slotIn);
    }

    public void run() {
        log.info("Starting replica {}", id);
        while (true) {
            var msg = environment.getNextReplicaMessage();
            log.info("Handling message {} from {}", msg.getMessageCase(), msg.getSrc());

            switch (msg.getMessageCase()) {
                case REQUEST -> {
                    var rm = msg.getRequest();
                    this.requests.add(rm.getCommand());
                }
                case DECISION -> {
                    var dm = msg.getDecision();
                    this.decisions.put(dm.getSlotNumber(), dm.getCommand());
                    while (this.decisions.containsKey(this.slotOut)) {
                        boolean sendResponse = false;
                        if (this.proposals.containsKey(this.slotOut)) {
                            if (!this.proposals.get(this.slotOut).equals(this.decisions.get(this.slotOut))) {
                                this.requests.add(this.proposals.get(this.slotOut));
                            } else {
                                sendResponse = true;
                            }
                            this.proposals.remove(this.slotOut);
                        }
                        this.perform(this.decisions.get(this.slotOut), sendResponse);
                    }
                }
            }
            this.propose();
        }
    }
}
