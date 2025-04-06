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

    private static final long WINDOW = 5;
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
            if (!decisions.containsKey(this.slotIn)) {
                log.info("Proposing with slotIn={}, slotOut={}", slotIn, slotOut);
                var cmd = this.requests.poll();
                this.proposals.put(this.slotIn, cmd);
                var pm = Paxos.ProposeMessage.newBuilder()
                        .setSlotNumber(slotIn)
                        .setCommand(cmd);
                var pmMessage = Paxos.PaxosMessage.newBuilder()
                        .setSrc(this.id)
                        .setPropose(pm)
                        .build();
                this.environment.sendMessageToColocatedLeader(pmMessage);
            }
            this.slotIn++;
        }
    }

    private void perform(Command cmd, boolean sendResponse) {
        for (long s = 1; s < this.slotOut; s++) {
            if (cmd.equals(this.decisions.get(s))) {
                this.slotOut++;
                return;
            }
        }
        var result = this.stateMachine.applyCommand(cmd);
        if (sendResponse) {
            log.info("Sending response for {}", cmd.getRequestId());
            this.environment.sendResponse(UUID.fromString(cmd.getRequestId()), result.toByteArray());
        }
        this.slotOut++;

        if (slotOut > 1) {
            this.durableStateStore.saveLastAppliedSlot(this.slotOut - 1);
        }
    }

    public void run() {
        log.info("Starting replica {}", id);
        while (true) {
            var request = environment.getNextReplicaMessage();
            var msg = request.message();
            log.info("Handling message {} from {}", msg.getMessageCase(), msg.getSrc());

            switch (msg.getMessageCase()) {
                case REQUEST -> {
                    var rm = msg.getRequest();
                    var command = Paxos.Command.newBuilder()
                            .setContent(rm.getCommand().getContent())
                            .setRequestId(request.requestId().toString())
                            .build();
                    this.requests.add(command);
                }
                case DECISION -> {
                    var dm = msg.getDecision();
                    log.info("Got decision for slot {}, slotOut={}, slotIn={}", dm.getSlotNumber(), slotOut, slotIn);
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
                case INACTIVE -> {
                    var im = msg.getInactive();
                    for (var r : requests) {
                        var result = Paxos.CommandResult.newBuilder()
                                .setSuggestedLeader(im.getSuggestedLeader())
                                .build();
                        environment.sendResponse(UUID.fromString(r.getRequestId()), result.toByteArray());
                    }
                    requests.clear();

                    for (var r: proposals.values()) {
                        var result = Paxos.CommandResult.newBuilder()
                                .setSuggestedLeader(im.getSuggestedLeader())
                                .build();
                        environment.sendResponse(UUID.fromString(r.getRequestId()), result.toByteArray());
                    }
                    proposals.clear();
                }
            }
            this.propose();
            logRequests();
        }
    }

    private void logRequests() {
        log.info("Requests: {}. Proposals: {}. SlotIn: {}. SlotOut: {}", requests.size(), proposals.size(), slotIn, slotOut);
    }
}
