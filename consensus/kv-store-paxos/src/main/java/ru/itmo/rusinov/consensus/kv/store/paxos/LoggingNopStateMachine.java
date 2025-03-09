package ru.itmo.rusinov.consensus.kv.store.paxos;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import ru.itmo.rusinov.consensus.paxos.core.StateMachine;

import java.io.File;
import java.nio.charset.StandardCharsets;

@Slf4j
public class LoggingNopStateMachine implements StateMachine {

    @Override
    public void initialize(File stateMachineDir) {
        log.info("Initialize {}", stateMachineDir);
    }

    @Override
    public Paxos.CommandResult applyCommand(Paxos.Command command) {
        log.info("Apply command: {}", command);

        return Paxos.CommandResult.newBuilder().setContent(ByteString.copyFrom("OK".getBytes(StandardCharsets.UTF_8))).build();
    }
}
