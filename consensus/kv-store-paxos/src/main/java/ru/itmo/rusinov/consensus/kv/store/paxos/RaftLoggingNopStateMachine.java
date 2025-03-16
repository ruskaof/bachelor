package ru.itmo.rusinov.consensus.kv.store.paxos;

import lombok.extern.slf4j.Slf4j;
import ru.itmo.rusinov.consensus.raft.core.StateMachine;

import java.io.File;

@Slf4j
public class RaftLoggingNopStateMachine implements StateMachine {
    @Override
    public void initialize(File stateMachineDir) {
        log.info("Initialize {}", stateMachineDir);
    }

    @Override
    public byte[] applyCommand(byte[] command) {
        log.info("Apply command: {}", command);
        return new byte[0];
    }
}
