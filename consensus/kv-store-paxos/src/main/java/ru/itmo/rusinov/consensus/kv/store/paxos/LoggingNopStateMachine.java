package ru.itmo.rusinov.consensus.kv.store.paxos;

import lombok.extern.slf4j.Slf4j;
import paxos.Paxos;
import ru.itmo.rusinov.consensus.paxos.core.StateMachine;
import ru.itmo.rusinov.consensus.paxos.core.command.CommandResult;

import java.io.File;

@Slf4j
public class LoggingNopStateMachine implements StateMachine {

    @Override
    public void initialize(File stateMachineDir) {
        log.info("Initialize {}", stateMachineDir);
    }

    @Override
    public CommandResult applyCommand(Paxos.Command command) {
        log.info("Apply command: {}", command);

        return null;
    }
}
