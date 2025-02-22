package ru.itmo.rusinov.consensus.kv.store.paxos;

import lombok.extern.slf4j.Slf4j;
import ru.itmo.rusinov.consensus.paxos.core.StateMachine;
import ru.itmo.rusinov.consensus.paxos.core.command.Command;
import ru.itmo.rusinov.consensus.paxos.core.command.CommandResult;

@Slf4j
public class LoggingNopStateMachine implements StateMachine {
    @Override
    public CommandResult applyCommand(Command command) {
        log.info("Apply command: {}", command);
        return null;
    }
}
