package ru.itmo.rusinov.consensus.paxos.core;

import ru.itmo.rusinov.consensus.paxos.core.command.Command;
import ru.itmo.rusinov.consensus.paxos.core.command.CommandResult;

public interface StateMachine {

    CommandResult applyCommand(Command command);
}
