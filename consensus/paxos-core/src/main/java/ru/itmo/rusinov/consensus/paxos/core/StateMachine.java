package ru.itmo.rusinov.consensus.paxos.core;

import paxos.Paxos;
import ru.itmo.rusinov.consensus.paxos.core.command.CommandResult;

import java.io.File;

public interface StateMachine {

    void initialize(File stateMachineDir);
    CommandResult applyCommand(Paxos.Command command);
}
