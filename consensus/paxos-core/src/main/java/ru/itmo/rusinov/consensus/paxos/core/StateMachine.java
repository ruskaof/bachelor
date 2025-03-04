package ru.itmo.rusinov.consensus.paxos.core;

import paxos.Paxos;
import java.io.File;

public interface StateMachine {

    void initialize(File stateMachineDir);
    Paxos.CommandResult applyCommand(Paxos.Command command);
}
