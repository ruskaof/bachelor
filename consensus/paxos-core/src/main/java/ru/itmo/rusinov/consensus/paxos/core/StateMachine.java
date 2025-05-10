package ru.itmo.rusinov.consensus.paxos.core;

import java.io.File;

public interface StateMachine {

    void initialize(File stateMachineDir);
    byte[] applyCommand(byte[] command);
}
