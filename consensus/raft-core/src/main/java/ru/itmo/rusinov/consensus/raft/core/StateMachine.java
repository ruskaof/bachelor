package ru.itmo.rusinov.consensus.raft.core;

import java.io.File;

public interface StateMachine {
    void initialize(File stateMachineDir);

    byte[] applyCommand(byte[] command);
}
