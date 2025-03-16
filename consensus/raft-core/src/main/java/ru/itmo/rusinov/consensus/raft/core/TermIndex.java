package ru.itmo.rusinov.consensus.raft.core;

import java.util.Comparator;

public record TermIndex(
        long term,
        long index
) implements Comparable<TermIndex> {

    private static final Comparator<TermIndex> comparator = Comparator.comparing(TermIndex::term)
            .thenComparing(TermIndex::index);

    @Override
    public int compareTo(TermIndex o) {
        return comparator.compare(this, o);
    }
}
