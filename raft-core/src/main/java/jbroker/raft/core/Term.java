package jbroker.raft.core;

public record Term(long value) implements Comparable<Term> {
    public static final Term ZERO = new Term(0);

    public Term {
        if (value < 0) {
            throw new IllegalArgumentException("term must be non-negative: " + value);
        }
    }

    public Term next() {
        return new Term(value + 1);
    }

    @Override
    public int compareTo(Term other) {
        return Long.compare(value, other.value);
    }
}
