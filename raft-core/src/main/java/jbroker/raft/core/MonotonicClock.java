package jbroker.raft.core;

public final class MonotonicClock implements Clock {
    @Override
    public long nanoTime() {
        return System.nanoTime();
    }
}
