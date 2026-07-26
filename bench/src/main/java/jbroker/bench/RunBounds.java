package jbroker.bench;

/**
 * Measured-region bounds shared by the steady-state scenarios. A run is
 * duration-bounded (default 30 s) unless {@code --records} is given
 * without {@code --duration-s}; {@code --duration-s} always wins when
 * both are present.
 *
 * <p>The warmup phase is a real run against the same topic whose samples
 * never reach the histogram or the throughput counters. It defaults to
 * max(10 s, 20% of the measured duration) and is overridable via
 * {@code --warmup-s} (0 disables — smoke runs only; published rows must
 * keep the default).
 */
record RunBounds(long durationNanos, long recordBudget, long warmupNanos) {

    static RunBounds parse(String[] args) {
        return parse(args, 30, -1);
    }

    /**
     * @param defaultDurationS   measured duration when no bound flag is given
     *                           and {@code defaultRecordBudget} is negative
     * @param defaultRecordBudget when non-negative, the scenario is
     *                           records-bounded by default (seed-style
     *                           scenarios where wall-clock pacing makes no
     *                           sense)
     */
    static RunBounds parse(String[] args, int defaultDurationS, long defaultRecordBudget) {
        boolean durationSet = BenchArgs.get(args, "--duration-s", null) != null;
        boolean recordsSet = BenchArgs.get(args, "--records", null) != null;
        long durationNanos = -1;
        long budget = -1;
        if (durationSet || (!recordsSet && defaultRecordBudget < 0)) {
            durationNanos = BenchArgs.getInt(args, "--duration-s", defaultDurationS) * 1_000_000_000L;
        } else {
            budget = recordsSet ? BenchArgs.getLong(args, "--records", 0) : defaultRecordBudget;
        }
        int defaultWarmupS = durationNanos > 0 ? (int) Math.max(10, Math.ceil(durationNanos / 1e9 * 0.2)) : 10;
        long warmupNanos = BenchArgs.getInt(args, "--warmup-s", defaultWarmupS) * 1_000_000_000L;
        return new RunBounds(durationNanos, budget, warmupNanos);
    }

    boolean durationBounded() {
        return durationNanos > 0;
    }
}
