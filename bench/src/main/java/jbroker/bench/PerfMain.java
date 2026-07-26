package jbroker.bench;

import java.util.Arrays;

/**
 * Entry point for the perf harness. Dispatches on the first argument.
 *
 * <pre>
 *   j-broker-bench producer       --broker H:P --topic T [--duration-s S | --records N]
 *   j-broker-bench batch-producer --broker H:P --topic T [--batch-bytes N] [--linger-ms MS]
 *   j-broker-bench produce-batch  --broker H:P --topic T [--batch-records N]
 *   j-broker-bench consumer       --broker H:P --topic T [--max-bytes BYTES]
 *   j-broker-bench acks-all       [--acks all|1] [--partitions N] [--min-insync N]
 *                                 [--bootstrap H:P,H:P,...]
 *   j-broker-bench replication    [--records N] [--payload-size B]
 *   j-broker-bench compaction     [--records N] [--keys K]
 *   j-broker-bench create-topic   --broker H:P --topic T [--config key=value]...
 * </pre>
 *
 * <p>All steady-state modes share [--duration-s S | --records N]
 * [--warmup-s S] [--payload-size B] [--csv FILE]; see each scenario's
 * javadoc for the full flag list and its latency semantics.
 * {@code producer}/{@code batch-producer}/{@code produce-batch}/
 * {@code consumer} talk to an already-running broker;
 * {@code acks-all}/{@code replication}/{@code compaction} start an
 * in-process cluster themselves ({@code acks-all} can target an external
 * one via {@code --bootstrap}).
 *
 * <p>Every mode prints a percentile/throughput summary; {@code --csv
 * FILE} appends one schema'd row per histogram (see PerfReport) so
 * multiple calls aggregate into a single series.
 */
public final class PerfMain {

    private PerfMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            System.exit(2);
        }
        var rest = Arrays.copyOfRange(args, 1, args.length);
        switch (args[0]) {
            case "producer" -> ProducerPerfTest.run(rest);
            case "batch-producer" -> BatchProducerPerfTest.run(rest);
            case "produce-batch" -> ProduceBatchPerfTest.run(rest);
            case "consumer" -> ConsumerPerfTest.run(rest);
            case "acks-all" -> AcksAllPerfTest.run(rest);
            case "replication" -> ReplicationPerfTest.run(rest);
            case "compaction" -> CompactionPausePerfTest.run(rest);
            case "create-topic" -> CreateTopicCmd.run(rest);
            case "check-batch" -> BatchCorrectness.run(rest);
            case "soak-produce" -> SoakRun.produce(rest);
            case "soak-verify" -> SoakRun.verify(rest);
            default -> {
                usage();
                System.exit(2);
            }
        }
    }

    private static void usage() {
        System.err.println("Usage: j-broker-bench <mode> [flags]");
        System.err.println("  mode: producer | batch-producer | produce-batch | consumer");
        System.err.println("        | acks-all | replication | compaction | create-topic");
        System.err.println("        | soak-produce | soak-verify   (scenario-chaos-with-load.sh)");
        System.err.println("  see class javadoc for per-mode flags");
    }
}
