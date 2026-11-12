package com.pulseq.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Random fault schedules. Each seed runs a cluster through crashes, restarts, partitions and
 * a lossy network while clients keep proposing, with the safety checks of {@link SimCluster}
 * active on every tick. Then the faults stop and the cluster must recover and agree.
 *
 * <p>To replay a failure, run {@code runSchedule(size, seed)} with the seed from the message.</p>
 */
class RaftFuzzTest {

    private static final int SEEDS = Integer.getInteger("raft.fuzz.seeds", 300);
    private static final int CHAOS_TICKS = 3_000;

    @Test
    void threeNodesSurviveRandomFaults() {
        for (long seed = 0; seed < SEEDS; seed++) {
            runSchedule(3, seed);
        }
    }

    @Test
    void fiveNodesSurviveRandomFaults() {
        for (long seed = 0; seed < SEEDS; seed++) {
            runSchedule(5, 1_000_000 + seed);
        }
    }

    /** The same schedules with the log on disk: every crash closes the files, every restart reopens them. */
    @Test
    void threeNodesWithLogsOnDiskSurviveRandomFaults(@TempDir Path directory) {
        for (long seed = 0; seed < Math.max(1, SEEDS / 15); seed++) {
            Path run = directory.resolve("seed-" + seed);
            runSchedule(new SimCluster(3, 2_000_000 + seed, RaftConfig.defaults(),
                    id -> new FileRaftStorage(run.resolve("node-" + id))), 2_000_000 + seed);
        }
    }

    static void runSchedule(int size, long seed) {
        runSchedule(new SimCluster(size, seed), seed);
    }

    static void runSchedule(SimCluster cluster, long seed) {
        int size = cluster.ids().size();
        Random chaos = new Random(seed ^ 0x5DEECE66DL);
        cluster.dropRate = chaos.nextDouble() * 0.3;
        cluster.duplicateRate = chaos.nextDouble() * 0.2;
        cluster.maxDelay = 1 + chaos.nextInt(8);

        for (int tick = 0; tick < CHAOS_TICKS; tick++) {
            randomEvent(cluster, chaos);
            cluster.step();
        }

        // Faults stop: the cluster must elect a leader and every node must reach the same state.
        cluster.heal();
        cluster.dropRate = 0;
        cluster.duplicateRate = 0;
        cluster.ids().forEach(cluster::restart);
        cluster.awaitConvergence();
        cluster.checkLogMatching();

        List<String> committed = cluster.committedCommands();
        assertEquals(committed.size(), new HashSet<>(committed).size(),
                "seed " + seed + ": a command was committed twice");
        for (int id : cluster.ids()) {
            assertEquals(committed, cluster.applied(id), "seed " + seed + ", node " + id);
        }
    }

    private static void randomEvent(SimCluster cluster, Random chaos) {
        int event = chaos.nextInt(1_000);
        int id = chaos.nextInt(cluster.ids().size());
        if (event < 4) {
            cluster.crash(id);
        } else if (event < 10) {
            cluster.restart(id);
        } else if (event < 13) {
            cluster.partition(randomSplit(cluster.ids(), chaos));
        } else if (event < 17) {
            cluster.heal();
        } else if (event < 200) {
            cluster.propose(id); // any node, including followers and stale leaders
        }
    }

    private static List<List<Integer>> randomSplit(List<Integer> ids, Random chaos) {
        List<Integer> shuffled = new ArrayList<>(ids);
        Collections.shuffle(shuffled, chaos);
        int cut = 1 + chaos.nextInt(shuffled.size() - 1);
        return List.of(shuffled.subList(0, cut), shuffled.subList(cut, shuffled.size()));
    }
}
