package com.pulseq.raft;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Scenario tests: one named failure each, on a simulated cluster. */
class RaftSimulationTest {

    @Test
    void electsOneLeaderAndTheOthersFollowIt() {
        SimCluster cluster = new SimCluster(3, 1);
        RaftNode leader = cluster.awaitLeader();
        cluster.awaitConvergence();

        for (int id : cluster.ids()) {
            assertEquals(leader.id(), cluster.node(id).leaderId());
            assertEquals(leader.currentTerm(), cluster.node(id).currentTerm());
        }
        long term = leader.currentTerm();
        cluster.run(500);
        assertEquals(term, cluster.leader().currentTerm(), "a healthy cluster must not re-elect");
    }

    @Test
    void singleNodeCommitsOnItsOwn() {
        SimCluster cluster = new SimCluster(1, 2);
        cluster.awaitLeader();
        String command = cluster.propose(0);
        assertEquals(List.of(command), cluster.applied(0));
    }

    @Test
    void followerRefusesProposals() {
        SimCluster cluster = new SimCluster(3, 3);
        RaftNode leader = cluster.awaitLeader();
        int follower = (leader.id() + 1) % 3;
        assertNull(cluster.propose(follower));
    }

    @Test
    void replicatesCommandsInOrderToEveryNode() {
        SimCluster cluster = new SimCluster(5, 4);
        List<String> sent = proposeMany(cluster, 50);
        cluster.awaitConvergence();

        for (int id : cluster.ids()) {
            assertEquals(sent, cluster.applied(id), "node " + id);
        }
    }

    @Test
    void keepsCommittedCommandsWhenTheLeaderCrashes() {
        SimCluster cluster = new SimCluster(3, 5);
        List<String> sent = proposeMany(cluster, 10);
        cluster.awaitConvergence();
        int oldLeader = cluster.leader().id();

        cluster.crash(oldLeader);
        RaftNode newLeader = cluster.awaitLeader();
        assertNotEquals(oldLeader, newLeader.id());
        sent.addAll(proposeMany(cluster, 10));
        cluster.awaitConvergence();
        assertEquals(sent, cluster.applied(newLeader.id()));

        cluster.restart(oldLeader);
        cluster.awaitConvergence();
        assertEquals(sent, cluster.applied(oldLeader), "the old leader catches up as a follower");
    }

    @Test
    void leaderCutOffFromTheMajorityCannotCommit() {
        SimCluster cluster = new SimCluster(5, 6);
        List<String> sent = proposeMany(cluster, 5);
        cluster.awaitConvergence();
        int isolated = cluster.leader().id();
        List<Integer> rest = new ArrayList<>(cluster.ids());
        rest.remove((Integer) isolated);

        cluster.partition(List.of(List.of(isolated), rest));
        List<String> lost = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            lost.add(cluster.propose(isolated)); // accepted by the stale leader, never committed
        }
        cluster.runUntil("the majority to elect a leader", 2_000,
                () -> cluster.leader() != null && cluster.leader().id() != isolated);
        int majorityLeader = cluster.leader().id();
        for (int i = 0; i < 5; i++) {
            sent.add(cluster.propose(majorityLeader));
        }
        cluster.run(200);
        assertEquals(sent.subList(0, 5), cluster.applied(isolated), "nothing new commits in the minority");

        cluster.heal();
        cluster.awaitConvergence();
        for (int id : cluster.ids()) {
            assertEquals(sent, cluster.applied(id), "node " + id);
            assertFalse(cluster.applied(id).stream().anyMatch(lost::contains));
        }
    }

    @Test
    void noLeaderWithoutAMajority() {
        SimCluster cluster = new SimCluster(3, 7);
        int leader = cluster.awaitLeader().id();
        cluster.crash(leader);
        cluster.crash((leader + 1) % 3);

        cluster.run(1_000);
        assertNull(cluster.leader(), "one node out of three must never lead");
    }

    @Test
    void restartedFollowerCatchesUp() {
        SimCluster cluster = new SimCluster(3, 8);
        int follower = (cluster.awaitLeader().id() + 1) % 3;
        cluster.crash(follower);
        List<String> sent = proposeMany(cluster, 200); // more than one AppendEntries batch
        cluster.awaitConvergence();

        cluster.restart(follower);
        cluster.awaitConvergence();
        assertEquals(sent, cluster.applied(follower));
    }

    @Test
    void wholeClusterRestartKeepsTheLog() {
        SimCluster cluster = new SimCluster(3, 9);
        List<String> sent = proposeMany(cluster, 20);
        cluster.awaitConvergence();

        cluster.ids().forEach(cluster::crash);
        cluster.ids().forEach(cluster::restart);
        cluster.awaitConvergence();
        for (int id : cluster.ids()) {
            assertEquals(sent, cluster.applied(id), "node " + id);
        }
    }

    @Test
    void survivesANetworkThatDropsDuplicatesAndReorders() {
        SimCluster cluster = new SimCluster(5, 10);
        cluster.dropRate = 0.2;
        cluster.duplicateRate = 0.1;
        cluster.maxDelay = 6;

        List<String> committedOrder = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            RaftNode leader = cluster.awaitLeader();
            String command = cluster.propose(leader.id());
            assertNotNull(command);
            cluster.run(3);
        }
        cluster.dropRate = 0;
        cluster.awaitConvergence();

        committedOrder.addAll(cluster.committedCommands());
        assertTrue(committedOrder.size() <= 100);
        assertEquals(committedOrder.size(), new HashSet<>(committedOrder).size(), "no command twice");
        for (int id : cluster.ids()) {
            assertEquals(committedOrder, cluster.applied(id), "node " + id);
        }
    }

    /**
     * The scenario of Figure 8 in the Raft paper. A leader must not treat an entry from an
     * earlier term as committed just because a majority now holds it: a node that never saw
     * the entry can still win the next election and overwrite it. Election timers are
     * disabled so the test decides who campaigns, and entries are sent one at a time so a
     * follower can hold the old entry without the new leader's own entry.
     */
    @Test
    void entryFromAnEarlierTermIsNotCommittedByCountingReplicas() {
        SimCluster cluster = new SimCluster(5, 11, new RaftConfig(100_000, 100_000, 2, 1));
        cluster.campaign(0);
        cluster.awaitConvergence();
        assertEquals(1, cluster.node(0).currentTerm());

        // Term 1: leader 0 replicates X to node 1 only, then crashes.
        cluster.partition(List.of(List.of(0, 1), List.of(2), List.of(3), List.of(4)));
        String x = cluster.propose(0);
        cluster.run(10);
        assertEquals(2, cluster.node(1).lastLogIndex());
        assertEquals(1, cluster.node(0).commitIndex(), "X is on two of five nodes: not committed");
        cluster.crash(0);

        // Term 2: node 4 wins with votes from 2 and 3, writes its own entry at index 2, and
        // crashes before sending it to anyone.
        cluster.partition(List.of(List.of(0), List.of(1), List.of(2, 3, 4)));
        cluster.campaign(4);
        cluster.runUntil("node 4 to lead", 50, () -> cluster.node(4).isLeader());
        cluster.partition(List.of(List.of(0), List.of(1), List.of(2), List.of(3), List.of(4)));
        cluster.run(5);
        assertEquals(2, cluster.node(4).currentTerm());
        assertEquals(1, cluster.node(2).lastLogIndex());
        cluster.crash(4);

        // Term 3: node 0 returns, wins, and copies X to nodes 2 and 3. X is now on a majority
        // (0, 1, 2, 3), but node 0 crashes before its own term-3 entry reaches anyone.
        cluster.restart(0);
        cluster.partition(List.of(List.of(0, 2, 3), List.of(1), List.of(4)));
        cluster.campaign(0); // term 2: refused, nodes 2 and 3 already voted for node 4
        cluster.run(6);
        cluster.campaign(0);
        cluster.runUntil("X to reach nodes 2 and 3", 50,
                () -> cluster.node(2).lastLogIndex() == 2 && cluster.node(3).lastLogIndex() == 2);
        cluster.step(); // node 0 receives both acknowledgements
        assertTrue(cluster.node(0).isLeader());
        assertEquals(3, cluster.node(0).currentTerm());
        assertTrue(cluster.node(0).commitIndex() < 2, "X is from term 1: holding it on a majority is not enough");
        cluster.crash(0);
        cluster.partition(List.of(List.of(0), List.of(1, 2, 3, 4)));

        // Term 4: node 4 returns. Its log ends in term 2, newer than X, so it wins and
        // replaces X everywhere. That is only safe because X was never committed.
        cluster.restart(4);
        cluster.campaign(4); // term 3: refused, nodes 2 and 3 already voted for node 0
        cluster.run(6);
        cluster.campaign(4);
        cluster.runUntil("node 4 to lead again", 50, () -> cluster.node(4).isLeader());
        assertEquals(4, cluster.node(4).currentTerm());

        cluster.restart(0);
        cluster.heal();
        String y = cluster.propose(4);
        cluster.awaitConvergence();
        for (int id : cluster.ids()) {
            assertEquals(List.of(y), cluster.applied(id), "node " + id);
        }
        assertFalse(cluster.committedCommands().contains(x));
    }

    private static List<String> proposeMany(SimCluster cluster, int count) {
        List<String> sent = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String command = cluster.propose(cluster.awaitLeader().id());
            assertNotNull(command);
            sent.add(command);
            cluster.step();
        }
        return sent;
    }
}
