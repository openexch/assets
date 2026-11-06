# Assets Engine — build + determinism targets

.PHONY: build build-java test determinism update-goldens integration nightly clean

build build-java:
	mvn clean package -DskipTests

test:
	mvn test

# Fast, in-process determinism + snapshot-replay + conservation + recovery suite (the gate).
determinism:
	mvn -pl assets-cluster -am test -Dtest='DeterminismCorpusTest,EngineSnapshotReplayTest,ConservationInvariantTest,BalanceSnapshotCodecTest,WireRoundTripTest,ReplayRecoveryTest,ConservationFuzzTest,TotalLossRebuildTest,SnapshotChunkingTransportTest,AssetsNodeMetricsTest' -Dsurefire.failIfNoSpecifiedTests=false

# Regenerate determinism golden files after an INTENTIONAL output change (review the diff, then commit).
update-goldens:
	mvn -pl assets-cluster -am test -Dtest=DeterminismCorpusTest -Dsurefire.failIfNoSpecifiedTests=false -DargLine="-Dupdate.golden=true"

# Integration: boot a real single-node embedded-driver AE cluster in-process and drive it over the
# wire. Kept OUT of the fast `determinism` gate because it launches a cluster. On a box already running
# the live money stack, PIN it off the matching engine's cores, e.g.:
#   taskset -c 20-23 mvn -pl assets-cluster test -Dtest=ClusterBootSmokeTest
integration:
	mvn -pl assets-cluster -am test -Dtest='ClusterBootSmokeTest,SnapshotCadenceClusterTest,SnapshotChunkedRestoreClusterTest,NodeMetricsClusterTest,TimerSurvivalClusterTest' -Dsurefire.failIfNoSpecifiedTests=false

# RC1 S8 nightly: the heavy harness behind flags (harness B9). The 100k cluster restore is the RC1-A05 evidence
# (restore + replay -> ready <= 30 s, corrupt CRC refused); the 1M sizing and the 30k-line strided splice are reports.
nightly:
	mvn -pl assets-cluster -am test -Dtest='CoreChunkedRestore100kClusterTest,CoreSnapshotSizingTest,ProductionShapeEquivalenceTest,SweepSurvivesRestoreClusterTest,SweepSurvivesLeaderChangeClusterTest' -Dsurefire.failIfNoSpecifiedTests=false -Dcore.restore.100k=true -Dcore.sizing.million=true -Dcore.scenario.large=true

clean:
	mvn clean
