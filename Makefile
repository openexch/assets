# Assets Engine — build + determinism targets

.PHONY: build build-java test determinism update-goldens clean

build build-java:
	mvn clean package -DskipTests

test:
	mvn test

# Fast, in-process determinism + snapshot-replay + conservation + recovery suite (the gate).
determinism:
	mvn -pl assets-cluster -am test -Dtest='DeterminismCorpusTest,EngineSnapshotReplayTest,ConservationInvariantTest,BalanceSnapshotCodecTest,WireRoundTripTest,ReplayRecoveryTest,ConservationFuzzTest,TotalLossRebuildTest' -Dsurefire.failIfNoSpecifiedTests=false

# Regenerate determinism golden files after an INTENTIONAL output change (review the diff, then commit).
update-goldens:
	mvn -pl assets-cluster -am test -Dtest=DeterminismCorpusTest -Dsurefire.failIfNoSpecifiedTests=false -DargLine="-Dupdate.golden=true"

# Integration: boot a real single-node embedded-driver AE cluster in-process and drive it over the
# wire. Kept OUT of the fast `determinism` gate because it launches a cluster. On a box already running
# the live money stack, PIN it off the matching engine's cores, e.g.:
#   taskset -c 20-23 mvn -pl assets-cluster test -Dtest=ClusterBootSmokeTest
integration:
	mvn -pl assets-cluster -am test -Dtest='ClusterBootSmokeTest' -Dsurefire.failIfNoSpecifiedTests=false

clean:
	mvn clean
