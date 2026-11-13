#!/usr/bin/env bash
# Collects the failing test classes' JVM diagnostics after a failed Maven test step.
#
# The ci-diagnostics profile writes each forked test JVM's GC and safepoint log (target/jvm-<pid>.log, at most 5 MB
# plus one rotated file) and core-cluster's pid-to-class map (target/jvm-pids.tsv). Only the failing classes' logs,
# their surefire reports and their outputs are copied, into jvm-diagnostics/<module>/<class>/.
set -u
mkdir -p jvm-diagnostics
for rep in $(grep -lE '<(failure|error)[ >]' */target/surefire-reports/TEST-*.xml 2>/dev/null); do
  mod=${rep%%/target/*}
  cls=$(basename "$rep" .xml); cls=${cls#TEST-}
  dest="jvm-diagnostics/$mod/$cls"
  mkdir -p "$dest"
  cp "$rep" "$dest/"
  cp "$mod/target/surefire-reports/$cls-output.txt" "$dest/" 2>/dev/null || true
  if [ -f "$mod/target/jvm-pids.tsv" ]; then
    for pid in $(awk -F'\t' -v c="$cls" '$2 == c { print $1 }' "$mod/target/jvm-pids.tsv"); do
      cp "$mod"/target/jvm-"$pid".log* "$dest/" 2>/dev/null || true
    done
  fi
done
du -sh jvm-diagnostics || true
