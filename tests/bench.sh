#!/bin/bash
# bench.sh BIN DATA [RUNS] [NCPU] [CPUSET]: min time per phase over RUNS runs.
BIN=$1; DATA=$2; RUNS=${3:-5}; N=${4:-1}; CPUS=${5:-}
OUT=$(mktemp -d)
for r in $(seq "$RUNS"); do
  rm -rf "$OUT"/*
  if [ -n "$CPUS" ]; then
    SS_DEBUG=1 NCPU=$N taskset -c "$CPUS" "$BIN" "$DATA" "$OUT" 2>&1
  else
    SS_DEBUG=1 NCPU=$N "$BIN" "$DATA" "$OUT" 2>&1
  fi | awk -v r="$r" '{t=$1; $1=""; $2=""; sub(/^ +/, ""); print r "\t" $0 "\t" t}'
done | awk -F'\t' '
  { if (!($2 in prev_seen)) { order[++n]=$2; prev_seen[$2]=1 }
    key=$1; d=$3-last[key]; last[key]=$3
    if (!($2 in best) || d < best[$2]) best[$2]=d
    if (!($2 in tot) || $3 < tot[$2]) tot[$2]=$3 }
  END { for (i=1;i<=n;i++) printf "%-28s %8.1f ms   (cum min %8.1f)\n", order[i], best[order[i]], tot[order[i]] }'
rm -rf "$OUT"
