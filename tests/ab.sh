#!/bin/bash
# ab.sh BIN_A BIN_B DATA [RUNS] [NCPU]: alternate A and B, report min/median ms per phase.
# Env: CPUS (taskset list, default 0-7 = 4 cores x 2 HT, like the scorer's 8 vCPUs),
#      ENV_A / ENV_B (extra "K=V K=V" for one side).
A=$1; B=$2; DATA=$3; RUNS=${4:-7}; N=${5:-8}; CPUS=${CPUS:-0-7}
OUT=$(mktemp -d)
for r in $(seq "$RUNS"); do
  for side in A B; do
    BIN=$A; EX=${ENV_A:-}; [ $side = B ] && BIN=$B && EX=${ENV_B:-}
    rm -rf "$OUT"/*
    env $EX SS_DEBUG=1 NCPU=$N taskset -c "$CPUS" "$BIN" "$DATA" "$OUT" 2>&1 | grep -v overlapped | awk -v s=$side '{t=$1; $1=""; $2=""; sub(/^ +/,""); d=t-prev; prev=t; print s "\t" $0 "\t" d}; END {print s "\ttotal\t" prev}'
  done
done | awk -F'\t' '
  { k=$1 SUBSEP $2; v[k]=v[k] " " $3; if (!($2 in seen)) { seen[$2]=1; order[++n]=$2 } }
  function stats(str,   a, m, i, j, t) { m=split(str, a, " "); for (i=1;i<=m;i++) for (j=i+1;j<=m;j++) if (a[j]+0<a[i]+0) {t=a[i];a[i]=a[j];a[j]=t}
    return sprintf("min %7.1f  med %7.1f", a[1], a[int((m+1)/2)]) }
  END { printf "%-22s %-28s %-28s\n", "phase", "A", "B"
        for (i=1;i<=n;i++) printf "%-22s %-28s %-28s\n", order[i], stats(v["A" SUBSEP order[i]]), stats(v["B" SUBSEP order[i]]) }'
rm -rf "$OUT"
