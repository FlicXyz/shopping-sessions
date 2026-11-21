#!/bin/bash
# gates.sh: build ~/ss/bin/pipeline from src/main.cpp and run correctness gates 1-5.
# Prints one line per gate; exits non-zero at the first failure.
set -u
REPO=/mnt/c/Users/user/dev/shopping-sessions
SS=$HOME/ss
PY=$HOME/venv/bin/python
cd "$REPO" || exit 1
W=$(clang++-18 -O3 -std=c++17 -static -march=x86-64-v3 -fno-stack-protector -Wall -Wextra -pthread -o "$SS/bin/pipeline" src/main.cpp 2>&1 | grep -cE "error|warning")
[ -x "$SS/bin/pipeline" ] && [ "$W" = 0 ] || { echo "G1 build: FAIL ($W errors/warnings)"; exit 1; }
echo "G1 build: ok"
SS_SELFTEST=5 "$SS/bin/pipeline" 2>"$SS/st.txt"; R=$(tail -1 "$SS/st.txt")
[ "$R" = "selftest: 0 mismatches" ] || { echo "G2 selftest: FAIL ($R)"; exit 1; }
echo "G2 selftest: ok"
A=$($PY scripts/run_fixtures.py fixtures -- "$SS/bin/pipeline" 2>&1 | tail -1)
B=$(SS_GENERIC=1 $PY scripts/run_fixtures.py fixtures -- "$SS/bin/pipeline" 2>&1 | tail -1)
[ "$A" = "36/36 fixtures passed" ] && [ "$B" = "36/36 fixtures passed" ] || { echo "G3 fixtures: FAIL (fast: $A | generic: $B)"; exit 1; }
echo "G3 fixtures: ok (fast + generic)"
rm -rf "$SS/g_out"; "$SS/bin/pipeline" "$SS/dev" "$SS/g_out" 2>/dev/null
R=$($PY scripts/compare.py "$SS/g_out" dev-answer | tail -1)
[ "$R" = "MATCH" ] || { echo "G4 dev: FAIL"; $PY scripts/compare.py "$SS/g_out" dev-answer | head -8; exit 1; }
echo "G4 dev: ok"
for pair in "d20 o20v1" "d20s11 r20s11" "d20s23 r20s23"; do
  set -- $pair
  rm -rf "$SS/g_out"; NCPU=8 "$SS/bin/pipeline" "$SS/$1" "$SS/g_out" 2>/dev/null
  R=$($PY scripts/compare.py "$SS/g_out" "$SS/$2" | tail -1)
  [ "$R" = "MATCH" ] || { echo "G5 $1: FAIL"; $PY scripts/compare.py "$SS/g_out" "$SS/$2" | head -8; exit 1; }
done
echo "G5 seeds 7/11/23 vs v1: ok"
