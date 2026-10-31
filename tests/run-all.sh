#!/bin/bash
cd "C:/Users/Administrator/Documents/deepseek-harness/default-workspace/dsh-engine" || exit 1
NODE="$HOME/.dsh/dsh-runtimes/dsh-primary-runtime/dependencies/node/bin/node.exe"
PY="$HOME/.dsh/dsh-runtimes/dsh-primary-runtime/dependencies/python/python.exe"
fail=0
run() {
  local name="$1"; shift
  echo "=== $name ==="
  "$@" > /tmp/p4test.out 2>&1
  local code=$?
  tail -4 /tmp/p4test.out
  if [ $code -ne 0 ]; then
    fail=1
    echo "FAILED: $name (exit $code)"
    tail -25 /tmp/p4test.out
  fi
}
"$NODE" scripts/build.mjs
run "python infra" "$PY" engine/python/tests/test_infra.py
run "python memory" "$PY" engine/python/tests/test_memory.py
run "python memory-change" "$PY" engine/python/tests/test_memory_change.py
run "python consumer" "$PY" engine/python/tests/test_consumer.py
run "python emotion-relocate" "$PY" engine/python/tests/test_emotion_relocate.py
run "python learning-relocate" "$PY" engine/python/tests/test_learning_relocate.py
run "python personality-relocate" "$PY" engine/python/tests/test_personality_relocate.py
run "python merge-consumers" "$PY" engine/python/tests/test_merge_consumers.py
run "python split-consumers" "$PY" engine/python/tests/test_split_consumers.py
run "python revise-consumers" "$PY" engine/python/tests/test_revise_consumers.py
run "python dispatcher" "$PY" engine/python/tests/test_dispatcher.py
run "python emotion" "$PY" engine/python/tests/test_emotion.py
run "python emotion-advance" "$PY" engine/python/tests/test_emotion_advance.py
run "python relationship" "$PY" engine/python/tests/test_relationship.py
run "python learning" "$PY" engine/python/tests/test_learning.py
run "python personality" "$PY" engine/python/tests/test_personality.py
run "python behavior" "$PY" engine/python/tests/test_behavior.py
run "python settlement" "$PY" engine/python/tests/test_settlement.py
run "python daily" "$PY" engine/python/tests/test_daily.py
run "node p3" "$NODE" tests/p3.mjs
run "node p4" "$NODE" tests/p4.mjs
run "node p5" "$NODE" tests/p5.mjs
run "node p6" "$NODE" tests/p6.mjs
run "node evidence" "$NODE" tests/evidence.mjs
run "node p8" "$NODE" tests/p8.mjs
run "node p9" "$NODE" tests/p9.mjs
run "node p10" "$NODE" tests/p10.mjs
run "node p11" "$NODE" tests/p11.mjs
run "node p12" "$NODE" tests/p12.mjs
run "node p13" "$NODE" tests/p13.mjs
run "node p14b" "$NODE" tests/p14b.mjs
run "node p14d" "$NODE" tests/p14d.mjs
run "node p14d1" "$NODE" tests/p14d1.mjs
run "node smoke" "$NODE" tests/smoke.mjs
run "node prestep" "$NODE" tests/prestep.mjs
run "node plugin-load" "$NODE" tests/plugin-load.mjs
echo "=== overall: $([ $fail -eq 0 ] && echo PASS || echo FAIL) ==="
exit $fail
