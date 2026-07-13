#!/bin/bash
# Restart the worker-harness service when the restart-trigger file appears.
# Invoked by worker-harness-restart.service, which is itself triggered by
# worker-harness-restart.path.
set -euo pipefail

TRIGGER="$HOME/.local/worker-harness/harness/restart-trigger"
if [ -f "$TRIGGER" ]; then
  rm -f "$TRIGGER"
  systemctl --user restart worker-harness.service
fi
