#!/bin/bash
# Swap a new worker-harness SIF into place and restart the service.
# Invoked by worker-harness-update.service, which is itself triggered by
# worker-harness-update.path when new-image.sif appears in the drop zone.
#
# Validation strategy: don't try to find the container runtime at all
# (its path varies across workers). Just swap the SIF and restart the
# service, then poll `systemctl --user is-active worker-harness.service`
# for a few seconds. If the service comes up, the SIF is good. If not,
# roll back to the previous one.
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin${PATH:+:$PATH}"

NEW="$HOME/.local/worker-harness/harness/new-image.sif"
CUR="$HOME/worker-harness/worker-harness-worker.sif"
LOG="$HOME/.local/worker-harness/harness/update.log"

{
  echo "--- $(date -Is) update.sh start ---"
  echo "PATH=$PATH"
} >>"$LOG"

if [ ! -f "$NEW" ]; then
  echo "no new image, exiting" >>"$LOG"
  exit 0
fi

# Backup current image, then move new into place.
if [ -f "$CUR" ]; then
  mv -f "$CUR" "${CUR}.old"
fi
mv -f "$NEW" "$CUR"
echo "moved new image into place" >>"$LOG"

# Restart the service with the new SIF.
systemctl --user restart worker-harness.service
echo "restart issued" >>"$LOG"

# A single `active` poll races a Type=simple service: systemd marks the
# wrapper active before its entrypoint has necessarily survived startup.
# An Apptainer instance with no valid %startscript can also leave only appinit
# alive forever while systemd still reports active. Require both the service
# and the actual worker daemon to remain live for ten uninterrupted polls.
# This preserves .old for images that start an empty/broken instance.
HEALTHY=no
ACTIVE_STREAK=0
for attempt in $(seq 1 20); do
  sleep 1
  state=$(systemctl --user is-active worker-harness.service 2>/dev/null || true)
  daemon_pid=$(pgrep -f '^python3? /worker_daemon.py$' | head -n 1 || true)
  if [ "$state" = "active" ] && [ -n "$daemon_pid" ]; then
    ACTIVE_STREAK=$((ACTIVE_STREAK + 1))
  else
    ACTIVE_STREAK=0
  fi
  echo "  poll $attempt: state=$state daemon=${daemon_pid:-missing} active_streak=$ACTIVE_STREAK/10" >>"$LOG"
  if [ "$ACTIVE_STREAK" -ge 10 ]; then
    HEALTHY=yes
    break
  fi
done

if [ "$HEALTHY" = "yes" ]; then
  echo "service stayed active for 10 seconds — new SIF is good" >>"$LOG"
  rm -f "${CUR}.old"
  exit 0
fi

# Service did not come up. The new SIF is bad — keep it for debugging
# and restore the previous one.
echo "service failed to reach active state — rolling back" >>"$LOG"
mv -f "$CUR" "${CUR}.failed"
if [ -f "${CUR}.old" ]; then
  mv -f "${CUR}.old" "$CUR"
  systemctl --user restart worker-harness.service
  echo "rolled back, restarted with previous SIF" >>"$LOG"
fi
exit 1
