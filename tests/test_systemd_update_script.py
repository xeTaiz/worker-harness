from pathlib import Path


def test_worker_image_update_requires_real_daemon_not_only_active_wrapper():
    script = Path("systemd/worker-harness-update.sh").read_text()
    assert "systemctl --user is-active worker-harness.service" in script
    assert "pgrep -f '^python3? /worker_daemon.py$'" in script
    assert 'if [ "$state" = "active" ] && [ -n "$daemon_pid" ]' in script
    assert "active_streak=$ACTIVE_STREAK/10" in script
    assert 'mv -f "$CUR" "${CUR}.failed"' in script
    assert 'mv -f "${CUR}.old" "$CUR"' in script
