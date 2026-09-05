#!/usr/bin/env bash
# Bootstrap one herdr machine for the agent fleet.
#
#   WH_HERDR_PUBLIC_KEY=/path/to/wh_herdr.pub scripts/deploy-herdr-machine.sh local
#   WH_HERDR_PUBLIC_KEY=/path/to/wh_herdr.pub scripts/deploy-herdr-machine.sh user@host
#
# Installs the forced-command shim, the headless herdr user unit, and the
# worker-harness service PUBLIC key. Bootstrap uses YOUR interactive SSH access;
# the private key must be generated and kept only on the control server.
# The installed public key permits only the shim's allowlisted verbs.
set -euo pipefail

readonly target="${1:?usage: deploy-herdr-machine.sh <ssh-target>|local}"
readonly here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly shim="$here/wh-remote-shim"
readonly unit="$here/../systemd/herdr-wh.service"
readonly public_key="${WH_HERDR_PUBLIC_KEY:?set WH_HERDR_PUBLIC_KEY to the service public key file}"

[[ -r "$shim" ]] || { echo "missing $shim" >&2; exit 1; }
[[ -r "$unit" ]] || { echo "missing $unit" >&2; exit 1; }
[[ -r "$public_key" ]] || { echo "missing public key: $public_key" >&2; exit 1; }

remote() {
    if [[ "$target" == "local" ]]; then
        bash -s
    else
        ssh -o BatchMode=no "$target" bash -s
    fi
}

pubkey="$(cat -- "$public_key")"
[[ "$pubkey" =~ ^ssh-ed25519\ [A-Za-z0-9+/=]+(\ .*)?$ && "$pubkey" != *$'\n'* ]] || {
    echo "expected one plain ssh-ed25519 public key" >&2
    exit 1
}

{
    printf 'set -euo pipefail\n'
    printf 'mkdir -p ~/.local/bin ~/.config/systemd/user ~/.ssh\n'
    printf 'chmod 0700 ~/.ssh\n'

    printf 'cat > ~/.local/bin/wh-remote-shim <<'"'"'WH_SHIM_EOF'"'"'\n'
    cat -- "$shim"
    printf 'WH_SHIM_EOF\n'
    printf 'chmod 0755 ~/.local/bin/wh-remote-shim\n'

    printf 'cat > ~/.config/systemd/user/herdr-wh.service <<'"'"'WH_UNIT_EOF'"'"'\n'
    cat -- "$unit"
    printf 'WH_UNIT_EOF\n'
    # herdr is /usr/bin/herdr on some machines and ~/.local/bin/herdr on others,
    # and a user unit inherits neither a login shell nor its PATH.
    printf '%s\n' '
herdr_bin="$(command -v herdr || true)"
[ -n "$herdr_bin" ] || herdr_bin="$HOME/.local/bin/herdr"
[ -x "$herdr_bin" ] || { echo "herdr not found on this machine" >&2; exit 1; }
sed -i "s|^ExecStart=.*|ExecStart=$herdr_bin --session wh server|" ~/.config/systemd/user/herdr-wh.service
'

    # The forced command pins this key to the shim: no interactive shell, no
    # pty, no forwarding, regardless of what the client asks for.
    printf 'pubkey=%q\n' "$pubkey"
    printf '%s\n' '
entry="command=\"$HOME/.local/bin/wh-remote-shim\",no-agent-forwarding,no-port-forwarding,no-pty,no-X11-forwarding $pubkey"
touch ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
keyblob="$(printf %s "$pubkey" | cut -d" " -f2)"
if grep -qF "$keyblob" ~/.ssh/authorized_keys; then
    grep -vF "$keyblob" ~/.ssh/authorized_keys > ~/.ssh/authorized_keys.wh-tmp
    mv ~/.ssh/authorized_keys.wh-tmp ~/.ssh/authorized_keys
    chmod 0600 ~/.ssh/authorized_keys
fi
printf "%s\n" "$entry" >> ~/.ssh/authorized_keys

systemctl --user daemon-reload
systemctl --user enable herdr-wh.service
systemctl --user restart herdr-wh.service
loginctl enable-linger "$USER"
systemctl --user is-active --quiet herdr-wh.service
[[ "$(loginctl show-user "$USER" -p Linger --value)" == "yes" ]] || {
    echo "linger is not enabled; run loginctl enable-linger as an authorized user" >&2
    exit 1
}
systemctl --user --no-pager --lines=0 status herdr-wh.service
'
} | remote
