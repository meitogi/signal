#!/usr/bin/env bash
# Runs each drop-in snippet of deploy/ for real : the kit's compose file plus a test overlay
# with the proxy itself, on a throwaway network, then probe.mjs from a container on that
# network (wss connect, key check, signal, /healthz, hello delay, per-IP cap). Then checks
# the relay logged the probe's own IP : X-Forwarded-For from the proxy, trusted through
# TRUST_PROXY = the network's subnet. Cleans up after.
#
#   deploy/test/run.sh [traefik|haproxy|caddy|all]     (default : all)
#
# Needs docker with compose, and openssl. Bind mounts use this checkout's absolute
# path : with a remote daemon (dind), that path must exist there too.
set -euo pipefail

TEST_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="$(dirname "$TEST_DIR")"
TMP="$TEST_DIR/.tmp"
export TEST_DIR

mkdir -p "$TMP"
if [ ! -f "$TMP/relay.pem" ]; then
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 7 \
    -subj /CN=relay.test -addext subjectAltName=DNS:relay.test \
    -keyout "$TMP/key.pem" -out "$TMP/cert.pem" 2>/dev/null
  cat "$TMP/cert.pem" "$TMP/key.pem" > "$TMP/relay.pem"
  chmod 644 "$TMP/relay.pem"
fi

run_one() {
  local proxy="$1" port="$2" expect_open="$3"
  local project="signal-relay-test-$proxy"
  local env="$TMP/$proxy.env"
  local compose=(docker compose -p "$project" --env-file "$env" -f "$DEPLOY_DIR/$proxy/compose.yml" -f "$TEST_DIR/$proxy.yml")
  cleanup() {
    "${compose[@]}" down --remove-orphans >/dev/null 2>&1 || true
    docker network rm "$project" >/dev/null 2>&1 || true
  }
  trap cleanup RETURN
  echo "== $proxy"
  docker network create "$project" >/dev/null
  local subnet
  subnet="$(docker network inspect "$project" -f '{{(index .IPAM.Config 0).Subnet}}')"
  cat > "$env" <<ENV
RELAY_DOMAIN=relay.test
RELAY_KEY=$(openssl rand -hex 16)
PROXY_NETWORK=$project
TRUST_PROXY=$subnet
TRAEFIK_ENTRYPOINT=websecure
TRAEFIK_CERTRESOLVER=letsencrypt
TRAEFIK_INFLIGHT=3
TRAEFIK_RATE_BURST=100
EXPECT_OPEN=$expect_open
ENV
  "${compose[@]}" up -d --build --wait --quiet-pull >/dev/null
  local status=0 out
  out="$(docker run --rm --network "$project" --env-file "$env" -e NODE_TLS_REJECT_UNAUTHORIZED=0 \
    -v "$TEST_DIR/probe.mjs:/app/probe.mjs:ro" signal-relay node probe.mjs "wss://relay.test:$port")" || status=$?
  echo "$out" | grep -v '^probe-ip '
  local probe_ip
  probe_ip="$(echo "$out" | sed -n 's/^probe-ip //p' | head -1)"
  if [ -n "$probe_ip" ] && "${compose[@]}" logs signal-relay 2>/dev/null | grep -q "+ $probe_ip :"; then
    echo "PASS the relay sees the probe's IP $probe_ip (X-Forwarded-For trusted from $subnet)"
  else
    echo "FAIL the relay never logged the probe's IP ${probe_ip:-?}"
    status=1
  fi
  if [ "$status" -ne 0 ]; then
    "${compose[@]}" logs --tail 30 || true
    return "$status"
  fi
}

target="${1:-all}"
failed=0
for proxy in traefik haproxy caddy; do
  [ "$target" = all ] || [ "$target" = "$proxy" ] || continue
  case "$proxy" in haproxy) port=8443 ;; *) port=443 ;; esac
  case "$proxy" in traefik) expect_open=3 ;; *) expect_open=10 ;; esac
  run_one "$proxy" "$port" "$expect_open" || failed=1
done
[ "$failed" -eq 0 ] && echo "all snippets OK" || { echo "a snippet FAILED"; exit 1; }
