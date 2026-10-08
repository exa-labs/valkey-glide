"""Check an installed wheel against an isolated, single-node Valkey cluster."""

import argparse
import platform
import socket
import subprocess
import tempfile
from importlib.metadata import version
from pathlib import Path
from threading import Event
from time import monotonic

import redis
from glide_sync import (
    AdvancedGlideClusterClientConfiguration,
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress,
)


def free_ports() -> tuple[int, int]:
    """Reserve two distinct loopback ports until both have been selected."""
    with socket.socket() as client, socket.socket() as bus:
        client.bind(("127.0.0.1", 0))
        bus.bind(("127.0.0.1", 0))
        return client.getsockname()[1], bus.getsockname()[1]


def wait_for(predicate, message: str, timeout: float = 15) -> None:
    """Bound readiness checks by a deadline instead of assuming startup latency."""
    deadline = monotonic() + timeout
    tick = Event()
    while not predicate():
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise AssertionError(message)
        tick.wait(min(0.02, remaining))


def check_connections(admin: redis.Redis, port: int) -> None:
    """Assert default and opted-in clients use exactly two and one sockets."""
    for disabled in (None, False, True):
        baseline = {row["id"] for row in admin.client_list()}
        advanced = (
            None
            if disabled is None
            else AdvancedGlideClusterClientConfiguration(
                disable_management_connections=disabled
            )
        )
        config = GlideClusterClientConfiguration(
            addresses=[NodeAddress("127.0.0.1", port)],
            client_name="wheel-smoke",
            request_timeout=5000,
            advanced_config=advanced,
        )
        request = config._create_a_protobuf_conn_request(cluster_mode=True)
        assert request.disable_management_connections is (disabled is True)
        client = GlideClusterClient.create(config)
        expected = 1 if disabled else 2
        try:
            assert client.set("wheel-smoke-key", "value") == "OK"
            assert client.get("wheel-smoke-key") == b"value"
            rows = [row for row in admin.client_list() if row["id"] not in baseline]
            assert len(rows) == expected, (disabled, rows)
            assert any(row["name"] == "wheel-smoke" for row in rows), rows
            print(
                f"PASS disable_management_connections={disabled}: SET/GET, {len(rows)} connections"
            )
        finally:
            client.close()
        wait_for(
            lambda: {row["id"] for row in admin.client_list()} == baseline,
            "Client sockets did not close",
        )


def main() -> None:
    """Start a cluster, run the wheel checks, and stop the server on any outcome."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-version")
    parser.add_argument("--expected-arch")
    args = parser.parse_args()
    if args.expected_version:
        assert version("valkey-glide-sync") == args.expected_version
    if args.expected_arch:
        aliases = {"arm64": "aarch64"}
        actual = platform.machine()
        assert aliases.get(actual, actual) == aliases.get(
            args.expected_arch, args.expected_arch
        )
    print(f"Native client imported on {platform.system()} {platform.machine()}")
    port, bus_port = free_ports()
    with tempfile.TemporaryDirectory(prefix="wheel-smoke-") as directory:
        log_path = Path(directory) / "server.log"
        with log_path.open("w") as log:
            server = subprocess.Popen(
                [
                    "valkey-server",
                    "--bind",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--cluster-port",
                    str(bus_port),
                    "--cluster-enabled",
                    "yes",
                    "--cluster-config-file",
                    "nodes.conf",
                    "--dir",
                    directory,
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        admin = redis.Redis(
            host="127.0.0.1",
            port=port,
            decode_responses=True,
            socket_timeout=1,
            socket_connect_timeout=1,
        )
        try:

            def ready() -> bool:
                """Fail immediately on server exit; tolerate connection refusal during startup."""
                assert server.poll() is None, "Valkey exited during startup"
                try:
                    return admin.ping()
                except redis.ConnectionError:
                    return False

            wait_for(ready, "Valkey did not start")
            admin.execute_command("CLUSTER", "ADDSLOTSRANGE", 0, 16383)
            wait_for(
                lambda: admin.cluster("info")["cluster_state"] == "ok",
                "Valkey cluster did not become ready",
            )
            check_connections(admin, port)
        finally:
            admin.close()
            server.terminate()
            server.wait(timeout=10)
            print(log_path.read_text())


if __name__ == "__main__":
    main()
