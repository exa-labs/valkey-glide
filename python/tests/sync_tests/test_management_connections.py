# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

from typing import Dict, Generator, Set, cast

import pytest
from glide_shared.config import (
    AdvancedGlideClusterClientConfiguration,
    GlideClusterClientConfiguration,
    ProtocolVersion,
    ReadFrom,
)
from glide_shared.routes import AllNodes
from glide_sync import GlideClusterClient

from tests.utils.cluster import ValkeyCluster
from tests.utils.utils import create_sync_client_config


def _client_ids(client: GlideClusterClient) -> Dict[bytes, Set[bytes]]:
    """Read server connection IDs per node, excluding replica links."""
    reports = cast(
        Dict[bytes, bytes], client.custom_command(["CLIENT", "LIST"], route=AllNodes())
    )
    result = {}
    for address, report in reports.items():
        rows = [
            dict(field.split(b"=", 1) for field in row.split())
            for row in report.splitlines()
        ]
        result[address] = {
            row[b"id"]
            for row in rows
            if b"S" not in row[b"flags"] and b"M" not in row[b"flags"]
        }
    return result


@pytest.fixture
def connection_count_cluster(request) -> Generator[ValkeyCluster, None, None]:
    """Isolate connection counts from clients opened by parallel tests."""
    cluster = ValkeyCluster(
        tls=request.config.getoption("--tls"),
        cluster_mode=True,
        shard_count=3,
        replica_count=1,
    )
    yield cluster


@pytest.mark.parametrize("disabled", [False, True])
@pytest.mark.parametrize("protocol", [ProtocolVersion.RESP2, ProtocolVersion.RESP3])
@pytest.mark.parametrize("read_from", [ReadFrom.PRIMARY, ReadFrom.PREFER_REPLICA])
def test_cluster_management_connection_count(
    request,
    connection_count_cluster: ValkeyCluster,
    disabled: bool,
    protocol: ProtocolVersion,
    read_from: ReadFrom,
):
    monitor_config = cast(
        GlideClusterClientConfiguration,
        create_sync_client_config(
            request,
            cluster_mode=True,
            protocol=protocol,
            valkey_cluster=connection_count_cluster,
        ),
    )
    monitor = GlideClusterClient.create(monitor_config)
    try:
        before = _client_ids(monitor)
        config = cast(
            GlideClusterClientConfiguration,
            create_sync_client_config(
                request,
                cluster_mode=True,
                protocol=protocol,
                read_from=read_from,
                valkey_cluster=connection_count_cluster,
            ),
        )
        advanced = cast(AdvancedGlideClusterClientConfiguration, config.advanced_config)
        advanced.disable_management_connections = disabled
        client = GlideClusterClient.create(config)
        try:
            assert client.ping() == b"PONG"
            # Different keys exercise routing across the slot map; GET also exercises
            # replica routing without depending on asynchronous replication catching up.
            for key in (
                "{first}:connections",
                "{second}:connections",
                "{third}:connections",
            ):
                assert client.set(key, "value") == "OK"
                value = client.get(key)
                assert (
                    value in (None, b"value")
                    if read_from == ReadFrom.PREFER_REPLICA
                    else value == b"value"
                )
            after = _client_ids(monitor)
            assert after.keys() == before.keys()
            assert len(after) == 6
            for address in before:
                assert len(after[address] - before[address]) == (1 if disabled else 2)
        finally:
            client.close()
    finally:
        monitor.close()
