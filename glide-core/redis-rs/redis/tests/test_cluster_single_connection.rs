// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

#![cfg(feature = "cluster-async")]
mod support;

use redis::{
    aio::MultiplexedConnection,
    cluster_async::{testing::MANAGEMENT_CONN_NAME, ClusterConnection},
    cluster_routing::{Route, RoutingInfo, SingleNodeRoutingInfo, SlotAddr},
    cmd, AsyncCommands,
};
use std::{collections::HashMap, time::Duration};
use support::{parse_client_info, TestClusterContext};

const CLIENT_NAME: &str = "connection_mode_test";

async fn client_list(connection: &mut MultiplexedConnection) -> Vec<HashMap<String, String>> {
    let list: String = cmd("CLIENT")
        .arg("LIST")
        .query_async(connection)
        .await
        .unwrap();
    list.lines().map(parse_client_info).collect()
}

/// Count physical sockets rather than names in a map, which would hide duplicates.
async fn assert_connection_counts(observers: &mut [MultiplexedConnection], single: bool) {
    for observer in observers {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut tick = tokio::time::interval(Duration::from_millis(20));
            loop {
                let clients = client_list(observer).await;
                let mut complete = true;
                for (name, expected) in [
                    (CLIENT_NAME, 1),
                    (MANAGEMENT_CONN_NAME, usize::from(!single)),
                ] {
                    let count = clients
                        .iter()
                        .filter(|client| client["name"] == name)
                        .count();
                    if single {
                        assert!(
                            count <= expected,
                            "Extra connections for {name}: {clients:?}"
                        );
                    }
                    complete &= count == expected;
                }
                if complete {
                    break;
                }
                // After failover, the old primary can disappear from CLUSTER SLOTS
                // before it is advertised as a replica. Default-mode management
                // sockets also carry the user name until setup renames them.
                // Wait for setup, but reject extra sockets immediately in single mode.
                tick.tick().await;
            }
        })
        .await
        .expect("Physical connection counts did not converge");
    }
}

async fn primary_id(connection: &mut ClusterConnection) -> String {
    let value = connection
        .route_command(
            cmd("CLUSTER").arg("MYID"),
            RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(Route::new(
                0,
                SlotAddr::Master,
            ))),
        )
        .await
        .unwrap();
    redis::from_owned_redis_value(value).unwrap()
}

/// Exercise creation, idle topology refresh, failover, and authenticated reconnects.
async fn check_connection_mode(single: bool, refresh_from_seeds: bool) {
    let cluster = TestClusterContext::new(6, 1);
    // With ordinary topology checks, discover the other five nodes from one seed.
    // Seed-only checks need all addresses to repair every node while idle, since
    // this test deliberately disables the separate user-connection health task.
    let seeds = if refresh_from_seeds {
        &cluster.nodes[..]
    } else {
        &cluster.nodes[..1]
    };
    let builder = redis::cluster::ClusterClient::builder(seeds.to_vec())
        .use_protocol(cluster.protocol)
        .client_name(CLIENT_NAME.into())
        .connection_timeout(Duration::from_secs(2))
        .response_timeout(Duration::from_secs(2))
        .periodic_topology_checks(Duration::from_millis(20))
        .refresh_topology_from_initial_nodes(refresh_from_seeds)
        .slots_refresh_rate_limit(Duration::ZERO, 0);
    let builder = if single {
        builder.disable_management_connections(true)
    } else {
        // Omit the new option to verify backwards-compatible defaults.
        builder.periodic_connections_checks(Some(Duration::from_millis(20)))
    };
    let client = builder.build().unwrap();
    let mut connection = client
        .get_async_connection(None, None, None, None)
        .await
        .unwrap();
    let mut observers = Vec::new();
    for node in &cluster.nodes {
        observers.push(
            redis::Client::open(node.clone())
                .unwrap()
                .get_multiplexed_async_connection(Default::default())
                .await
                .unwrap(),
        );
    }
    assert_connection_counts(&mut observers, single).await;

    let _: () = connection
        .set("connection-mode-key", "value")
        .await
        .unwrap();
    assert_eq!(
        connection
            .get::<_, String>("connection-mode-key")
            .await
            .unwrap(),
        "value"
    );

    let old_primary = primary_id(&mut connection).await;
    connection
        .route_command(
            cmd("CLUSTER").arg("FAILOVER").arg("TAKEOVER"),
            RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(Route::new(
                0,
                SlotAddr::ReplicaRequired,
            ))),
        )
        .await
        .unwrap();
    // CLUSTER MYID cannot produce MOVED, so only periodic topology checks can
    // change the client-side primary route used here.
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        while primary_id(&mut connection).await == old_primary {
            tick.tick().await;
        }
    })
    .await
    .expect("Periodic topology checks did not discover the new primary");
    assert_connection_counts(&mut observers, single).await;

    // Existing observer sockets remain authenticated after the password changes.
    let password = "replacement-password";
    for observer in &mut observers {
        let _: () = cmd("ACL")
            .arg("SETUSER")
            .arg("default")
            .arg("resetpass")
            .arg(format!(">{password}"))
            .query_async(observer)
            .await
            .unwrap();
    }
    connection
        .update_connection_password(Some(password.into()))
        .await
        .unwrap();

    let mut old_ids = Vec::new();
    for observer in &mut observers {
        let clients = client_list(observer).await;
        let id = clients
            .iter()
            .find(|client| client["name"] == CLIENT_NAME)
            .unwrap()["id"]
            .clone();
        let killed: usize = cmd("CLIENT")
            .arg("KILL")
            .arg("ID")
            .arg(&id)
            .query_async(observer)
            .await
            .unwrap();
        assert_eq!(killed, 1);
        old_ids.push(id);
    }
    // No commands are sent through the cluster client until its background
    // tasks have reconnected every user socket with the new password.
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            let mut reconnected = true;
            for (observer, old_id) in observers.iter_mut().zip(&old_ids) {
                let clients = client_list(observer).await;
                reconnected &= clients
                    .iter()
                    .any(|client| client["name"] == CLIENT_NAME && client["id"] != *old_id);
            }
            if reconnected {
                break;
            }
            tick.tick().await;
        }
    })
    .await
    .expect("Background checks did not reconnect user connections");
    assert_connection_counts(&mut observers, single).await;
    let _: () = connection
        .set("connection-mode-key", "reconnected")
        .await
        .unwrap();
    assert_eq!(
        connection
            .get::<_, String>("connection-mode-key")
            .await
            .unwrap(),
        "reconnected"
    );
}

#[tokio::test]
async fn test_single_connection_per_node() {
    for refresh_from_seeds in [false, true] {
        check_connection_mode(true, refresh_from_seeds).await;
    }
}

#[tokio::test]
async fn test_default_management_connections() {
    for refresh_from_seeds in [false, true] {
        check_connection_mode(false, refresh_from_seeds).await;
    }
}
