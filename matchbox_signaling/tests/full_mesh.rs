#[cfg(test)]
mod tests {
    use axum::Router;
    use futures::{SinkExt, StreamExt};
    use matchbox_protocol::{JsonPeerEvent, PeerId};
    use matchbox_signaling::SignalingServer;
    use std::{net::Ipv4Addr, str::FromStr};
    use tokio::{
        net::TcpStream,
        sync::mpsc::{error::TryRecvError, unbounded_channel},
    };
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

    // Helper to take the next PeerEvent from a stream
    async fn recv_peer_event(
        client: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> JsonPeerEvent {
        let message: Message = client.next().await.unwrap().unwrap();
        JsonPeerEvent::from_str(&message.to_string()).expect("json peer event")
    }

    // Helper to extract PeerId when expecting an Id assignment
    fn get_peer_id(peer_event: JsonPeerEvent) -> PeerId {
        if let JsonPeerEvent::IdAssigned(id) = peer_event {
            id
        } else {
            panic!("Peer_event was not IdAssigned: {peer_event:?}");
        }
    }

    #[tokio::test]
    async fn ws_connect() {
        let mut server = SignalingServer::full_mesh_builder((Ipv4Addr::LOCALHOST, 0)).build();
        let addr = server.bind().unwrap();
        tokio::spawn(server.serve());

        tokio_tungstenite::connect_async(format!("ws://{addr}/room_a"))
            .await
            .expect("handshake");
    }

    #[tokio::test]
    async fn uuid_assigned() {
        let mut server = SignalingServer::full_mesh_builder((Ipv4Addr::LOCALHOST, 0)).build();
        let addr = server.bind().unwrap();
        tokio::spawn(server.serve());

        let (mut client, _response) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/room_a"))
                .await
                .unwrap();

        let id_assigned_event = recv_peer_event(&mut client).await;

        assert!(matches!(id_assigned_event, JsonPeerEvent::IdAssigned(..)));
    }

    #[tokio::test]
    async fn nested_server() {
        let mut server = SignalingServer::full_mesh_builder((Ipv4Addr::LOCALHOST, 0))
            .build_with(|router| Router::new().nest("/nested/", router));
        let addr = server.bind().unwrap();
        tokio::spawn(server.serve());

        let (mut client, _response) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/nested/room_a"))
                .await
                .unwrap();

        let id_assigned_event = recv_peer_event(&mut client).await;

        assert!(matches!(id_assigned_event, JsonPeerEvent::IdAssigned(..)));
    }

    #[tokio::test]
    async fn new_peer() {
        let mut server = SignalingServer::full_mesh_builder((Ipv4Addr::LOCALHOST, 0)).build();
        let addr = server.bind().unwrap();
        tokio::spawn(server.serve());

        let (mut client_a, _response) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/room_a"))
                .await
                .unwrap();

        let _a_uuid = get_peer_id(recv_peer_event(&mut client_a).await);

        let (mut client_b, _response) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/room_a"))
                .await
                .unwrap();

        let b_uuid = get_peer_id(recv_peer_event(&mut client_b).await);

        let new_peer_event = recv_peer_event(&mut client_a).await;

        assert_eq!(new_peer_event, JsonPeerEvent::NewPeer(b_uuid));
    }

    /// Peers joining a room at the same moment each learn of every other: of each pair, the one
    /// that joined first is told of the second. Many rounds of many simultaneous joins, as a
    /// race between two joins finds no peer told of the other only now and then.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn simultaneous_joins_meet() {
        const PEERS: usize = 16;
        const ROUNDS: usize = 20;
        let mut server = SignalingServer::full_mesh_builder((Ipv4Addr::LOCALHOST, 0)).build();
        let addr = server.bind().unwrap();
        tokio::spawn(server.serve());

        for round in 0..ROUNDS {
            let room = format!("ws://{addr}/round_{round}");
            // Every client in its own task, so the server handles the joins at once.
            let (told_tx, mut told_rx) = unbounded_channel::<(PeerId, PeerId)>();
            let (ids_tx, mut ids_rx) = unbounded_channel::<PeerId>();
            let clients: Vec<_> = (0..PEERS)
                .map(|_| {
                    let (room, told_tx, ids_tx) = (room.clone(), told_tx.clone(), ids_tx.clone());
                    tokio::spawn(async move {
                        let (mut client, _response) =
                            tokio_tungstenite::connect_async(room).await.unwrap();
                        let me = get_peer_id(recv_peer_event(&mut client).await);
                        ids_tx.send(me).unwrap();
                        while let Some(Ok(message)) = client.next().await {
                            if let Ok(JsonPeerEvent::NewPeer(other)) =
                                JsonPeerEvent::from_str(&message.to_string())
                            {
                                told_tx.send((me, other)).unwrap();
                            }
                        }
                    })
                })
                .collect();

            let mut ids = Vec::new();
            while ids.len() < PEERS {
                ids.push(ids_rx.recv().await.unwrap());
            }
            let pairs = PEERS * (PEERS - 1) / 2;
            let mut told = Vec::new();
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
            while told.len() < pairs {
                match tokio::time::timeout_at(deadline, told_rx.recv()).await {
                    Ok(Some(pair)) => told.push(pair),
                    _ => break,
                }
            }
            for (i, a) in ids.iter().enumerate() {
                for b in &ids[i + 1..] {
                    let times = told
                        .iter()
                        .filter(|&&(to, of)| (to, of) == (*a, *b) || (to, of) == (*b, *a))
                        .count();
                    assert_eq!(
                        times, 1,
                        "round {round}: {a} and {b} told of each other {times} times"
                    );
                }
            }
            for client in clients {
                client.abort();
            }
        }
    }

    #[tokio::test]
    async fn disconnect_peer() {
        let mut server = SignalingServer::full_mesh_builder((Ipv4Addr::LOCALHOST, 0)).build();
        let addr = server.bind().unwrap();
        tokio::spawn(server.serve());

        let (mut client_a, _response) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/room_a"))
                .await
                .unwrap();

        let _a_uuid = get_peer_id(recv_peer_event(&mut client_a).await);

        let (mut client_b, _response) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/room_a"))
                .await
                .unwrap();

        let b_uuid = get_peer_id(recv_peer_event(&mut client_b).await);

        // Ensure Peer B was received
        let new_peer_event = recv_peer_event(&mut client_a).await;
        assert_eq!(new_peer_event, JsonPeerEvent::NewPeer(b_uuid));

        // Disconnect Peer B
        _ = client_b.close(None).await;
        let peer_left_event = recv_peer_event(&mut client_a).await;

        assert_eq!(peer_left_event, JsonPeerEvent::PeerLeft(b_uuid));
    }

    #[tokio::test]
    async fn signal() {
        let mut server = SignalingServer::full_mesh_builder((Ipv4Addr::LOCALHOST, 0)).build();
        let addr = server.bind().unwrap();
        tokio::spawn(server.serve());

        let (mut client_a, _response) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/room_a"))
                .await
                .unwrap();

        let a_uuid = get_peer_id(recv_peer_event(&mut client_a).await);

        let (mut client_b, _response) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/room_a"))
                .await
                .unwrap();

        let _b_uuid = get_peer_id(recv_peer_event(&mut client_b).await);

        let new_peer_event = recv_peer_event(&mut client_a).await;
        let peer_uuid = match new_peer_event {
            JsonPeerEvent::NewPeer(PeerId(peer_uuid)) => peer_uuid.to_string(),
            _ => panic!("unexpected event"),
        };

        _ = client_a
            .send(Message::text(format!(
                "{{\"Signal\": {{\"receiver\": \"{peer_uuid}\", \"data\": \"123\" }}}}"
            )))
            .await;

        let signal_event = recv_peer_event(&mut client_b).await;
        assert_eq!(
            signal_event,
            JsonPeerEvent::Signal {
                data: serde_json::Value::String("123".to_string()),
                sender: a_uuid,
            }
        );
    }

    #[tokio::test]
    async fn on_connection_req_callback() {
        let (connection_requested_tx, mut connection_requested_rx) = unbounded_channel();

        let mut server = SignalingServer::client_server_builder((Ipv4Addr::LOCALHOST, 0))
            .on_connection_request({
                let connection_requested_tx = connection_requested_tx.clone();
                move |_| {
                    connection_requested_tx
                        .send(())
                        .expect("send connection requested");
                    Ok(true)
                }
            })
            .build();
        let addr = server.bind().unwrap();
        tokio::spawn(server.serve());

        // Connect
        _ = tokio_tungstenite::connect_async(format!("ws://{addr}/room_a")).await;

        connection_requested_rx
            .recv()
            .await
            .expect("connection requested");
    }

    #[tokio::test]
    async fn deny_on_connection_req_callback() {
        let (upgrade_called_tx, mut upgrade_called_rx) = unbounded_channel();
        let (peer_connected_tx, mut peer_connected_rx) = unbounded_channel();

        let mut server = SignalingServer::full_mesh_builder((Ipv4Addr::LOCALHOST, 0))
            .on_connection_request({
                let upgrade_called_tx = upgrade_called_tx.clone();
                move |_| {
                    upgrade_called_tx.send(()).expect("send upgrade called");
                    Ok(false) // <-- Deny access!
                }
            })
            .on_peer_connected({
                // This should not get called because we deny all connections on upgrade
                let peer_connected_tx = peer_connected_tx.clone();
                move |_| peer_connected_tx.send(()).expect("send peer connected")
            })
            .build();
        let addr = server.bind().unwrap();
        tokio::spawn(server.serve());

        // Connect
        _ = tokio_tungstenite::connect_async(format!("ws://{addr}/room_a")).await;

        upgrade_called_rx.recv().await.expect("upgrade called");
        assert_eq!(peer_connected_rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn on_id_assignment_callback() {
        let (id_assigned_tx, mut id_assigned_rx) = unbounded_channel();

        let mut server = SignalingServer::full_mesh_builder((Ipv4Addr::LOCALHOST, 0))
            .on_id_assignment({
                let id_assigned_tx = id_assigned_tx.clone();
                move |_| id_assigned_tx.send(()).expect("send id assigned")
            })
            .build();
        let addr = server.bind().unwrap();
        tokio::spawn(server.serve());

        // Connect
        tokio_tungstenite::connect_async(format!("ws://{addr}/room_a"))
            .await
            .expect("handshake");

        id_assigned_rx.recv().await.expect("id assigned");
    }

    #[tokio::test]
    async fn on_connect_callback() {
        let (peer_connected_tx, mut peer_connected_rx) = unbounded_channel();

        let mut server = SignalingServer::full_mesh_builder((Ipv4Addr::LOCALHOST, 0))
            .on_peer_connected({
                let peer_connected_tx = peer_connected_tx.clone();
                move |_| peer_connected_tx.send(()).expect("send peer connected")
            })
            .build();
        let addr = server.bind().unwrap();
        tokio::spawn(server.serve());

        // Connect
        tokio_tungstenite::connect_async(format!("ws://{addr}/room_a"))
            .await
            .expect("handshake");

        peer_connected_rx.recv().await.expect("peer connected");
    }

    #[tokio::test]
    async fn on_disconnect_callback() {
        let (disconnected_tx, mut disconnected_rx) = unbounded_channel::<()>();

        let mut server = SignalingServer::full_mesh_builder((Ipv4Addr::LOCALHOST, 0))
            .on_peer_disconnected({
                let disconnected_tx = disconnected_tx.clone();
                move |_| {
                    disconnected_tx.send(()).expect("send disconnected");
                }
            })
            .build();
        let addr = server.bind().unwrap();
        tokio::spawn(server.serve());

        // Connect
        {
            tokio_tungstenite::connect_async(format!("ws://{addr}/room_a"))
                .await
                .expect("handshake");
        }
        // Disconnects due to scope drop

        disconnected_rx.recv().await.expect("disconnected");
    }
}
