use serde_json::{Map, Value, json};

use flow_domain::SIGNALING_PROTOCOL_ID;

pub fn document(signaling_urls: &[String]) -> Value {
    let mut servers = Map::new();
    let mut server_references = Vec::with_capacity(signaling_urls.len());
    for (index, origin) in signaling_urls.iter().enumerate() {
        let name = if index == 0 {
            "primary".to_owned()
        } else {
            format!("failover_{}", index + 1)
        };
        let host = origin
            .strip_prefix("wss://")
            .unwrap_or(origin)
            .trim_end_matches('/');
        servers.insert(
            name.clone(),
            json!({
                "host": host,
                "protocol": "wss",
                "description": if index == 0 {
                    "Primary Flow P2P signaling endpoint"
                } else {
                    "Ordered Flow P2P signaling failover endpoint"
                }
            }),
        );
        server_references.push(json!({"$ref": format!("#/servers/{name}")}));
    }

    json!({
        "asyncapi": "3.1.0",
        "id": "urn:heterocloud:flow:signaling:v1",
        "info": {
            "title": "HeteroCloud Flow P2P Signaling API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Client-facing WebSocket contract for Flow P2P rooms. Connect to a URL returned by POST /v1/rooms/{room_id}/join. The first text frame must be signed_context and uses the three X-Flow-* values unchanged. Wait for authenticated before sending targeted signaling frames. Payload objects are transported without interpretation so both peers must use the same SDP/ICE representation."
        },
        "defaultContentType": "application/json",
        "servers": servers,
        "channels": {
            "roomSignaling": {
                "address": "/v1/signal/{room_id}",
                "description": "One scoped P2P room signaling channel. The room must be ready and belong to the signed principal context.",
                "servers": server_references,
                "parameters": {
                    "room_id": {
                        "description": "Room UUID returned by room creation or matchmaking assignment",
                        "examples": ["0198a121-ffbd-70c2-a3c8-c65516d7b8fb"]
                    }
                },
                "messages": {
                    "authentication": {"$ref": "#/components/messages/authentication"},
                    "clientSignal": {"$ref": "#/components/messages/clientSignal"},
                    "authenticated": {"$ref": "#/components/messages/authenticated"},
                    "peerJoined": {"$ref": "#/components/messages/peerJoined"},
                    "peerLeft": {"$ref": "#/components/messages/peerLeft"},
                    "serverSignal": {"$ref": "#/components/messages/serverSignal"},
                    "protocolError": {"$ref": "#/components/messages/protocolError"}
                }
            }
        },
        "operations": {
            "authenticate": {
                "action": "send",
                "summary": "Send the mandatory first authentication frame",
                "description": "Copy x-flow-principal, x-flow-timestamp, and x-flow-signature from the issued access context into principal_context, timestamp, and signature. Send this as the first WebSocket text frame.",
                "channel": {"$ref": "#/channels/roomSignaling"},
                "messages": [{"$ref": "#/channels/roomSignaling/messages/authentication"}]
            },
            "sendSignal": {
                "action": "send",
                "summary": "Send a targeted SDP, ICE, renegotiation, or leave frame",
                "channel": {"$ref": "#/channels/roomSignaling"},
                "messages": [{"$ref": "#/channels/roomSignaling/messages/clientSignal"}]
            },
            "receiveFrames": {
                "action": "receive",
                "summary": "Receive authentication, presence, relayed signaling, and error frames",
                "channel": {"$ref": "#/channels/roomSignaling"},
                "messages": [
                    {"$ref": "#/channels/roomSignaling/messages/authenticated"},
                    {"$ref": "#/channels/roomSignaling/messages/peerJoined"},
                    {"$ref": "#/channels/roomSignaling/messages/peerLeft"},
                    {"$ref": "#/channels/roomSignaling/messages/serverSignal"},
                    {"$ref": "#/channels/roomSignaling/messages/protocolError"}
                ]
            }
        },
        "components": {
            "messages": {
                "authentication": {
                    "name": "authentication",
                    "title": "Signed context authentication",
                    "payload": {"$ref": "#/components/schemas/AuthenticationFrame"},
                    "examples": [{
                        "name": "issuedAccessContext",
                        "payload": {
                            "type": "signed_context",
                            "principal_context": "<X-Flow-Principal value>",
                            "timestamp": "<X-Flow-Timestamp value>",
                            "signature": "<X-Flow-Signature value>"
                        }
                    }]
                },
                "clientSignal": {
                    "name": "clientSignal",
                    "title": "Targeted client signaling frame",
                    "payload": {"$ref": "#/components/schemas/ClientSignalFrame"},
                    "examples": [
                        {
                            "name": "offer",
                            "payload": {
                                "type": "offer",
                                "target": "0198a124-328e-7aad-b374-4237a4de904a",
                                "payload": {"sdp": "v=0..."}
                            }
                        },
                        {
                            "name": "iceCandidate",
                            "payload": {
                                "type": "ice_candidate",
                                "target": "0198a124-328e-7aad-b374-4237a4de904a",
                                "payload": {
                                    "candidate": "candidate:1 1 UDP 2122260223 192.0.2.10 50000 typ host",
                                    "sdpMid": "0",
                                    "sdpMLineIndex": 0
                                }
                            }
                        }
                    ]
                },
                "authenticated": {
                    "name": "authenticated",
                    "payload": {"$ref": "#/components/schemas/AuthenticatedFrame"}
                },
                "peerJoined": {
                    "name": "peerJoined",
                    "payload": {"$ref": "#/components/schemas/PeerJoinedFrame"}
                },
                "peerLeft": {
                    "name": "peerLeft",
                    "payload": {"$ref": "#/components/schemas/PeerLeftFrame"}
                },
                "serverSignal": {
                    "name": "serverSignal",
                    "payload": {"$ref": "#/components/schemas/ServerSignalFrame"}
                },
                "protocolError": {
                    "name": "protocolError",
                    "payload": {"$ref": "#/components/schemas/ErrorFrame"}
                }
            },
            "schemas": schemas()
        },
        "x-flow-signaling-protocol": SIGNALING_PROTOCOL_ID
    })
}

pub fn documentation_urls(api_urls: &[String]) -> Vec<String> {
    api_urls
        .iter()
        .map(|origin| format!("{}/asyncapi.json", origin.trim_end_matches('/')))
        .collect()
}

fn schemas() -> Value {
    let uuid = || json!({"type": "string", "format": "uuid"});
    let peer = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["connection_id", "principal_id"],
        "properties": {
            "connection_id": uuid(),
            "principal_id": uuid()
        }
    });
    json!({
        "AuthenticationFrame": {
            "type": "object",
            "additionalProperties": false,
            "required": ["type", "principal_context", "timestamp", "signature"],
            "properties": {
                "type": {"const": "signed_context"},
                "principal_context": {
                    "type": "string",
                    "description": "Exact X-Flow-Principal value from the issued access context"
                },
                "timestamp": {
                    "type": "string",
                    "description": "Exact X-Flow-Timestamp value from the issued access context"
                },
                "signature": {
                    "type": "string",
                    "description": "Exact X-Flow-Signature value from the issued access context"
                }
            }
        },
        "SignalKind": {
            "type": "string",
            "enum": ["offer", "answer", "ice_candidate", "renegotiate", "leave"]
        },
        "ClientSignalFrame": {
            "type": "object",
            "additionalProperties": false,
            "required": ["type", "target", "payload"],
            "properties": {
                "type": {"$ref": "#/components/schemas/SignalKind"},
                "target": {
                    "type": "string",
                    "format": "uuid",
                    "description": "Destination principal_id from authenticated.peers, peer_joined, or matchmaking assignment.peer_principal_ids"
                },
                "payload": {
                    "type": "object",
                    "description": "Opaque application-defined SDP, ICE, or control payload. Flow preserves this object unchanged.",
                    "additionalProperties": true
                }
            }
        },
        "Peer": peer,
        "AuthenticatedFrame": {
            "type": "object",
            "additionalProperties": false,
            "required": ["type", "connection_id", "room_id", "principal_id", "peers"],
            "properties": {
                "type": {"const": "authenticated"},
                "connection_id": uuid(),
                "room_id": uuid(),
                "principal_id": uuid(),
                "peers": {
                    "type": "array",
                    "description": "Connections already active in the room when authentication completes",
                    "items": {"$ref": "#/components/schemas/Peer"}
                }
            }
        },
        "PeerJoinedFrame": {
            "type": "object",
            "additionalProperties": false,
            "required": ["type", "peer"],
            "properties": {
                "type": {"const": "peer_joined"},
                "peer": {"$ref": "#/components/schemas/Peer"}
            }
        },
        "PeerLeftFrame": {
            "type": "object",
            "additionalProperties": false,
            "required": ["type", "peer"],
            "properties": {
                "type": {"const": "peer_left"},
                "peer": {"$ref": "#/components/schemas/Peer"}
            }
        },
        "ServerSignalFrame": {
            "type": "object",
            "additionalProperties": false,
            "required": ["type", "kind", "sender", "payload", "sent_at"],
            "properties": {
                "type": {"const": "signal"},
                "kind": {"$ref": "#/components/schemas/SignalKind"},
                "sender": uuid(),
                "payload": {"type": "object", "additionalProperties": true},
                "sent_at": {"type": "string", "format": "date-time"}
            }
        },
        "ErrorFrame": {
            "type": "object",
            "additionalProperties": false,
            "required": ["type", "code", "message"],
            "properties": {
                "type": {"const": "error"},
                "code": {"type": "string"},
                "message": {"type": "string"}
            }
        },
        "ServerFrame": {
            "oneOf": [
                {"$ref": "#/components/schemas/AuthenticatedFrame"},
                {"$ref": "#/components/schemas/PeerJoinedFrame"},
                {"$ref": "#/components/schemas/PeerLeftFrame"},
                {"$ref": "#/components/schemas/ServerSignalFrame"},
                {"$ref": "#/components/schemas/ErrorFrame"}
            ]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{document, documentation_urls};

    #[test]
    fn publishes_ha_websocket_servers_and_complete_frame_contract() {
        let document = document(&[
            "wss://flow-a.example.test".into(),
            "wss://flow-b.example.test:8443".into(),
        ]);
        assert_eq!(document["asyncapi"], "3.1.0");
        assert_eq!(
            document["servers"]["primary"]["host"],
            "flow-a.example.test"
        );
        assert_eq!(
            document["servers"]["failover_2"]["host"],
            "flow-b.example.test:8443"
        );
        assert_eq!(
            document["channels"]["roomSignaling"]["address"],
            "/v1/signal/{room_id}"
        );
        assert_eq!(
            document["components"]["schemas"]["AuthenticationFrame"]["properties"]["principal_context"]
                ["description"],
            "Exact X-Flow-Principal value from the issued access context"
        );
        assert!(
            document["components"]["schemas"]["ServerFrame"]["oneOf"]
                .as_array()
                .is_some_and(|variants| variants.len() == 5)
        );
    }

    #[test]
    fn derives_asyncapi_discovery_urls_from_every_api_origin() {
        assert_eq!(
            documentation_urls(&[
                "https://flow-a.example.test/".into(),
                "https://flow-b.example.test".into(),
            ]),
            [
                "https://flow-a.example.test/asyncapi.json",
                "https://flow-b.example.test/asyncapi.json",
            ]
        );
    }
}
