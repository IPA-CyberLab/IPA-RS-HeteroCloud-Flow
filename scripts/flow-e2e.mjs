#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import process from "node:process";

const apiUrl = (process.env.FLOW_API_URL || "https://flow.heterocloud.mizuame.app").replace(/\/$/, "");
const durationSeconds = numberEnv("FLOW_DURATION_SECONDS", 600, 10, 86_400);
const intervalSeconds = numberEnv("FLOW_INTERVAL_SECONDS", 30, 5, 3_600);
const requestTimeoutMs = numberEnv("FLOW_REQUEST_TIMEOUT_MS", 30_000, 1_000, 120_000);
const playwrightModule = process.env.PLAYWRIGHT_MODULE || "playwright";
const hostResolverRules = process.env.FLOW_HOST_RESOLVER_RULES || "";

const { chromium } = createRequire(import.meta.url)(playwrightModule);

const startedAt = Date.now();
const deadline = startedAt + durationSeconds * 1_000;
let iteration = 0;
let passed = 0;

function numberEnv(name, fallback, minimum, maximum) {
  const value = Number.parseInt(process.env[name] || "", 10);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    return fallback;
  }
  return value;
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function normalizeHeaders(value) {
  const source = value?.headers || value;
  if (!source || typeof source !== "object") {
    throw new Error("context command/file must return a JSON object of X-Flow headers");
  }
  const headers = {};
  for (const [key, raw] of Object.entries(source)) {
    const normalized = key.toLowerCase().replaceAll("_", "-");
    if (["x-flow-principal", "x-flow-timestamp", "x-flow-signature"].includes(normalized)) {
      headers[normalized] = String(raw);
    }
  }
  for (const key of ["x-flow-principal", "x-flow-timestamp", "x-flow-signature"]) {
    if (!headers[key]) {
      throw new Error(`missing ${key} in access context`);
    }
  }
  return headers;
}

function loadContext() {
  if (process.env.FLOW_CONTEXT_COMMAND) {
    const output = execFileSync("sh", ["-c", process.env.FLOW_CONTEXT_COMMAND], {
      encoding: "utf8",
      maxBuffer: 256 * 1024,
      stdio: ["ignore", "pipe", "inherit"],
    });
    return normalizeHeaders(JSON.parse(output));
  }
  const path = process.env.FLOW_CONTEXT_FILE;
  if (!path || !existsSync(path)) {
    throw new Error("set FLOW_CONTEXT_FILE or FLOW_CONTEXT_COMMAND");
  }
  return normalizeHeaders(JSON.parse(readFileSync(path, "utf8")));
}

async function request(path, method, headers, body) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), requestTimeoutMs);
  try {
    let response;
    try {
      response = await fetch(`${apiUrl}${path}`, {
        method,
        headers: {
          ...headers,
          ...(body ? { "content-type": "application/json" } : {}),
        },
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
    } catch (cause) {
      const error = new Error(`request failed ${method} ${path}: ${cause}`, { cause });
      error.retryable = true;
      throw error;
    }
    const text = await response.text();
    let parsed;
    try {
      parsed = text ? JSON.parse(text) : null;
    } catch {
      parsed = { raw: text.slice(0, 512) };
    }
    if (!response.ok) {
      const retryable = [502, 503, 504].includes(response.status);
      const error = new Error(`HTTP ${response.status} ${method} ${path}: ${JSON.stringify(parsed)}`);
      error.retryable = retryable;
      throw error;
    }
    return parsed;
  } finally {
    clearTimeout(timer);
  }
}

async function requestWithRetry(path, method, headers, body) {
  let lastError;
  for (let attempt = 0; attempt < 4; attempt += 1) {
    try {
      return await request(path, method, headers, body);
    } catch (error) {
      lastError = error;
      if (!error.retryable || attempt === 3) {
        throw error;
      }
      await sleep(250 * (attempt + 1));
    }
  }
  throw lastError;
}

async function connectPeers(pageA, pageB, connectionA, connectionB, headers) {
  const startPeer = async (page, connection, offerer, label) =>
    page.evaluate(
      async ({ connection, headers, offerer, label }) => {
        const iceServers = connection.ice?.ice_servers || [];
        if (!iceServers.some((server) => server.urls?.some((url) => String(url).startsWith("stun:")))) {
          throw new Error("join response did not include a STUN server");
        }
        if (!iceServers.some((server) => server.urls?.some((url) => String(url).startsWith("turn:")))) {
          throw new Error("join response did not include a TURN server");
        }

        const peerConnection = new RTCPeerConnection({ iceServers });
        const socket = new WebSocket(connection.urls[0]);
        const pendingCandidates = [];
        let remoteDescriptionSet = false;
        let remotePrincipal;
        let offered = false;
        let dataChannel;
        let resolveResult;
        let rejectResult;
        const result = new Promise((resolve, reject) => {
          resolveResult = resolve;
          rejectResult = reject;
        });
        let settled = false;
        let finishing = false;
        let timeout;

        const fail = (error) => {
          if (settled) return;
          settled = true;
          clearTimeout(timeout);
          socket.close();
          peerConnection.close();
          rejectResult(error);
        };

        const connectionStats = async () => {
          for (let attempt = 0; attempt < 30; attempt += 1) {
            const reports = new Map();
            for (const report of await peerConnection.getStats()) reports.set(report.id, report);
            const transport = [...reports.values()].find(
              (report) => report.type === "transport" && report.selectedCandidatePairId,
            );
            const pair = transport
              ? reports.get(transport.selectedCandidatePairId)
              : [...reports.values()].find(
                  (report) => report.type === "candidate-pair" &&
                    (report.selected || report.nominated || report.state === "succeeded"),
                );
            if (pair?.localCandidateId && pair.remoteCandidateId) {
              const local = reports.get(pair.localCandidateId);
              const remote = reports.get(pair.remoteCandidateId);
              return {
                selected_candidate_pair: {
                  state: pair.state,
                  local_candidate_type: local?.candidateType || null,
                  remote_candidate_type: remote?.candidateType || null,
                  current_round_trip_time: pair.currentRoundTripTime || null,
                  bytes_sent: pair.bytesSent || 0,
                  bytes_received: pair.bytesReceived || 0,
                },
              };
            }
            await new Promise((resolve) => setTimeout(resolve, 100));
          }
          throw new Error("WebRTC selected candidate pair was not reported");
        };

        const finish = async (value) => {
          if (settled || finishing) return;
          finishing = true;
          clearTimeout(timeout);
          let stats;
          try {
            stats = await connectionStats();
          } catch (error) {
            finishing = false;
            fail(error);
            return;
          }
          settled = true;
          socket.close();
          peerConnection.close();
          resolveResult({ ...value, stats });
        };

        timeout = setTimeout(() => fail(new Error(`${label} WebRTC connection timed out`)), 30_000);

        const send = (type, target, payload) => {
          if (socket.readyState !== WebSocket.OPEN) {
            throw new Error(`${label} signaling socket is not open`);
          }
          socket.send(JSON.stringify({ type, target, payload }));
        };

        const flushCandidates = () => {
          if (!remotePrincipal) return;
          while (pendingCandidates.length) {
            const candidate = pendingCandidates.shift();
            send("ice_candidate", remotePrincipal, candidate);
          }
        };

        const maybeOffer = async () => {
          if (!offerer || offered || !remotePrincipal) return;
          offered = true;
          const offer = await peerConnection.createOffer();
          await peerConnection.setLocalDescription(offer);
          send("offer", remotePrincipal, { sdp: offer.sdp });
        };

        peerConnection.onicecandidate = (event) => {
          if (!event.candidate) return;
          const candidate = {
            candidate: event.candidate.candidate,
            sdpMid: event.candidate.sdpMid,
            sdpMLineIndex: event.candidate.sdpMLineIndex,
          };
          if (remotePrincipal) send("ice_candidate", remotePrincipal, candidate);
          else pendingCandidates.push(candidate);
        };

        peerConnection.ondatachannel = (event) => {
          dataChannel = event.channel;
          dataChannel.onmessage = (message) => {
            if (label === "peer-b" && message.data === "flow-e2e-ping") {
              dataChannel.send("flow-e2e-pong");
              setTimeout(() => {
                void finish({ state: peerConnection.connectionState, ice: peerConnection.iceConnectionState });
              }, 250);
            }
          };
        };

        const createdDataChannel = offerer ? peerConnection.createDataChannel("flow-e2e") : null;
        if (createdDataChannel) {
          dataChannel = createdDataChannel;
          dataChannel.onopen = () => dataChannel.send("flow-e2e-ping");
          dataChannel.onmessage = (message) => {
            if (label === "peer-a" && message.data === "flow-e2e-pong") {
              void finish({ state: peerConnection.connectionState, ice: peerConnection.iceConnectionState });
            }
          };
        }

        peerConnection.onconnectionstatechange = () => {
          if (["failed", "closed"].includes(peerConnection.connectionState)) {
            fail(new Error(`${label} connection state ${peerConnection.connectionState}`));
          }
        };

        socket.onopen = () => {
          try {
            socket.send(JSON.stringify({
              type: "signed_context",
              principal_context: headers["x-flow-principal"],
              timestamp: headers["x-flow-timestamp"],
              signature: headers["x-flow-signature"],
            }));
          } catch (error) {
            fail(error);
          }
        };

        socket.onmessage = (event) => {
          void (async () => {
            try {
              const frame = JSON.parse(event.data);
              if (frame.type === "error") throw new Error(`${label} signaling error: ${frame.code}`);
              if (frame.type === "authenticated") {
                remotePrincipal = frame.peers?.[0]?.principal_id;
                flushCandidates();
                await maybeOffer();
                return;
              }
              if (frame.type === "peer_joined") {
                remotePrincipal = frame.peer.principal_id;
                flushCandidates();
                await maybeOffer();
                return;
              }
              if (frame.type !== "signal") return;
              remotePrincipal = frame.sender;
              if (frame.kind === "offer") {
                await peerConnection.setRemoteDescription({ type: "offer", sdp: frame.payload.sdp });
                remoteDescriptionSet = true;
                for (const candidate of pendingCandidates.splice(0)) {
                  await peerConnection.addIceCandidate(candidate);
                }
                const answer = await peerConnection.createAnswer();
                await peerConnection.setLocalDescription(answer);
                send("answer", remotePrincipal, { sdp: answer.sdp });
              } else if (frame.kind === "answer") {
                await peerConnection.setRemoteDescription({ type: "answer", sdp: frame.payload.sdp });
                remoteDescriptionSet = true;
                for (const candidate of pendingCandidates.splice(0)) {
                  await peerConnection.addIceCandidate(candidate);
                }
              } else if (frame.kind === "ice_candidate") {
                if (!remoteDescriptionSet) pendingCandidates.push(frame.payload);
                else await peerConnection.addIceCandidate(frame.payload);
              }
            } catch (error) {
              fail(error);
            }
          })();
        };

        socket.onerror = () => fail(new Error(`${label} signaling socket error`));
        socket.onclose = (event) => {
          if (!event.wasClean) fail(new Error(`${label} signaling socket closed`));
        };
        return result;
      },
      { connection, headers, offerer, label },
    );

  const first = startPeer(pageA, connectionA, true, "peer-a");
  await sleep(500);
  const second = startPeer(pageB, connectionB, false, "peer-b");
  return Promise.all([first, second]);
}

const chromiumArgs = hostResolverRules ? [`--host-resolver-rules=${hostResolverRules}`] : [];
const browser = await chromium.launch({ headless: true, args: chromiumArgs });
try {
  await requestWithRetry("/health/live", "GET", {});
  const openapi = await requestWithRetry("/openapi.json", "GET", {});
  if (!openapi?.openapi || !openapi?.paths?.["/v1/rooms/{room_id}/join"]) {
    throw new Error("public Flow OpenAPI is missing the room join contract");
  }
  while (Date.now() < deadline) {
    iteration += 1;
    const started = new Date().toISOString();
    try {
      const headers = loadContext();
      const room = await requestWithRetry("/v1/rooms", "POST", headers, {
        mode: "p2p",
        name: `flow-e2e-${Date.now()}`,
        max_participants: 2,
        metadata: { test: "flow-e2e", iteration },
      });
      if (!room?.id) throw new Error("room creation response omitted id");
      const joinBody = { display_name: "flow-e2e" };
      const [joinA, joinB] = await Promise.all([
        requestWithRetry(`/v1/rooms/${room.id}/join`, "POST", headers, joinBody),
        requestWithRetry(`/v1/rooms/${room.id}/join`, "POST", headers, joinBody),
      ]);
      if (joinA.mode !== "p2p" || joinB.mode !== "p2p") throw new Error("join response was not p2p");
      for (const join of [joinA, joinB]) {
        const urls = join.connection?.ice?.ice_servers?.flatMap((server) =>
          (Array.isArray(server.urls) ? server.urls : [server.urls]).filter(Boolean),
        ) || [];
        if (!urls.some((url) => String(url).startsWith("stun:"))) throw new Error("STUN was not issued");
        if (!urls.some((url) => String(url).startsWith("turn:") && String(url).includes("transport=udp"))) {
          throw new Error("UDP TURN was not issued");
        }
        if (!urls.some((url) => String(url).startsWith("turn:") && String(url).includes("transport=tcp"))) {
          throw new Error("TCP TURN was not issued");
        }
      }
      const context = await browser.newContext();
      const pageA = await context.newPage();
      const pageB = await context.newPage();
      try {
        const peers = await connectPeers(pageA, pageB, joinA.connection, joinB.connection, headers);
        if (!peers.every((peer) => peer.state === "connected")) {
          throw new Error(`WebRTC did not reach connected state: ${JSON.stringify(peers)}`);
        }
        passed += 1;
        console.log(JSON.stringify({ iteration, room_id: room.id, started_at: started, status: "passed", peers }));
      } finally {
        await context.close();
      }
    } catch (error) {
      console.error(JSON.stringify({ iteration, started_at: started, status: "failed", error: String(error?.stack || error) }));
      process.exitCode = 1;
      break;
    }
    const remaining = deadline - Date.now();
    if (remaining > 0) await sleep(Math.min(intervalSeconds * 1_000, remaining));
  }
  const elapsedSeconds = Math.round((Date.now() - startedAt) / 1_000);
  console.log(JSON.stringify({ status: passed > 0 && process.exitCode !== 1 ? "passed" : "failed", iterations: iteration, passed, elapsed_seconds: elapsedSeconds }));
} finally {
  await browser.close();
}
