#!/usr/bin/env node
// Run two relay-only Chromium peers against one TURN host. Read the coturn REST
// secret from stdin; do not persist or print the secret or derived credential.
import { createHmac, randomBytes } from "node:crypto";
import { createRequire } from "node:module";
import process from "node:process";

const host = process.argv[2];
if (!host || !/^\d+\.\d+\.\d+\.\d+$/.test(host)) throw new Error("pass the TURN IPv4 address");
const pieces = [];
for await (const part of process.stdin) pieces.push(part);
const secret = Buffer.concat(pieces);
if (secret.length < 32) throw new Error("TURN secret must contain at least 32 bytes");
const username = `${Math.floor(Date.now() / 1000) + 300}:browser-probe-${randomBytes(8).toString("hex")}`;
const credential = createHmac("sha1", secret).update(username).digest("base64");
secret.fill(0);
const { chromium } = createRequire(import.meta.url)(process.env.PLAYWRIGHT_MODULE || "playwright");
const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
try {
  for (const port of [3478, 3479]) {
    for (const transport of ["udp", "tcp"]) {
      const page = await browser.newPage();
      try {
        const result = await page.evaluate(async ({ host, port, transport, username, credential }) => {
          const server = { urls: `turn:${host}:${port}?transport=${transport}`, username, credential };
          const a = new RTCPeerConnection({ iceServers: [server], iceTransportPolicy: "relay" });
          const b = new RTCPeerConnection({ iceServers: [server], iceTransportPolicy: "relay" });
          const pendingA = [];
          const pendingB = [];
          a.onicecandidate = ({ candidate }) => {
            if (candidate) (b.remoteDescription ? b.addIceCandidate(candidate) : pendingB.push(candidate));
          };
          b.onicecandidate = ({ candidate }) => {
            if (candidate) (a.remoteDescription ? a.addIceCandidate(candidate) : pendingA.push(candidate));
          };
          const received = new Promise((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error("relay-only data channel timed out")), 30000);
            const channel = a.createDataChannel("turn-probe");
            channel.onopen = () => channel.send("request");
            channel.onmessage = ({ data }) => {
              if (data === "response") { clearTimeout(timer); resolve(); }
            };
            b.ondatachannel = ({ channel: remote }) => {
              remote.onmessage = ({ data }) => { if (data === "request") remote.send("response"); };
            };
          });
          try {
            await a.setLocalDescription(await a.createOffer());
            await b.setRemoteDescription(a.localDescription);
            for (const candidate of pendingB.splice(0)) await b.addIceCandidate(candidate);
            await b.setLocalDescription(await b.createAnswer());
            await a.setRemoteDescription(b.localDescription);
            for (const candidate of pendingA.splice(0)) await a.addIceCandidate(candidate);
            await received;
            const report = await a.getStats();
            const selected = [...report.values()].find((item) => item.type === "transport" && item.selectedCandidatePairId);
            const pair = selected && report.get(selected.selectedCandidatePairId);
            const local = pair && report.get(pair.localCandidateId);
            const remote = pair && report.get(pair.remoteCandidateId);
            if (local?.candidateType !== "relay" || remote?.candidateType !== "relay") {
              throw new Error("selected candidate pair is not relay-to-relay");
            }
            if (local.relayProtocol && local.relayProtocol !== transport) {
              throw new Error("selected relay transport differs from the requested transport");
            }
            return { port, transport, local: local.candidateType, remote: remote.candidateType,
                     relayProtocol: local.relayProtocol || null, bidirectional: true };
          } finally {
            a.close();
            b.close();
          }
        }, { host, port, transport, username, credential });
        console.log(JSON.stringify(result));
      } finally {
        await page.close();
      }
    }
  }
} finally {
  await browser.close();
}
