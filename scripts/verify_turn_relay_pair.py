#!/usr/bin/env python3
"""Verify actual bidirectional TURN data between two public relay hosts.

Reads the coturn REST shared secret from stdin; never prints credentials. This
operator-side probe uses a short-lived synthetic principal, not a tenant API
token. Install aioice separately and run it from outside both relay networks.
"""

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import secrets
import sys
import time

from aioice.turn import create_turn_endpoint


class Receiver(asyncio.DatagramProtocol):
    def __init__(self):
        self.received = asyncio.Queue()

    def datagram_received(self, data, addr):
        self.received.put_nowait((data, addr))


async def verify_one(hosts, port, protocol, username, password):
    opened = []
    try:
        for host in hosts:
            endpoint, receiver = await asyncio.wait_for(
                create_turn_endpoint(
                    Receiver, (host, port), username, password,
                    lifetime=60, transport=protocol,
                ),
                timeout=15,
            )
            opened.append((endpoint, receiver))
        addresses = [endpoint.get_extra_info("sockname") for endpoint, _ in opened]
        if any(addr[0] != host for addr, host in zip(addresses, hosts)):
            raise RuntimeError("TURN advertised an unexpected relay address")

        markers = [secrets.token_bytes(24), secrets.token_bytes(24)]
        # A channel binding also creates the TURN permission. Repeated sends
        # allow both sides time to bind before validating data in both directions.
        for _ in range(5):
            for i in range(2):
                opened[i][0].sendto(markers[i], addresses[1 - i])
            await asyncio.sleep(0.4)

        async def receive_expected(receiver, marker):
            while True:
                data, _ = await receiver.received.get()
                if data == marker:
                    return

        await asyncio.wait_for(
            asyncio.gather(
                receive_expected(opened[0][1], markers[1]),
                receive_expected(opened[1][1], markers[0]),
            ),
            timeout=10,
        )
        print(json.dumps({"port": port, "transport": protocol, "bidirectional": True}), flush=True)
    finally:
        for endpoint, _ in opened:
            endpoint.close()
        # Allow deletion requests to be sent before closing the event loop.
        if opened:
            await asyncio.sleep(0.5)


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host_a", help="first relay IPv4 address")
    parser.add_argument("host_b", help="second relay IPv4 address")
    parser.add_argument("--ports", type=int, nargs="+", default=[3478, 3479])
    args = parser.parse_args()
    secret = sys.stdin.buffer.read()
    if len(secret) < 32:
        raise ValueError("TURN secret must contain at least 32 bytes")
    username = f"{int(time.time()) + 180}:operator-probe-{secrets.token_hex(8)}"
    password = base64.b64encode(hmac.new(secret, username.encode(), hashlib.sha1).digest()).decode()
    del secret
    for port in args.ports:
        for protocol in ("udp", "tcp"):
            await verify_one((args.host_a, args.host_b), port, protocol, username, password)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print(json.dumps({"error": type(exc).__name__, "detail": str(exc)}), file=sys.stderr)
        sys.exit(1)
