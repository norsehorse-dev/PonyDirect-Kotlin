# PonyDirect-Kotlin

The Kotlin/JVM implementation of [PonyDirect](../PonyDirect), a small,
dependency-free peer-to-peer transport for already-paired peers. It delivers
opaque application bytes two ways: **LAN-direct** (find a paired peer by mDNS and
exchange sealed envelopes over TCP) and **WAN-direct** (reach a paired peer across
the internet by STUN + UDP hole punching, the app's own channel used only to
introduce the two peers).

Both paths authenticate the peer with a short handshake keyed on a symmetric secret
the two sides already share. PonyDirect carries opaque bytes; **the application owns
the encryption and the key exchange.** In [CarrierPony](https://carrierpony.com) the
bytes are already-sealed (roster-hiding) envelopes.

It is a plain `kotlin("jvm")` library: **no Android, no `Context`.** The transport
is pure JVM (`java.net` UDP sockets); local-network discovery (mDNS via
`NsdManager`) is supplied by the app through the `Discovery` interface. It builds
and tests on any JDK 17+, which is what lets an app that ships it still go through
F-Droid's build server.

## The boundary

- `PonyDirectKeyProvider` - a stable 32-byte symmetric key per peer (both sides
  derive the same value out of band). Used only for the handshake and probe MACs.
- `PonyDirectSignaling` - the app's own confidential channel for relaying the WAN
  offer/answer/ICE candidates.
- `PonyDirectEnvelopeSink` - where delivered payloads arrive.
- `PonyDirectDiscovery` - app-supplied mDNS discovery.

## Status

Early. The wire crypto, framing, and the application boundary are in place and
unit-tested, byte-compatible with the Swift implementation per
[`WIRE-PROTOCOL.md`](WIRE-PROTOCOL.md). The LAN and WAN transports land as they
stabilize against CarrierPony.

## License

Apache-2.0. See `LICENSE` and `NOTICE`.
