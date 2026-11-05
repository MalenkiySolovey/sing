# Extended sing source authority

Source authority: https://github.com/shtorm-7/sing

Pristine base: `fe045639087fe73d1412b99f09486bbef96433db`, tag
`v0.9.7-extended-1.2.1`. The module path remains `github.com/sagernet/sing`.
The GitHub fork-network parent does not change this selected source lineage.

The `solovey/extended-patches` queue starts at that exact base. Official continues
to consume its own upstream graph; it does not inherit this queue.

| Patch | Problem / ownership | Risk / interop | Regression | Upstream / removal |
|---|---|---|---|---|
| SING-EXTENDED-0001 | CachedPacketConn read, cached handoff and Close raced over one owned buffer during TrustTunnel UDP revocation. Buffer lifetime belongs to this module. Serialize only the in-memory cache; upstream I/O is outside the lock. | LOW, generic lifecycle; INTEROP_NOT_REQUIRED, packet bytes/framing unchanged | Existing common/bufio/net_test.go TestCachedPacketConnOwnership; pristine race and duplicate ownership reproduced, focused common/bufio race suite passes after fix | UPSTREAM_READY_NOT_SUBMITTED; remove after an equivalent source-authority fix passes this regression |
| SING-EXTENDED-0002 | LazyConn replaced its embedded connection and handshake flag while HTTP shutdown called Close or inspected state. The TLS wrapper owner must publish its completed handshake atomically while Close can interrupt an in-flight handshake. | LOW-MEDIUM, generic lifecycle; INTEROP_NOT_REQUIRED, TLS framing and established TLS Close ownership unchanged | common/tls/listener_test.go concurrent handshake/state/Close and blocked-handshake cancellation; Panel XHTTP packet-up CI race was the reproducer | UPSTREAM_READY_NOT_SUBMITTED after focused regression/CI; remove after an equivalent Shtorm fix passes both source regressions and affected Panel XHTTP/TLS fallback integration |

Consumer: Solovey Extended Provider, using an immutable revision selected by its
module graph. Panel owns selection/provenance, not a replayable copy of this fix.

Update: obtain an exact pristine Shtorm revision, replay this linear queue, inspect
both textual conflicts and semantic obsolescence, drop equivalent upstream fixes,
run the focused source regression and affected Panel integration, then advance
the immutable consumer pin. No upstream submission has been authorized.

License/provenance: retain the inherited LICENSE and source notices unchanged.
This repository preserves the selected Shtorm history and original attribution.
