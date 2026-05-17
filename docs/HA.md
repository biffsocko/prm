# PRM High Availability — Operator Runbook (slice 2 / Tier 2)

This document covers the **hot-standby active-passive** topology: two PRM processes pointed at a Postgres primary with a streaming-replication standby, fronted by an L4 load balancer. RPO is zero for committed Postgres writes; RTO is under 60 seconds end-to-end.

For the design rationale and the full 4-tier redundancy plan, see [DESIGN.md](../DESIGN.md#high-availability-and-disaster-recovery).

## What "leader" means in PRM

Only the prmd instance holding the Postgres advisory lock serves traffic. The other process is a hot standby: connected, ready, but blocked on its own `pg_try_advisory_lock` attempt. There is no quorum and no consensus protocol — Postgres itself is the source of truth for "who is leader."

```
            Clients
               │
       ┌───────▼────────┐
       │  L4 Load LB    │   health check: /healthz returns 200
       │                │   only when /pg_advisory_lock held
       └───┬─────┬──────┘
           │     └ failover only (when active health goes red)
           ▼
   ┌─────────────┐    ┌─────────────┐
   │ prmd active │    │ prmd standby│
   │ holds lock  │    │ waits lock  │
   └──────┬──────┘    └──────┬──────┘
          │                  │ (on promotion)
          ▼                  ▼
   ┌─────────────────────────────┐
   │  PostgreSQL primary         │
   │  ─── streaming replication ──▶ standby
   │  ── continuous WAL ship ────▶ object storage
   └─────────────────────────────┘
```

## Lock key

PRM uses a fixed 64-bit advisory lock key: **`0x70726D5F6861`** (ASCII `prm_ha`). Different deployments can override this in the prmd config; just make sure both processes in a pair agree.

## What happens on failover

1. The active prmd's Postgres connection drops (or the process dies).
2. The advisory lock is released by Postgres (session-scoped).
3. The standby's polling `pg_try_advisory_lock` succeeds on its next attempt (default poll interval 1s).
4. The standby's `/healthz` flips from 503 to 200.
5. The L4 load balancer's next health check sees the flip and routes new traffic to the standby.
6. Clients with the active connection see EOF/RST → their reconnect-with-backoff kicks in (TUI does this automatically; bot SDKs should implement the same pattern).
7. Within ~10–30 seconds of the failure, traffic is flowing through the new active and clients have re-authenticated + re-joined channels.

In-memory channel state (members, presence) is **cold on the standby**. It rebuilds from `channel_acl` (durable) plus reconnecting clients.

## Operator setup checklist

### Postgres side

1. **Primary** running Postgres 15+.
2. **Synchronous streaming replication** to a standby. The exact mechanism (Patroni, custom scripts, managed RDS / Cloud SQL replicas) is out of scope; PRM doesn't care as long as the standby has zero-loss durability and the failover promotion is automated.
3. **WAL archiving** to S3 / MinIO / Cloud Storage. Continuous, ideally on every WAL segment switch (target a few seconds of RPO).
4. **A dedicated PRM Postgres user** with `CREATE` on the prm schema and `EXECUTE` on `pg_try_advisory_lock` / `pg_advisory_unlock` (default for non-superusers).

### PRM side

1. Two prmd processes running on different hosts:

   ```
   prmd serve --addr :6697 --cert ./cert.pem --key ./key.pem \
              --storage postgres://prm:secret@pg-primary:5432/prm?sslmode=verify-full
   ```

2. Both prmds point at the **same Postgres primary**. On failover, both will follow the new primary (handled by your Postgres failover mechanism).
3. The advisory lock key is currently a build-time constant. Future config will expose it.

### Load balancer side

1. L4 (TCP) load balancer in front of both prmds on the realtime port (6697).
2. Health check probes a separate `/healthz` HTTP endpoint on each prmd (slice 3 will expose this on the REST control plane port).
3. Health check passes only when the prmd holds the advisory lock. Standby returns 503 unless/until it acquires.
4. Health check interval: 1–2 seconds. Failover detection time is ~(probe interval × failures-to-trigger) + (lock acquire poll interval). Aim for <30 seconds total.

## Verifying the failover

1. Stand up the pair as above.
2. Connect a TUI client: `prm --insecure pg-active.example.com:6697 acme alex general`.
3. Send a few messages to confirm the active path works.
4. SSH to the active prmd host and `kill -9 $(pgrep prmd)`.
5. Within 30 seconds the TUI should:
   - Show "disconnected: ... (reconnecting...)"
   - Show "RECONNECTING (attempt 1)" then 2, etc.
   - Show "** reconnected; rejoining #general **"
6. Resume sending messages on the same connection (visually — under the hood it's a brand-new TLS session against the standby).
7. Bring the original active host back up as the new standby. The roles have swapped.

## Restore-from-backup procedure (Tier 1 fallback)

If both prmds are unrecoverable but the Postgres backup is good:

1. Restore the most recent base backup + WAL to a fresh Postgres host:
   ```bash
   pg_basebackup --target dest/ --waldir wal/ \
     ...  # restore from S3 / MinIO archive
   # apply WAL segments, then start Postgres
   ```
2. Spin up a single prmd pointed at the restored Postgres. It'll immediately take the advisory lock (no contention since the old leaders are gone).
3. Resume serving. Clients reconnect.

**Test this procedure monthly.** Backups that haven't been restored are theater. Add a cron / GitHub Action that restores the latest archive to a scratch Postgres, spins up a prmd against it, verifies a known-good test tenant comes up, then tears it down.

## What's still manual

- Postgres-side failover automation (Patroni or equivalent) is not provided by PRM. Use what your infrastructure standardizes on.
- `prmd /healthz` endpoint is not yet implemented (slice 3); slice 2 verifies leadership via the advisory lock from inside the prmd process. Until /healthz lands, the load balancer can health-check by attempting a TLS connection + Hello frame and treating timeout-or-close as unhealthy.
- Cross-region geo-replication (Tier 4) is not in scope here. See the deferred-features section of [DESIGN.md](../DESIGN.md).
