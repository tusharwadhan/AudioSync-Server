# SyncAura DB migration → Supabase (runbook)

**Why:** Neon free tier caps **compute-hours**, and our always-on WebSocket
server keeps Postgres awake 24/7, so it burns the monthly quota and pauses
(this is what took social down). Supabase free pauses only after **7 days of
inactivity** — a live app never triggers it — so it's structurally immune to
this failure. 500 MB storage vs our ~34 MB is plenty.

The app needs **no code change** to switch: [db.py](db.py) `_normalize_url()`
already rewrites `postgres://` → `postgresql+asyncpg://` and `sslmode=require`
→ `ssl=require`. You only change the `DATABASE_URL` env var on Render.

---

## Do NOW (while Neon is still paused)

1. **Create the Supabase project** — https://supabase.com → New project (free).
   - Pick the region closest to the Render server (Neon SyncAura was **US-West / Oregon**, so choose **West US** to keep latency low).
   - Set a strong DB password; save it.
2. **Grab the DIRECT connection string** — Project → Settings → Database →
   *Connection string* → **URI**, **port 5432** (the direct one, *not* the
   6543 transaction pooler — migrations and asyncpg want a real session).
   It looks like:
   `postgresql://postgres:<PASS>@db.<ref>.supabase.co:5432/postgres?sslmode=require`
3. Keep that URL handy for July 1. (Don't commit it anywhere — it's a secret.)

---

## Do on JULY 1 (Neon free quota resets on the 1st → data reachable again)

1. **Get Neon's direct connection string** — Neon console → SyncAura →
   Connection details → the `psql`/URI string (libpq form, port 5432).
2. **Run the migration** (from `server/`):
   ```bash
   export SOURCE_DATABASE_URL='postgresql://…neon…:5432/neondb?sslmode=require'
   export TARGET_DATABASE_URL='postgresql://postgres:…@db.<ref>.supabase.co:5432/postgres?sslmode=require'
   bash scripts/migrate_to_supabase.sh
   ```
   No local PostgreSQL tools? Run it in Docker (no install needed):
   ```bash
   docker run --rm -e SOURCE_DATABASE_URL -e TARGET_DATABASE_URL \
     -v "$PWD/scripts:/s" postgres:17 bash /s/migrate_to_supabase.sh
   ```
   The script dumps the whole `public` schema (tables + data + sequences +
   `alembic_version`), restores it into Supabase, and verifies every table's
   row count matches. It's idempotent — safe to re-run.
3. **Point Render at Supabase** — Render dashboard → the server service →
   Environment → set `DATABASE_URL` to the Supabase **direct** URL → save
   (triggers a redeploy).
4. **Turn the announcement off** — in [main.py](main.py) set
   `ANNOUNCEMENT_CONFIG["visible"] = False`, commit + push (HTTP/1.1).
5. **Verify** — open the app: friends + presence load, send a test DM. Or
   check `/api/v1/update/check` (always up) and watch the WS stop EOF-looping.

---

## Option: bring social back SOONER (don't wait for July 1)

If you don't want ~13 days of downtime and can accept the existing social
graph resetting (friends/chats/lounge history wiped — a young user base just
re-adds friends):

1. Create the Supabase project (steps above).
2. Create the schema fresh on it from this repo:
   ```bash
   cd server
   export DATABASE_URL='<supabase direct URL>'
   alembic upgrade head
   ```
3. Set Render's `DATABASE_URL` to Supabase, redeploy, flip the announcement off.

Social is back immediately on a host that won't pause; users start with an
empty friend list. (Synced library — favorites/playlists/downloads — also
starts empty since it lives in the same DB.)

---

## After the move (recommended hardening)
- Keep **Neon as a synced standby** and add the multi-URL auto-failover in
  `db.py` (health-check + switch + periodic one-way sync) so a single host
  going down no longer takes social offline.
- Reduce DB wake-ups (cache presence in memory; avoid a query on every
  heartbeat) so even a capped free tier lasts far longer.
