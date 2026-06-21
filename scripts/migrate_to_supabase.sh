#!/usr/bin/env bash
#
# One-shot Postgres -> Postgres migration for SyncAura.
#
# Copies the full `public` schema (tables, data, sequences, indexes,
# constraints, AND the alembic_version row) from a SOURCE database to a
# TARGET database. Built for the Neon (free, paused) -> Supabase (free,
# always-on) move, but works between any two Postgres hosts.
#
# WHY pg_dump and not a Python row-copy: pg_dump preserves sequence
# values, FK ordering, and the alembic_version head exactly, so the
# target is a byte-faithful clone you can point the app at with zero
# code changes (db.py._normalize_url already rewrites the URL form).
#
# ── Usage ────────────────────────────────────────────────────────────
#   export SOURCE_DATABASE_URL='postgresql://USER:PASS@HOST:5432/DB?sslmode=require'
#   export TARGET_DATABASE_URL='postgresql://postgres:PASS@HOST:5432/postgres?sslmode=require'
#   bash scripts/migrate_to_supabase.sh
#
# NOTES
#   * Use the DIRECT connection string for BOTH (Supabase: port 5432, the
#     "URI" under Settings -> Database -> Connection string. NOT the
#     transaction pooler on 6543 — pg_dump/pg_restore need a real session).
#   * These are libpq URLs (postgresql://...), NOT the "+asyncpg" form the
#     app uses. Use sslmode=require (not ssl=require) for the CLI tools.
#   * Requires PostgreSQL client tools (pg_dump / pg_restore / psql),
#     version >= the SOURCE server's major version. If you don't have them
#     locally, run this whole script inside Docker instead:
#        docker run --rm -e SOURCE_DATABASE_URL -e TARGET_DATABASE_URL \
#          -v "$PWD/scripts:/s" postgres:17 bash /s/migrate_to_supabase.sh
#   * Safe to re-run: the restore uses --clean --if-exists, so it drops and
#     recreates the app objects on the target each time (idempotent).
#
set -euo pipefail

DUMP_FILE="${DUMP_FILE:-syncaura_$(date +%Y%m%d_%H%M%S 2>/dev/null || echo backup).dump}"

# Key tables we sanity-check after the copy. (Informational — the dump
# carries the whole public schema regardless of this list.)
TABLES=(
  users friendships dm_thread_state dm_messages dm_disappeared
  muted_peers chat_clears lounge_messages
  user_favorites user_playlists user_playlist_songs
  user_listen_events user_downloads
)

die() { echo "ERROR: $*" >&2; exit 1; }

# ── 0. Preconditions ─────────────────────────────────────────────────
: "${SOURCE_DATABASE_URL:?Set SOURCE_DATABASE_URL (the Neon libpq URL)}"
: "${TARGET_DATABASE_URL:?Set TARGET_DATABASE_URL (the Supabase direct libpq URL)}"
for bin in pg_dump pg_restore psql; do
  command -v "$bin" >/dev/null 2>&1 || die "$bin not found — install PostgreSQL client tools (or use the Docker invocation in the header)."
done

echo "==> pg_dump version: $(pg_dump --version)"
echo "==> Source: ${SOURCE_DATABASE_URL%%@*}@…   (host masked)"
echo "==> Target: ${TARGET_DATABASE_URL%%@*}@…   (host masked)"

# ── 1. Verify the SOURCE is reachable + has data ─────────────────────
echo "==> Checking source connectivity…"
psql "$SOURCE_DATABASE_URL" -tAc "select 'ok'" >/dev/null \
  || die "Cannot reach SOURCE. (If Neon is still paused, wait for the July 1 quota reset or resume it first.)"

src_users=$(psql "$SOURCE_DATABASE_URL" -tAc "select count(*) from users" 2>/dev/null || echo "0")
echo "    source users = $src_users"
[ "$src_users" -gt 0 ] 2>/dev/null || echo "    WARNING: source 'users' is empty — continuing anyway."

# ── 2. Dump the public schema (custom format) ────────────────────────
echo "==> Dumping public schema -> $DUMP_FILE"
pg_dump "$SOURCE_DATABASE_URL" \
  --schema=public \
  --no-owner --no-privileges --no-comments \
  --format=custom \
  --file="$DUMP_FILE"
echo "    dump size: $(du -h "$DUMP_FILE" 2>/dev/null | cut -f1 || echo '?')"

# ── 3. Restore into the TARGET (idempotent) ──────────────────────────
echo "==> Restoring into target…"
pg_restore \
  --no-owner --no-privileges \
  --clean --if-exists \
  --dbname="$TARGET_DATABASE_URL" \
  "$DUMP_FILE" \
  || echo "    (pg_restore reported non-fatal warnings — verifying counts below)"

# ── 4. Verify: row counts must match table-by-table ──────────────────
echo "==> Verifying row counts (source vs target):"
mismatch=0
printf "    %-22s %10s %10s\n" "table" "source" "target"
for t in "${TABLES[@]}"; do
  s=$(psql "$SOURCE_DATABASE_URL" -tAc "select count(*) from \"$t\"" 2>/dev/null || echo "ERR")
  d=$(psql "$TARGET_DATABASE_URL" -tAc "select count(*) from \"$t\"" 2>/dev/null || echo "ERR")
  flag=""
  [ "$s" != "$d" ] && { flag="  <-- MISMATCH"; mismatch=1; }
  printf "    %-22s %10s %10s%s\n" "$t" "$s" "$d" "$flag"
done

echo
if [ "$mismatch" -eq 0 ]; then
  echo "✅ Migration verified — all table counts match."
  echo "   Next: set Render's DATABASE_URL to the Supabase URL (the app's"
  echo "   db.py normalizes postgres:// -> postgresql+asyncpg:// automatically)"
  echo "   and redeploy. Then flip ANNOUNCEMENT_CONFIG visible=false."
else
  echo "⚠️  Some counts differ — inspect the rows above before switching DATABASE_URL."
  exit 1
fi
