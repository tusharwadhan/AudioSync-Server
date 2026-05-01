# SyncAura — Auth & Postgres Setup (Phase 1)

This is the operator's checklist for bringing the new authenticated /
cloud-sync surface online. It only covers what's new in Phase 1: Firebase
Auth + Neon Postgres + the `users` table. Existing yt-dlp / room-manager
endpoints are unchanged and don't depend on any of this.

## Required environment variables

| Var | Purpose | Required when |
|---|---|---|
| `DATABASE_URL` | Neon (or any Postgres) connection string. Either `postgres://`, `postgresql://`, or `postgresql+asyncpg://` is accepted; `sslmode=require` is auto-rewritten to `ssl=require`. | Always (any DB-backed endpoint, including `/auth/sync`) |
| `FIREBASE_CREDENTIALS` | Firebase Admin service account JSON, as a **single-line string**. Used to verify Google Sign-In ID tokens and (separately) to send FCM. | Always |
| `SYNCAURA_API_KEY` | Existing API key gate. Unchanged. | Always |

If `FIREBASE_CREDENTIALS` is unset, the server falls back to the local file
`audiosync-dfee2-firebase-adminsdk-fbsvc-4fe0940bca.json` for dev, but the
deployed environment should always set the env var.

## One-time setup

### 1. Neon (or any Postgres host)

1. Create a project at https://neon.tech.
2. Copy the **pooled** connection string from the dashboard.
3. Set it as `DATABASE_URL` on the deploy target (Render / Railway / etc.)
   and in your local `.env` for testing.

### 2. Firebase Console

1. In the existing `audiosync-dfee2` Firebase project, go to
   **Authentication → Sign-in method** and enable **Google**.
2. **Project Settings → General → Your apps**: confirm the Android app
   `com.syncaura.music` is registered. Add the SHA-1 fingerprints for
   both your debug keystore and the release keystore (`syncaura-release.jks`).
3. Download the updated `google-services.json` and replace
   `app/google-services.json` in the Android project.
4. **Project Settings → Service accounts**: generate a new private key
   (JSON). The same service account is reused for FCM and ID-token
   verification — paste its contents (single line, no surrounding quotes
   needed) as `FIREBASE_CREDENTIALS` on the server.

### 3. Apply migrations

From `server/`, with `DATABASE_URL` set:

```bash
alembic upgrade head
```

This creates the `users` table and `ix_users_email` index. New migrations
land in `server/alembic/versions/`; rerun the same command after pulling
to keep your DB current.

To author a new migration after editing `models.py`:

```bash
alembic revision --autogenerate -m "short description"
```

## What's wired up

- **`POST /api/v1/auth/sync`** — accepts `Authorization: Bearer <Firebase ID token>`,
  verifies the token, upserts the matching row in `users`, and returns
  `{uid, email, display_name, photo_url, created}`. The client should call
  this once per cold start and after profile changes.
- **`db.py`** — async SQLAlchemy engine + `get_session()` FastAPI dependency.
  Lazy-initialized; the server still boots if `DATABASE_URL` is unset, but
  any endpoint that depends on `get_session` will 500 with a clear message.
- **`auth.py`** — `get_current_user` FastAPI dependency. Returns
  `AuthedUser` (TypedDict) on success, raises 401 with a useful detail
  message otherwise.
- **`models.User`** — Firebase UID is the primary key. Phase 2+ tables will
  FK to `users.id`.

## Smoke test

```bash
# Replace TOKEN with a real Firebase ID token from a signed-in client.
curl -sS -X POST \
  -H "X-API-Key: $SYNCAURA_API_KEY" \
  -H "Authorization: Bearer $TOKEN" \
  https://your-host/api/v1/auth/sync
```

Expected: `200 OK` with the user payload on first call (`created: true`),
and `created: false` on subsequent calls.
