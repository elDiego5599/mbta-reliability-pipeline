# Security review

Date: 2026-11-25. Reviewed commit: `9265901` (main, clean worktree).

## Scope and method

A lightweight review in the guidance mode of Cloudflare's `security-audit`
skill (github.com/cloudflare/security-audit-skill, commit `c1c8a8c`), adapted
to what this project is: a local data pipeline that reads public MBTA feeds and
writes files under `data/`. There is no listening server, no login, no secret
and no second user. The skill's rule applies: a finding needs a lower-trust
actor, a crossed boundary and a concrete result; anything else is hardening.

Trust boundaries that exist:

1. The MBTA CDN (`https://cdn.mbta.com`), read over TLS, is the only source of
   untrusted bytes.
2. GitHub: who can change `main`, and what CI may do with its token.
3. The Mac: launchd runs code from the main checkout as the owner's user.

Checks run:

- `uvx pip-audit` 2.10.1 on the 18 packages exported from `uv.lock`.
- `gitleaks` 8.30.1 over all 40 commits on every ref, and over the working tree.
- Read-only `gh api` calls for the repository's security settings.
- Source reading of `fetch.py`, `static.py`, `bronze.py`, `silver.py`,
  `poller.py`, `ops/launchd/*` and `.github/workflows/ci.yml`.
- The live feeds fetched once through the patched code (sizes below).

No confirmed vulnerability was found. Everything below is hardening, ranked by
how much it matters for this project.

## Findings

### 1. Low, fixed: no size limit on downloaded or unpacked data

**Where:** `fetch.py` read the whole body with `response.content`; `static.py`
read every zip member with no cap on member count or unpacked size.

**Risk:** only the MBTA CDN (or someone able to break TLS to it) controls these
bytes. A huge body, a gzip-encoded body that expands on decoding (httpx decodes
`Content-Encoding` transparently), or a zip bomb would exhaust memory. The
poller is `KeepAlive` in launchd, so it would crash and restart every 30
seconds, losing realtime data that cannot be fetched again.

**Fix (this branch):**

- `Feed.max_bytes`: realtime feeds 32 MiB, static zip 256 MiB. The body is
  streamed and the limit counts decoded bytes, so a gzip bomb stops at the
  limit. An oversized body is logged as error `ResponseTooLarge` and not
  retried (the poller records it as `failed`).
- `read_tables` rejects a zip with more than 200 members or whose declared
  unpacked size is above 1 GiB, before reading any member. Python's `zipfile`
  stops each member at its declared size and fails the CRC check if the data is
  longer, so the declared size is a real bound; a test pins that.
- Measured today: VehiclePositions 67 KB, TripUpdates 764 KB, Alerts 248 KB,
  static zip 19.2 MB, 138 MB unpacked in 32 members. The limits leave 7 to 40
  times headroom.

**Tests:** `tests/test_fetch.py` (at the limit, above the limit, gzip bomb,
default limits) and `tests/test_static_tables.py` (unpacked size, member count,
understated member size).

### 2. Low, fixed: CI actions pinned by tag, token scope implicit

**Where:** `.github/workflows/ci.yml`.

**Risk:** `actions/checkout@v7` and `astral-sh/setup-uv@v10.4.0` are mutable
tags; whoever controls those repositories could change the code CI runs. The
workflow had no `permissions:` block. The repository default is already
read-only (checked with `gh api`), so the token could not write today, but that
depended on a setting outside the code. CI has no secrets, so the impact was
small.

**Fix (this branch):** `permissions: contents: read` at the top level; both
actions pinned to full commit SHAs with the version in a comment;
`persist-credentials: false` on checkout so the token is not left in
`.git/config`; `.github/dependabot.yml` updates actions and `uv.lock` monthly so
the pins do not rot. `tests/test_ci.py` keeps these in place.

### 3. Low, owner action: GitHub settings

Current state (read-only check): repository public; secret scanning and push
protection already **enabled**; Dependabot security updates **disabled**;
Dependabot alerts **disabled**; `main` has **no protection**; any action is
allowed and SHA pinning is not required.

Why `main` matters here: launchd runs whatever the main checkout holds, and that
checkout follows `main` on every `git pull`. These are for the owner to change in
the repository settings:

1. **Settings > Advanced Security:** enable Dependabot alerts and Dependabot
   security updates. Optionally enable non-provider patterns for secret
   scanning.
2. **Settings > Rules > Rulesets:** a ruleset for `main` that requires a pull
   request, requires the `test` status check, and blocks force pushes and
   deletion.
3. **Settings > Actions > General:** after this branch is merged, tick
   "Require actions to be pinned to a full-length commit SHA" and allow only
   actions by GitHub plus `astral-sh/setup-uv`.
4. Leave "Workflow permissions" on read-only (it already is).

### 4. Informational: launchd agents

Reviewed `install.sh`, `uninstall.sh` and the three templates. No issue:

- The agents load into the `gui/<uid>` domain and run as the owner's user. No
  `sudo`, no root, no `UserName` key.
- `ProgramArguments` is an array, so no shell parses it; paths are absolute and
  built from `pwd -P`.
- `install.sh` refuses a repository path with `&`, `<` or `>`, the characters
  that would break the plist XML or the Bash replacement.
- The plists only name binaries inside `.venv`, and `install.sh` checks they
  exist before loading.

Note: whoever can write to `~/gtfs-realtime` can run code as the owner at the
next restart. That is the same user, so it is not a boundary, but it is why the
checkout must only ever pull reviewed commits from `main`.

Not security, but worth a later task: `data/logs/*.log` grow without rotation.
Resolved on 2026-11-25: each job rotates its own log at 10 MB, keeping five old files.

### 5. Informational: secrets

`gitleaks` found nothing in history or the working tree. No code reads a key,
token or environment variable. `.gitignore` covers `data/`, `.env*`, key files
and DuckDB files. Secret scanning with push protection is on for new pushes.

### 6. Informational: dependencies

`pip-audit` reports no known vulnerability in the 18 locked packages. `uv.lock`
carries hashes and CI installs with `uv sync --locked`. Dependabot (finding 2
and 3) keeps this current.

### 7. Informational: handling of downloaded data

- **Zip slip:** not possible. Members are never written to disk; each name must
  match `^[a-z0-9_]+\.txt$` (no `/`, no `..`) and a test covers `../evil.txt`.
  Output paths come from the SHA-256 of the zip and fixed file names.
- **Bronze paths:** built from a constant feed name and the integer header
  timestamp, never from strings in the payload.
- **Content:** CSV values are stored as strings in Parquet and never evaluated;
  protobuf is parsed by the `upb` runtime with its default recursion limit.
- **Transport:** HTTPS with certificate checks on (httpx default) and redirects
  not followed (httpx default).
- **Silver:** `zstd` decompression only reads bronze files this pipeline wrote.

## Not covered

No full six-phase audit, no fuzzing of the protobuf or CSV parsers, no review of
the dependencies' own code, and nothing yet for the planned dbt, Dagster and
Evidence stages. Revisit when the dashboard is published, since a public static
site adds a new boundary.
