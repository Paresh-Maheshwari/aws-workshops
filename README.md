# aws-workshops

Every **online, hands-on AWS workshop** that is still open to register for, on one
page, in your own timezone.

**→ [paresh-maheshwari.github.io/aws-workshops](https://paresh-maheshwari.github.io/aws-workshops/)**

AWS runs free hands-on workshops through [aws-experience.com](https://aws-experience.com),
but they are scattered across eleven regional portals with no combined view and no
filter for "virtual and hands-on". This harvests all of them every six hours and
renders a single searchable page.

The whole thing is one `main.go` — harvester, UI and HTTP server — with no
dependencies outside the Go standard library, no template files and no database.
The published site is a single static HTML file with the listing baked in.

## What gets listed

An event appears only if it is **all four** of:

| | |
|---|---|
| `virtual` | not in-person |
| `handsonworkshop` | not a webinar or briefing |
| `status=live` | not draft or cancelled |
| starts in the future | not already begun |

Popular workshops recur, and the same title legitimately runs on several portals
with different hosts. Every run is listed separately and never merged — see
[De-duplication](#de-duplication) for why that matters.

## Run it locally

```bash
go build -trimpath -ldflags="-s -w" -o awsworkshops main.go
./awsworkshops                  # http://127.0.0.1:8765
./awsworkshops -open            # ...and open a browser
```

First start takes about ten seconds: it makes 22 requests, paced deliberately
because the origin throttles bursts.

Cross-compiling needs nothing special, as there is no cgo:

```bash
GOOS=windows GOARCH=amd64 go build -o awsworkshops.exe main.go
GOOS=darwin  GOARCH=arm64 go build -o awsworkshops-mac main.go
```

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `127.0.0.1` | interface to bind. Local-only by default |
| `-port` | `8765` | port |
| `-refresh` | `6h` | background re-harvest interval; `0` disables |
| `-open` | `false` | open the page once it is ready |
| `-dump FILE` | — | write harvested JSON and exit, no server |
| `-static FILE` | — | write the rendered page and exit, no server |
| `-version` | — | print version |

## Routes

Only when running as a server. The static build has no origin behind it.

| Route | Purpose |
|---|---|
| `/` | the page, data injected inline |
| `/api/workshops` | the same data as JSON |
| `/refresh` | re-harvest now |
| `/healthz` | count, build time, per-endpoint warnings |

## Two output modes

The page template is shared; `renderPage` decides what chrome to include, so the
two modes cannot drift apart.

| | live server (`/`) | static build (`-static`) |
|---|---|---|
| Refresh button | shown, calls `/refresh` | omitted, no endpoint exists |
| JSON link | `/api/workshops` | omitted, the page is the only artefact |
| Footer panel | "This server", lists the routes | "This page", explains it is a scheduled snapshot |

Nothing in the static page contacts an origin at load time, so it works from any
file server or CDN.

## Deployment

`.github/workflows/deploy.yml` harvests and publishes on a six-hourly cron, on
push to `main`, and on manual dispatch. Enable it with
**Settings → Pages → Source: GitHub Actions**.

No secrets or tokens are needed. The AWS Experience API is public and
unauthenticated, and Pages deployment uses the built-in `GITHUB_TOKEN` through
`permissions:`.

Two details worth knowing:

- **The repo must be public.** GitHub Pages is public-repos-only on GitHub Free;
  private Pages needs Pro. The workflow reads its own visibility at runtime and
  skips the deploy job while private, so it still goes green and still attaches
  the built page as a downloadable artifact. That is the way to test before going
  public.
- **The binary is cached** on `hashFiles('main.go', 'go.mod')`, so the four daily
  cron runs skip the Go toolchain entirely and only re-harvest.

The workflow refuses to publish a page that is empty, still contains
unsubstituted `__PLACEHOLDER__` tokens, or reports zero workshops.

## Notes

Things that are non-obvious, and were established by measurement rather than
assumption.

### De-duplication

De-duplication is a **guard, not a filter**. Across 827 harvested records there
were 827 distinct `id`s, so keying on `id` removes nothing.

Do not "improve" this by keying on title, or even title + start:

- 56 titles legitimately run on several portals
- 7 title+start pairs are backed by two different `id`s

*Fundamentals of Amazon EKS* alone had 7 records with 7 distinct ids and 7
distinct slugs, two of them starting at the same instant with different speaker
rosters. Merging on anything but `id` silently deletes real, bookable sessions.

### Ordering

Sorted by start, then title, then id. `sort.Slice` is not stable, so without the
explicit tie-breaks events sharing a start time would shuffle between harvests
and produce noise in every diff.

### Timezones: detected, not chosen

The record carries only UTC instants (`start`, `end`). Every date, weekday and
clock time is derived in the browser against the zone the reader's own machine
reports:

```js
Intl.DateTimeFormat().resolvedOptions().timeZone
```

That reads the OS clock setting. No request, no IP geolocation, no control to
operate and nothing persisted. `Asia/Kolkata` is only a fallback for an engine
that reports nothing usable.

Because the zone is applied per instant rather than as a fixed offset, DST
resolves correctly on both sides of a transition: `Europe/London` renders
24 Oct 2026 as 13:00 BST and 26 Oct as 12:00 GMT.

Moving this derivation out of Go was verified before the change, not after.
`Intl` in `Asia/Kolkata` reproduced Go's `time/tzdata` output on all 274 records
for `day`, `time`, `weekday` and `dayLabel`, with zero mismatches. The pipeline
was then re-checked against `America/Los_Angeles`, `Europe/London`, `Asia/Tokyo`,
`Pacific/Auckland` and `UTC` — zero mismatches each, with day groupings that
shift as expected (57, 55, 63, 62, 55 days).

`time/tzdata` is still imported because the Go side formats the build stamp and
the harvest log in IST.

Beware that zone ids have aliases: an engine with older ICU reports
`Asia/Kolkata` as `Asia/Calcutta`. Both format identically, so nothing special is
needed now that there is no list to de-duplicate against — but never compare zone
ids as raw strings.

The badge in the standfirst names the resolved zone so the reader can see which
clock the times belong to. Its label uses `timeZoneName:'shortGeneric'`, which
gives "India Time" or "PT" where `'short'` would give a bare `GMT+5:30`.

### Event links

Links must use `/e/<slug>`. The `/events/<slug>` form returns HTTP 200 but
renders the generic portal calendar, which looks like a working link and is not.
Around 13 records carry no slug upstream, so those get a clearly-labelled
portal-calendar fallback rather than a dead link.

### Throttling

The origin drops bursts, so fetches run at concurrency 3 with 200 ms pacing and
up to 3 attempts with backoff. `/healthz` lists any endpoint that still failed. A
failed background refresh keeps the previous data rather than emptying the page.

### Capacity

Seat counts are the organiser's stated capacity, not live availability. Check the
event page before relying on them.

### No authentication

There is none. The default bind of `127.0.0.1` means only your machine can reach
the local server; binding anywhere else exposes the page to everyone who can
reach the host, and the program warns when you do.

### One quirk of the embedded UI

Go raw string literals cannot contain a backtick, which is why the embedded
JavaScript uses string concatenation instead of template literals.

## Licence

Event data belongs to Amazon Web Services and comes from aws-experience.com.
This is an unofficial view and is not affiliated with AWS.
