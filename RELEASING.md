# Releasing usher

usher ships as a **single macOS binary** through three channels, all driven by
[`.goreleaser.yaml`](./.goreleaser.yaml):

1. **GitHub Release** — GoReleaser builds `darwin/arm64` + `darwin/amd64`, folds
   them into one universal binary, checksums everything, and attaches the
   tarballs to a `v*` tag.
2. **Homebrew tap** — GoReleaser writes `Casks/usher.rb` into the separate tap
   repo `georgenijo/homebrew-usher`. Users install with
   `brew tap georgenijo/usher && brew install --cask usher`.
3. **curl installer** — [`scripts/install.sh`](./scripts/install.sh) fetches the
   latest release tarball, verifies its SHA-256, and drops the binary into
   `~/.local/bin`.

This is macOS-only on purpose: usher depends on launchd, the macOS Keychain
(`/usr/bin/security`), and the Accessibility tree behind its backends. The build
is pure-Go (`CGO_ENABLED=0`, verified) — no framework linking, so no cgo
toolchain is needed.

---

## TL;DR — the happy path (unsigned)

```sh
# from a clean tree, after the manual one-time setup below
git tag v0.2.0
git push origin v0.2.0     # the release.yml workflow does the rest
```

This produces an **unsigned, un-notarized** release — fine for a local-only
tool, but macOS will Gatekeeper-flag the binary on first run. To ship a release
that runs without the Gatekeeper prompt, do the signed flow under
[Per-release sign + notarize](#per-release-sign--notarize-manual) instead.

---

## One-time setup (MANUAL — needs external accounts)

These steps require things this repo cannot create for you. They are **blocked**
until you do them by hand.

### 1. Create the Homebrew tap repo — BLOCKED (needs a GitHub repo + push)

Create `github.com/georgenijo/homebrew-usher`. It only needs a `Casks/`
directory and a `README.md`. GoReleaser pushes `Casks/usher.rb` into it on each
release. The hand-authored seed in this repo's [`Casks/usher.rb`](./Casks/usher.rb)
can be copied in so the tap is usable before the first automated release (its
version/sha256 are placeholders until then).

```sh
# scaffold the tap once
gh repo create georgenijo/homebrew-usher --public \
  --description "Homebrew tap for usher"
git clone https://github.com/georgenijo/homebrew-usher
mkdir -p homebrew-usher/Casks
cp Casks/usher.rb homebrew-usher/Casks/usher.rb   # seed (placeholder checksums)
# commit + push
```

### 2. Create the tap push token — BLOCKED (needs a GitHub PAT)

Create a fine-grained GitHub PAT with **contents: write** on
`georgenijo/homebrew-usher`. Add it to the **main** repo's Actions secrets as
`HOMEBREW_TAP_GITHUB_TOKEN` (referenced by `.github/workflows/release.yml`).

```sh
gh secret set HOMEBREW_TAP_GITHUB_TOKEN --repo georgenijo/usher
```

### 3. Apple Developer ID — BLOCKED (needs the Apple Developer Program, $99/yr)

Required only for the signed/notarized flow. Obtain a **"Developer ID
Application"** certificate via Xcode / Keychain, and create an **app-specific
password** at appleid.apple.com for `notarytool`. Note your **Team ID**.

The free `macos-latest` GitHub runner does **not** hold your cert, so signing
cannot run in the CI workflow — it must run locally (or on a self-hosted runner
with the cert imported). This is why `release.yml` produces unsigned artifacts.

---

## Per-release sign + notarize (MANUAL)

GoReleaser does **not** sign or notarize. Run this locally, between the tagged
build and the publish. Replace `XXXXXXXXXX` with your Team ID.

```sh
# 1. Build unsigned binaries into dist/ FROM THE REAL TAG (not a snapshot), so
#    -X main.version gets v0.2.0 and the archive names carry the tag version.
#    `--clean` here starts from a fresh dist/ (it must NOT appear in step 4).
git tag v0.2.0
goreleaser build --clean

# 2. Sign each binary with the hardened runtime (required for notarization).
#    Include usher_darwin_all — the universal (fat) binary GoReleaser lipo-folds
#    is a SEPARATE file from the thin ones, so re-signing the thin binaries does
#    not sign it. The curl installer prefers the universal archive, so an
#    unsigned fat binary would be Gatekeeper-flagged on every curl install.
for BIN in dist/usher_darwin_all/usher \
           dist/usher_darwin_arm64_v8.0/usher \
           dist/usher_darwin_amd64_v1/usher; do
  codesign --force --options runtime \
    --sign "Developer ID Application: George Nijo (XXXXXXXXXX)" "$BIN"
done

# 3. Submit each to Apple notarytool and wait for the verdict.
for BIN in dist/usher_darwin_all/usher \
           dist/usher_darwin_arm64_v8.0/usher \
           dist/usher_darwin_amd64_v1/usher; do
  ditto -c -k --keepParent "$BIN" "${BIN}.zip"
  xcrun notarytool submit "${BIN}.zip" \
    --apple-id "george.nijo8@gmail.com" \
    --team-id "XXXXXXXXXX" \
    --password "$APP_SPECIFIC_PASSWORD" --wait
  # Stapling a bare CLI binary is a no-op (only bundles/dmg/pkg can be stapled),
  # so we don't staple here; notarization is recorded server-side and Gatekeeper
  # checks it online on first run.
done

# 4. Publish: GoReleaser reuses the already-built (now signed) dist/ binaries.
#    NO `--clean` here — it would wipe dist/ (and the signatures) before publish;
#    `--skip=build` keeps the signed artifacts and only archives + uploads them.
GITHUB_TOKEN=... HOMEBREW_TAP_GITHUB_TOKEN=... \
  goreleaser release --skip=build
```

> The exact `dist/usher_darwin_*` directory names depend on the GoReleaser
> build id and the Go version's arch suffixes; run `goreleaser build --snapshot`
> once and `ls dist/` to confirm them before scripting the loop.

To confirm Gatekeeper accepts a signed binary — check the universal (it ships via
the curl installer) as well as a thin one:

```sh
spctl --assess --type execute -vv dist/usher_darwin_all/usher
codesign --verify --deep --strict --verbose=2 dist/usher_darwin_all/usher
codesign --verify --deep --strict --verbose=2 dist/usher_darwin_arm64_v8.0/usher
```

---

## Validate before tagging

```sh
goreleaser check                       # lint .goreleaser.yaml against the v2 schema
goreleaser build --snapshot --clean    # confirm both arches + the universal build
dist/usher_darwin_all/usher version    # confirm -X main.version stamping works
```

A pre-release dry run (no publish, but exercises the cask push path against a
real tag) — use a hyphenated tag so `prerelease: auto` marks it pre-release:

```sh
git tag v0.0.1-rc1 && git push origin v0.0.1-rc1
```

---

## User install paths (post-publish)

```sh
# Homebrew (recommended)
brew tap georgenijo/usher
brew install --cask usher
usher install                 # register the launchd LaunchAgent

# curl (no Homebrew)
curl -fsSL https://raw.githubusercontent.com/georgenijo/usher/main/scripts/install.sh | sh
usher install

# from source (developer)
go install github.com/georgenijo/usher/cmd/usher@latest
```

---

## What is intentionally NOT automated

| Step | Why it's manual / blocked |
| --- | --- |
| Code signing (`codesign`) | Needs a Developer ID cert on the build machine. |
| Notarization (`notarytool`) | Needs an Apple Developer account + app-specific password. |
| Creating the tap repo | Needs a GitHub repo created and pushed. |
| Tap push token | Needs a GitHub PAT with `contents: write` on the tap repo. |
| Homebrew bottles | Needs a CI matrix on each targeted macOS version; out of scope for v1. |
| Linux / Windows builds | Out of scope — usher is macOS-only. |

Everything above the line (build, checksum, archive, GitHub Release, cask
generation) is fully automated by `.goreleaser.yaml`. Everything in this table
requires an external account, certificate, or push and is therefore left as a
documented manual step.
