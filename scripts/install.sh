#!/bin/sh
# curl installer for usher (#21). Fetches the latest GitHub release tarball,
# verifies its SHA-256 against the published checksums file, and drops the
# binary into ~/.local/bin. macOS-only — usher targets launchd / the Keychain /
# the Accessibility tree, so there is no Linux/Windows artifact to install.
#
#   curl -fsSL https://raw.githubusercontent.com/georgenijo/usher/main/scripts/install.sh | sh
#
# Override the install dir with USHER_BIN_DIR, or a specific version with
# USHER_VERSION (e.g. USHER_VERSION=v0.2.0).
set -eu

REPO="georgenijo/usher"
BIN_DIR="${USHER_BIN_DIR:-$HOME/.local/bin}"

# macOS only.
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
if [ "$OS" != "darwin" ]; then
  echo "usher: this installer is macOS-only (got $OS)" >&2
  exit 1
fi

# Map the host arch to the names GoReleaser uses. The release ships a universal
# (fat) binary as the "all" archive that runs on both, so we prefer that and
# fall back to the host-specific thin archive if "all" is absent.
case "$(uname -m)" in
  arm64 | aarch64) HOST_ARCH=arm64 ;;
  x86_64 | amd64)  HOST_ARCH=amd64 ;;
  *) echo "usher: unsupported arch $(uname -m)" >&2; exit 1 ;;
esac

# Resolve the version: explicit override, else the latest release tag.
if [ -n "${USHER_VERSION:-}" ]; then
  VERSION="$USHER_VERSION"
else
  VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep '"tag_name"' \
    | head -n1 \
    | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')"
fi
if [ -z "$VERSION" ]; then
  echo "usher: could not determine the release version" >&2
  exit 1
fi

# Archives are named usher_<ver>_darwin_<arch>.tar.gz with the leading 'v'
# stripped from the version (GoReleaser's default {{.Version}}).
VER="${VERSION#v}"
BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
CHECKSUMS="usher_${VER}_checksums.txt"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "Fetching usher ${VERSION} checksums…"
curl -fsSL "${BASE_URL}/${CHECKSUMS}" -o "${TMP}/${CHECKSUMS}"

# Prefer the universal ("all") archive; fall back to the host arch if a release
# was built thin-only. Pick whichever name actually appears in the checksums.
TARBALL=""
for arch in all "$HOST_ARCH"; do
  candidate="usher_${VER}_darwin_${arch}.tar.gz"
  if grep -q " ${candidate}\$" "${TMP}/${CHECKSUMS}" || grep -q "  ${candidate}\$" "${TMP}/${CHECKSUMS}"; then
    TARBALL="$candidate"
    break
  fi
done
if [ -z "$TARBALL" ]; then
  echo "usher: no darwin archive for ${VERSION} found in ${CHECKSUMS}" >&2
  exit 1
fi

echo "Downloading ${TARBALL}…"
curl -fsSL "${BASE_URL}/${TARBALL}" -o "${TMP}/${TARBALL}"

# Verify the checksum. shasum ships on macOS; -c reads the GNU two-space format
# GoReleaser emits. Restrict the check to our one tarball line.
echo "Verifying SHA-256…"
( cd "$TMP" && grep " ${TARBALL}\$" "${CHECKSUMS}" | shasum -a 256 -c - )

# Unpack and install.
tar -xzf "${TMP}/${TARBALL}" -C "$TMP"
mkdir -p "$BIN_DIR"
install -m 0755 "${TMP}/usher" "${BIN_DIR}/usher"

echo ""
echo "usher ${VERSION} installed to ${BIN_DIR}/usher"
case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *) echo "note: ${BIN_DIR} is not on your PATH — add it to your shell profile." ;;
esac
echo "Next: register the always-on daemon with 'usher install'."
