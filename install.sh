#!/bin/sh
# Installs the berthq binary from a GitHub release.
#
#   curl -fsSL https://raw.githubusercontent.com/talovicnedim/berthq/main/install.sh | sh
#
# BERTHQ_VERSION picks a release (default: the latest, e.g. v0.1.0) and BERTHQ_INSTALL_DIR the
# folder (default: /usr/local/bin if writable, else ~/.local/bin). BERTHQ_RELEASES points at
# another copy of the releases (a mirror). Nothing runs with sudo.
#
# Everything is inside main, so a download cut off halfway runs nothing.

set -eu

releases="${BERTHQ_RELEASES:-https://github.com/talovicnedim/berthq/releases}"

fail() {
	echo "berthq install: $*" >&2
	exit 1
}

need() {
	command -v "$1" >/dev/null 2>&1 || fail "needs $1"
}

# The archive name for this machine, as release.yml builds them.
platform() {
	os=$(uname -s)
	arch=$(uname -m)
	case "$os" in
	Darwin) echo macos-universal ;;
	Linux)
		# The Linux binaries link glibc (2.35+); on musl (Alpine) use the Docker image instead.
		if ldd --version 2>&1 | grep -qi musl; then
			fail "Linux with musl (e.g. Alpine) isn't supported; use the Docker image ghcr.io/talovicnedim/berthq"
		fi
		case "$arch" in
		x86_64 | amd64) echo linux-x86_64 ;;
		aarch64 | arm64) echo linux-arm64 ;;
		*) fail "no Linux build for $arch" ;;
		esac
		;;
	*) fail "no build for $os; see https://github.com/talovicnedim/berthq#install" ;;
	esac
}

# The newest release's tag, from where /releases/latest redirects (no API rate limit).
latest() {
	url=$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$releases/latest") ||
		fail "couldn't find the latest release at $releases"
	tag="${url##*/}"
	case "$tag" in
	v*) echo "$tag" ;;
	*) fail "no release found at $releases" ;;
	esac
}

sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | cut -d ' ' -f 1
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$1" | cut -d ' ' -f 1
	else
		fail "needs sha256sum or shasum to check the download"
	fi
}

install_dir() {
	if [ -n "${BERTHQ_INSTALL_DIR:-}" ]; then
		echo "$BERTHQ_INSTALL_DIR"
	elif [ -d /usr/local/bin ] && [ -w /usr/local/bin ]; then
		echo /usr/local/bin
	else
		echo "$HOME/.local/bin"
	fi
}

main() {
	need curl
	need tar
	name=$(platform)
	version="${BERTHQ_VERSION:-$(latest)}"
	case "$version" in
	v*) ;;
	*) version="v$version" ;;
	esac
	archive="berthq-$version-$name.tar.gz"

	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' EXIT

	echo "Downloading berthq $version ($name)"
	curl -fsSL -o "$tmp/$archive" "$releases/download/$version/$archive" ||
		fail "couldn't download $archive; is $version a release?"
	curl -fsSL -o "$tmp/$archive.sha256" "$releases/download/$version/$archive.sha256" ||
		fail "couldn't download $archive.sha256"

	expected=$(cut -d ' ' -f 1 <"$tmp/$archive.sha256")
	actual=$(sha256 "$tmp/$archive")
	[ "$expected" = "$actual" ] || fail "checksum mismatch for $archive: expected $expected, got $actual"

	tar -xzf "$tmp/$archive" -C "$tmp" berthq
	# Run it before replacing anything: a binary this machine can't run (e.g. a glibc older than
	# 2.35) must not overwrite a working one.
	installed=$("$tmp/berthq" --version 2>&1) ||
		fail "the downloaded berthq doesn't run here (Linux builds need glibc 2.35+): $installed"
	dir=$(install_dir)
	mkdir -p "$dir"
	# Move into place in one step, so a running berthq keeps its old binary.
	cp "$tmp/berthq" "$dir/.berthq.new"
	chmod 755 "$dir/.berthq.new"
	mv -f "$dir/.berthq.new" "$dir/berthq"

	echo "Installed $installed to $dir/berthq"
	case ":$PATH:" in
	*":$dir:"*) ;;
	*) echo "Add $dir to your PATH, e.g.: export PATH=\"$dir:\$PATH\"" ;;
	esac
	echo "Start it with: berthq start"
}

main "$@"
