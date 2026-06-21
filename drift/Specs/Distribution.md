---
component: distribution
status: implemented
files: .goreleaser.yaml, .github/workflows/release.yml
tested: false
---

# Distribution — Homebrew & Releases

Drift ships as a **single self-contained binary** (web UI assets are embedded via
`//go:embed static` in `pkg/web/server.go`), so it distributes cleanly as a
prebuilt download — no runtime deps, no `npm`, no JVM.

Primary channel: **Homebrew**, via [GoReleaser](https://goreleaser.com) + a tap.

## Pipeline

```
git tag vX.Y.Z  →  .github/workflows/release.yml  →  goreleaser release --clean
   ├─ cross-compile cmd/drift: {darwin,linux} × {amd64,arm64}, CGO_ENABLED=0
   ├─ ldflags inject main.version / main.commit / main.date  (→ `drift version`)
   ├─ archives (.tar.gz) + checksums.txt
   ├─ GitHub Release on gribovan2005/drift
   └─ generate Homebrew cask → push to gribovan2005/homebrew-drift (Casks/)
```

Install: `brew install gribovan2005/drift/drift`.

## Key decisions

- **Cask, not formula.** GoReleaser deprecated `brews:` (binary-only formulae);
  current Homebrew wants prebuilt binaries shipped as **casks**. Config uses
  `homebrew_casks:`. `binary:` is auto-detected from the archive (also deprecated
  as an explicit field).
- **Unsigned binaries.** A cask `post.install` hook strips the
  `com.apple.quarantine` xattr so Gatekeeper doesn't block the CLI. (Proper
  codesigning/notarization is a later upgrade.)
- **Separate tap token.** CI's built-in `GITHUB_TOKEN` can't push to a second
  repo, so pushing the cask to `homebrew-drift` needs a PAT in the
  `HOMEBREW_TAP_GITHUB_TOKEN` Actions secret (`repo` scope, or fine-grained
  `contents: rw` on the tap).
- **Version injection.** `cmd/drift/main.go` declares `version/commit/date`
  (defaults `dev`/`none`/`unknown`); GoReleaser overrides them with `-X`.

## One-time maintainer setup

1. Create public repo `gribovan2005/homebrew-drift` (can be empty).
2. Add Actions secret `HOMEBREW_TAP_GITHUB_TOKEN` (PAT with tap write access) to
   the `drift` repo.

## Module-path note

`go.mod` is `github.com/andrejgribov/drift` while the GitHub remote is
`gribovan2005/drift`. GoReleaser builds from the local checkout, so this does
**not** affect Homebrew. It would only break `go install github.com/...@latest`;
rename the module if that channel is wanted later.

## Local dry-run

```bash
goreleaser check                                   # validate config
goreleaser release --snapshot --clean --skip=publish   # full build, no push
```

## See also

- [[CLI & Jobs]] — `drift version`, command surface
- [[Web UI & Builder]] — embedded assets (why the binary is self-contained)
- [[Index]]
