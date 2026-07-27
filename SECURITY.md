# Security Policy

## Reporting a vulnerability

Open a private security advisory on GitHub (Security → Advisories → *Report a
vulnerability*), or email the maintainer. Please do not open a public issue for
an undisclosed vulnerability.

## Supported versions

Forgepoint is pre-1.0. Fixes land on `main` and ship in the next tag; nothing is
backported. Run the newest published
[release](https://github.com/abd-ulbasit/forgepoint/releases). Treat the gRPC
contracts, the Helm values, and the CLI flags alike as unstable until there is a
`v1.0.0`.

## What CI enforces

The [`Security`](.github/workflows/security.yml) workflow runs on every push to
`main`, every pull request, every `v*` tag, and on demand. Four gates, in order:

| Gate | Tool | Fails on |
|---|---|---|
| Go dependencies | [`govulncheck`](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck) v1.1.4, one leg per module in `go.work` | any **reachable** vulnerability |
| Working tree | Trivy `fs` — `vuln,secret,misconfig` | HIGH / CRITICAL |
| Built image | Trivy `image` — `os,library` | HIGH / CRITICAL |
| Shipped web deps | `npm audit --omit=dev` (in [`CI`](.github/workflows/ci.yml)) | HIGH / CRITICAL |

`govulncheck` is call-graph aware: it reports a vulnerable module only when the
build actually reaches the vulnerable symbol. That is the reason this gate is
worth failing on — it is not "a line in `go.sum` matches an OSV record".

Trivy runs **without** `ignore-unfixed`. An advisory with no upstream fix is
still a real risk and deserves a decision, not a silent pass. There is currently
no `.trivyignore` in this repo and no accepted-advisory allowlist; if one becomes
necessary, each entry must carry a written justification and an expiry date, and
this file is where it gets documented.

Known and deliberately out of scope of the gates above: the build toolchain in
`web/` (vite, esbuild) carries dev-server advisories, and `react-router` 6.x
carries two MODERATE advisories whose only fix is the 7.x major. Neither ships in
the production bundle; the `npm audit` gate is scoped to production dependencies
for that reason, and Dependabot proposes the majors as reviewable PRs.

## Open findings

The Trivy filesystem gate had never executed before 2026-07-27 — it referenced
an action tag that upstream deleted, so the job died in setup. The first run
that got past setup surfaced eight HIGH **IaC misconfigurations**, all
pre-existing. They are listed here rather than suppressed, and the gate stays
red until they are resolved:

| Rule | Where | Fix |
|---|---|---|
| KSV-0014 | `deploy/k8s/infra/postgres/postgres.yaml`, `deploy/backup/postgres-backup-cronjob.yaml`, `deploy/k8s/observability/grafana/grafana.yaml`, `deploy/llm/ollama.yaml` | `securityContext.readOnlyRootFilesystem: true` plus writable `emptyDir` mounts for each image's scratch paths |
| KSV-0118 | `deploy/llm/ollama.yaml` | give the ollama pod/container a non-default `securityContext` |
| AWS-0164 (x3) | `deploy/terraform/modules/network/main.tf:79` | `map_public_ip_on_launch = false` on the public subnets |
| AWS-0132 | `deploy/terraform/modules/state-backend/main.tf:33-41` | SSE-KMS with a customer-managed key instead of SSE-S3 |

Three of the KSV-0014 sites carry in-file comments saying `readOnlyRootFilesystem`
was left unset on purpose because the upstream image does not cleanly support it,
with non-root, `drop: ["ALL"]` and `allowPrivilegeEscalation: false` as
compensating controls. Either those comments or this gate is wrong; resolving
that needs a cluster to test against, so it is open rather than decided.

The `image` job `needs` this one, so build → SBOM → cosign does not run while
these stand. That is the honest state: the signing pipeline is wired and
unblocked at the action level, but it has not yet produced a Rekor entry.

## Supply chain

On a `v*` tag the image job additionally:

1. builds the service image from the repo root context (distroless runtime, no
   shell, no package manager),
2. generates a CycloneDX SBOM with Syft and uploads it as a build artifact,
3. signs the image **by digest** with `cosign` in keyless mode, and
4. attaches the SBOM as a signed CycloneDX attestation.

Keyless means there is no long-lived signing key to leak: cosign exchanges the
workflow's GitHub OIDC token for a short-lived Fulcio certificate, and the
signature plus certificate land in the public Rekor transparency log. Signing
targets the digest rather than a tag, so re-pointing a tag cannot invalidate or
hijack a signature.

To verify a published image:

```sh
cosign verify \
  --certificate-identity-regexp '^https://github.com/abd-ulbasit/forgepoint/\.github/workflows/security\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/abd-ulbasit/forgepoint-<service>@<digest>
```

## Keeping it that way

Dependency drift is what put ten reachable CVEs in this repo in the first place,
so the gates above are paired with [Dependabot](.github/dependabot.yml): weekly
updates for all 15 Go modules, the `web/` npm tree, and every pinned GitHub
Action. Actions are pinned by commit SHA with the version in a trailing comment —
a moved tag cannot swap an action out from under a run, and Dependabot rewrites
the SHA and the comment together so the pin stays a pin.

Run the same Go gate locally before pushing:

```sh
go install golang.org/x/vuln/cmd/govulncheck@v1.1.4
for m in $(awk '/^use \(/{f=1;next} /^\)/{f=0} f{gsub(/[ \t]/,""); if ($0 != "") print}' go.work); do
  ( cd "$m" && govulncheck ./... ) || exit 1
done
```
