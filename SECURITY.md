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
still a real risk and deserves a decision, not a silent pass. Accepted findings
live in [`.trivyignore.yaml`](.trivyignore.yaml), never in a bare `.trivyignore`
(see "Accepted findings" below).

The filesystem scan also renders the Helm charts (`TRIVY_HELM_KUBE_VERSION`;
before that was set, Trivy failed to render all 12 charts and silently skipped
them) and evaluates the Terraform with `terraform.tfvars.example` (`tf-vars`),
because Trivy does not read `*.example` files on its own.

Known and deliberately out of scope of the gates above: the build toolchain in
`web/` (vite, esbuild) carries dev-server advisories, and `react-router` 6.x
carries two MODERATE advisories whose only fix is the 7.x major. Neither ships in
the production bundle; the `npm audit` gate is scoped to production dependencies
for that reason, and Dependabot proposes the majors as reviewable PRs.

## Triage of the IaC findings (opened 2026-07-27, decided 2026-10-24)

The Trivy filesystem gate had never executed before 2026-07-27 — it referenced
an action tag that upstream deleted, so the job died in setup. The first run
that got past setup surfaced HIGH **IaC misconfigurations**, all pre-existing,
and the gate stayed red on them for three months. Each one now has a decision:

| Rule | Where | Decision |
|---|---|---|
| KSV-0014 | `deploy/k8s/infra/postgres/postgres.yaml` | **Fixed.** Read-only root plus emptyDirs for `/var/run/postgresql` and `/tmp`. The "image cannot run read-only" comment was wrong, and the Kyverno exception for postgres is gone. |
| KSV-0014 | `deploy/k8s/observability/grafana/grafana.yaml` | **Fixed.** Read-only root, `/tmp` emptyDir, `GF_LOG_MODE=console`. |
| KSV-0014, KSV-0118 (x2) | `deploy/llm/ollama.yaml` | **Fixed.** seccomp, drop ALL, no privilege escalation, read-only root. Still uid 0 until the PVC is chowned (see `deploy/llm/README.md`). |
| KSV-0014 | `deploy/backup/postgres-backup-cronjob.yaml` | **Accepted until 2027-01-31** (`.trivyignore.yaml`). The job installs tools at runtime and is broken for other reasons (below); a rewrite fixes both. |
| AWS-0164 (x3) | `deploy/terraform/modules/network/main.tf` | **Fixed.** `map_public_ip_on_launch = false`. NAT uses an EIP; load balancers do not need it. |
| AWS-0132 | `deploy/terraform/modules/state-backend/main.tf` | **Fixed.** SSE-KMS with a dedicated CMK. |
| KSV-0109 | `deploy/helm/fp-ai-gateway/templates/configmap.yaml` | **False positive, accepted until 2027-04-30.** Surfaced only once the charts rendered: the key `FP_TEAM_TOKEN_BUDGET` contains "TOKEN" but holds an integer budget. |

None of the Kubernetes fixes has been run against a cluster yet. Verify postgres
(synced by the `fp-infra` Argo CD app), grafana and ollama after rollout.

**Still red after this triage (2026-10-24):** the same scan reports HIGH
dependency CVEs. They are dependency bumps, left to the open Dependabot PRs so
the two do not conflict, but those PRs do **not** fully clear the gate as they
stand:

| Package | Fixed in | Open Dependabot PR brings it to |
|---|---|---|
| `golang.org/x/crypto` | 0.55.0 | 0.55.0 (#17) |
| `golang.org/x/text` | 0.41.0 | 0.41.0 (#17) |
| `google.golang.org/grpc` | 1.83.1 | 1.83.2 (#17) |
| `golang.org/x/net` | **0.60.0** (one CVE; another is fixed in 0.56.0) | 0.58.0 (#17), still vulnerable |
| `github.com/klauspost/compress` | **1.18.7** | 1.18.6 (#17), still vulnerable |
| `github.com/moby/go-archive` | **0.3.0** | unchanged |
| `axios` (web) | **1.20.0** | 1.19.0 (#10), still vulnerable |
| `follow-redirects` (web) | **1.16.1** | unchanged |

#17 also leaves out `cli/`, `gen/go` and `services/bff`, which carry the same
`x/net`, `x/text` and `grpc` advisories. After merging #17 and #10, bump the
remaining packages by hand (or let Dependabot's next run propose them), then
re-run Security.

**Known defect, not a scanner finding:** `postgres-backup-cronjob.yaml` cannot
succeed as written. It runs as uid 999 but `apk add`s packages and writes `mc`
to `/usr/local/bin`, downloads that binary at runtime without verification,
calls `gpg --passphrase` without `--pinentry-mode loopback`, and dumps database
names (`fp_features`, `fp_experiments`, `fp_notifications`) that
`scripts/init-postgres.sh` does not create. It also depends on `minio/mc`,
which is no longer on Docker Hub. It needs a rewrite (pinned image with the
tools baked in, no runtime downloads, read-only root), not a patch.

## Accepted findings

`.trivyignore.yaml` is the only allowlist, and both the filesystem and the
image scan read it. Each entry must have:

- a `statement` that says why it is a false positive or an accepted risk, and
  what would remove it;
- an `expired_at` date. After that date Trivy stops honouring the entry and the
  gate fails again, which forces a fresh decision rather than a permanent mute;
- the narrowest scope possible (`paths`, or a specific package/CVE), never a
  rule ID for the whole repo.

## Supply chain

Every image is `ghcr.io/abd-ulbasit/forgepoint/<service>`, the name the Helm
charts, the Rollouts and the Kyverno allowlist use. The pipeline is split so
that nothing reaches the registry unscanned:

1. **`image`** (every run, `contents: read` only): builds the service image from
   the repo root context (distroless runtime, no shell, no package manager) into
   a local tarball, scans **that tarball** with Trivy (HIGH/CRITICAL fail), and
   generates a CycloneDX SBOM of it with Syft. It never pushes.
2. **`publish`** (pushes to `main` and `v*` tags only; the only job with
   `packages: write` and `id-token: write`): refuses a `v*` tag whose commit is
   not on `main`, checks the tarball's config digest against the one that was
   scanned, pushes it, checks the registry's manifest still points at that
   config, then signs the image **by digest** with `cosign` in keyless mode and
   attaches the SBOM as a signed CycloneDX attestation.

Pull requests build and scan but never push or sign. Every published image,
`main` builds included, is signed, so the cluster can require a signature on
any image from this repository.

Keyless means there is no long-lived signing key to leak: cosign exchanges the
workflow's GitHub OIDC token for a short-lived Fulcio certificate, and the
signature plus certificate land in the public Rekor transparency log. Signing
targets the digest rather than a tag, so re-pointing a tag cannot invalidate or
hijack a signature.

To verify a published image:

```sh
cosign verify \
  --certificate-identity-regexp '^https://github\.com/abd-ulbasit/forgepoint/\.github/workflows/security\.yml@refs/(heads/main|tags/v.*)$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/abd-ulbasit/forgepoint/<service>@<digest>
```

Use `@refs/tags/v.*$` instead to accept release builds only.

## Keeping it that way

Dependency drift is what put ten reachable CVEs in this repo in the first place,
so the gates above are paired with [Dependabot](.github/dependabot.yml): weekly
updates for all 15 Go modules, the `web/` npm tree, every pinned GitHub Action,
the digest-pinned Docker base images and deploy images (`docker`, `helm`,
`docker-compose`), and the Terraform providers with their lock files. Database
majors (postgres, redis) are excluded: those are migrations, not bumps.
Actions are pinned by commit SHA with the version in a trailing comment —
a moved tag cannot swap an action out from under a run, and Dependabot rewrites
the SHA and the comment together so the pin stays a pin.

Run the same Go gate locally before pushing:

```sh
go install golang.org/x/vuln/cmd/govulncheck@v1.1.4
for m in $(awk '/^use \(/{f=1;next} /^\)/{f=0} f{gsub(/[ \t]/,""); if ($0 != "") print}' go.work); do
  ( cd "$m" && govulncheck ./... ) || exit 1
done
```
