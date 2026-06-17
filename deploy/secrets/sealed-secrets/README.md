# Sealed Secrets — homelab / k3s (no-cloud) secrets path

This is the GitOps-safe secrets mechanism for the **local k3s / homelab cluster**,
which has **no AWS** (so the External Secrets Operator path in
[`../external-secrets/`](../external-secrets/) cannot work — there is no Secrets
Manager backend and no IRSA/OIDC to authenticate to it).

See the parent [`../README.md`](../README.md) for the cloud-vs-local decision and
the cardinal rule: **no plaintext secret is ever committed to Git.**

## The idea in one paragraph

A plaintext (or merely `base64`-encoded) Kubernetes `Secret` committed to Git is a
credential leak — base64 is encoding, not encryption. Sealed Secrets fixes this
with **asymmetric encryption**: the in-cluster controller holds a private key and
publishes the matching public key. You encrypt your Secret **locally** with
`kubeseal` against that public key, producing a `SealedSecret` whose `encryptedData`
is ciphertext only the controller can decrypt — and only inside this cluster. You
commit the `SealedSecret`. The controller watches for it and decrypts it in-cluster
into a normal `Secret`. Git therefore holds only ciphertext: **safe to commit.**

```
  you (laptop)                     Git repo            k3s cluster
  ┌───────────────┐                                    ┌─────────────────────────┐
  │ plaintext     │  kubeseal      ┌────────────────┐  │ sealed-secrets          │
  │ Secret (NEVER ├──(encrypt)────►│ SealedSecret   ├─►│ controller (private key)│
  │ committed)    │  pub key       │ (ciphertext) ✅│  │   └─ decrypts ──► Secret │
  └───────────────┘                └────────────────┘  │                  fp-auth │
        ▲                                               └─────────────────────────┘
        └─ exists only here + in your shell; deleted after sealing
```

## One-time: install the controller (NOT done by this repo)

This repo does **not** install operators. On the k3s cluster an operator installs
the Sealed Secrets controller once, e.g.:

```bash
helm repo add sealed-secrets https://bitnami-labs.github.io/sealed-secrets
helm install sealed-secrets sealed-secrets/sealed-secrets \
  -n kube-system
```

The controller generates its key pair on first start. **Back up that private key**
(it is the only thing that can decrypt your SealedSecrets); losing it means
re-sealing everything against a fresh key.

## How to seal a Secret and commit it safely

The golden rule: **the plaintext only ever exists in your shell and is piped
straight into `kubeseal` — it is never written to a committed file.**

```bash
# 1) Build the plaintext Secret IN MEMORY (--dry-run, never applied, never saved).
#    Use a non-committed file or env vars for the actual values. Here we read the
#    JWT key from a freshly generated value and the DSN from an env var so neither
#    is typed into a file that could be committed.
kubectl create secret generic fp-auth \
  --namespace fp-system \
  --from-literal=FP_JWT_SECRET="$(openssl rand -base64 48)" \
  --from-literal=FP_DATABASE_URL="$FP_DATABASE_URL" \
  --dry-run=client -o yaml \
| \
# 2) Pipe that plaintext straight into kubeseal, which encrypts it against the
#    controller's PUBLIC key and emits a SealedSecret (ciphertext). --format yaml
#    for a committable manifest. (kubeseal fetches the pub key from the cluster;
#    use --cert <file> to seal offline against a saved cert.)
kubeseal \
  --controller-name sealed-secrets \
  --controller-namespace kube-system \
  --format yaml \
> sealedsecret-fp-auth.yaml

# 3) Inspect: sealedsecret-fp-auth.yaml contains ONLY encryptedData ciphertext.
#    There is NO plaintext anywhere in it. THIS file is safe to commit.
git add deploy/secrets/sealed-secrets/sealedsecret-fp-auth.yaml
```

The plaintext Secret from step 1 was never written to disk — it lived only in the
pipe. Do **not** redirect step 1 to a file and commit it.

## Applying (operator action — NOT done here)

ArgoCD (or `kubectl apply -f`) syncs the committed `SealedSecret`. The controller
decrypts it into a `Secret` named `fp-auth` in `fp-system`. The fp-auth Helm chart
is then installed with the same external-secret wiring as the cloud path:

```yaml
# fp-auth values (k3s)
secrets:
  create: false          # chart renders NO Secret of its own
  existingSecret: fp-auth # the Deployment envFrom's the controller-decrypted Secret
```

Because both paths land on a `Secret` named `fp-auth` with keys `FP_JWT_SECRET` and
`FP_DATABASE_URL`, **the application and the chart are identical across cloud and
homelab** — only the mechanism that fills the Secret differs.

## Files here

- [`sealedsecret-fp-auth.example.yaml`](./sealedsecret-fp-auth.example.yaml) — an
  **example** with placeholder (non-real) ciphertext. Sealed values are bound to
  ONE controller's key, so you cannot reuse someone else's blob: **regenerate with
  the steps above against your own cluster.**

## Key facts to remember

- **Safe to commit:** ciphertext only; useless without the controller's private key.
- **Per-cluster:** a SealedSecret only decrypts in the cluster whose key sealed it.
- **Strict scope (default):** ciphertext only decrypts into a Secret of the **same
  name + namespace**, blocking re-use under a different name.
- **Back up the controller's private key**, or you re-seal everything on key loss.
- **Rotation is manual:** to change a value, re-run the seal flow and commit the new
  SealedSecret. (ESO's auto-refresh from a cloud store is one reason cloud uses ESO.)
