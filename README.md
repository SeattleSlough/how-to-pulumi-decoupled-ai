# Decoupled AI Factory Implementation Blueprint

This repository provides an automated Infrastructure-as-Software blueprint for deploying a **decoupled AI stack** across AWS, Databricks, and CoreWeave using Pulumi and GitHub Actions.

**Note:** This document assumes working familiarity with Pulumi concepts (stacks, outputs, providers, `pulumi.Config`) and focuses on the architectural decisions specific to this cross-cloud pattern. For Pulumi fundamentals, see [pulumi.com/docs](https://pulumi.com/docs/).

---

## 1. Objective: What Are We Building?

This blueprint bridges the gap between enterprise data governance and high-density, low-cost GPU compute.

By uncoupling your data lakehouse from your compute engine, you maintain enterprise data security and line-of-sight governance inside **AWS and Databricks**, while running high-density AI training/inference workloads on **CoreWeave's bare-metal NVIDIA H100 clusters**. Pulumi serves as the unified cross-cloud control plane that coordinates networking, zero-trust dynamic credentials, and automated infrastructure teardowns.

### Key Technical Outcomes

* **Zero Static Keys:** Dynamic OIDC token federation using **Pulumi ESC**.
* **Isolated Cloud Lifecycles:** Decoupled stacks using **Pulumi Stack References** to separate long-lived storage from ephemeral hardware.
* **Runaway Cost Prevention:** Programmatic failure traps for explicit crashes, plus a separate TTL kill-clock (Section 6a) that bounds the cost of a workload that hangs silently with no error at all.
* **A Real, Encrypted Cross-Cloud Network Path:** A Megaport private circuit between AWS and CoreWeave, with MACsec encryption enforced on the AWS-facing hop — provisioned as part of the permanent data plane, not assumed to exist.

---

## 2. Architectural Overview

```text
               ┌─────────────────────────────────────────────────────────┐
               │              AWS / DATABRICKS PERIMETER                 │
               │                                                         │
               │   ┌─────────────────────┐     ┌─────────────────────┐   │
               │   │  AWS S3 LAKEHOUSE   │     │  AWS S3 MODEL STORE │   │
               │   │ (Mosaic Data Shard) │     │ (Epoch Checkpoints) │   │
               │   └──────────┬──────────┘     └──────────▲──────────┘   │
               └──────────────┼───────────────────────────┼──────────────┘
                              │                           │
                1. OUTBOUND   │                           │   2. INBOUND
               Micro-Batches  │                           │  Checkpoint Sync
               (Streaming API)│                           │  (Boto3 / AWS CLI)
                              ▼                           │
               ┌──────────────────────────────────────────┴──────────────┐
               │               COREWEAVE GPU CLUSTER                     │
               │         (reached via a Megaport private circuit)        │
               │        [ NVIDIA H100 Node + Local NVMe Scratch ]        │
               └─────────────────────────────────────────────────────────┘
```

Two Pulumi stacks implement this: `infra_data_platform` (permanent) and `infra_ml_compute` (ephemeral). The sections below walk through each piece of the lifecycle — why it's needed, and a representative snippet of how it's built. Full implementations are linked inline; none of the code below is complete or meant to be copy-pasted as-is.

---

## 3. Environment & Identity Federation

**Why:** Storing static AWS or CoreWeave tokens in CI/CD secrets or state files creates an audit liability — anyone with read access to the state store effectively has standing account access.

**How:** Pulumi ESC opens a short-lived OIDC session with AWS, then reuses that same session to authenticate every downstream secret fetch — so nothing in this pipeline ever touches a long-lived credential:

```yaml
# ESC-env-def.yaml (excerpt)
values:
  aws:
    login:
      fn::open::aws-login:
        oidc:
          duration: 2h
          roleArn: arn:aws:iam::123456789012:role/PulumiEscGitHubRole
          sessionName: pulumi-esc-session
    secrets:
      fn::open::aws-secrets:
        region: us-east-1
        login: ${aws.login}      # reuses the OIDC session above
        get:
          coreweaveKubeconfig:
            secretId: prod/coreweave/kubeconfig
```

Full definition: [`ESC-env-def.yaml`](./ESC-env-def.yaml).

---

## 4. Permanent Data Plane

**Why:** S3 storage, the Databricks workspace, Unity Catalog, and the network bridge to CoreWeave should never be torn down when a GPU training run finishes — they outlive any individual job.

**How:** `infra_data_platform` provisions that permanent footprint and exports the handles the compute stack will need, including the Megaport private circuit that actually connects the two clouds:

```python
# infra_data_platform/__main__.py (excerpt)
megaport_provider = megaport.Provider("mp-provider", api_token=megaport_token)

cross_cloud_bridge = megaport.Vxc(
    "aws-coreweave-private-pipe",
    rate_limit=10000,
    a_end_mcr_id="your-aws-direct-connect-gateway-id",
    b_end_mcr_id="coreweave-datacenter-pop-id",
    opts=pulumi.ResourceOptions(provider=megaport_provider),
)

pulumi.export("lakehouse_url", external_data_volume.url)
pulumi.export("checkpoint_bucket_id", checkpoint_bucket.id)
```

This stack also carries a cost-control lifecycle policy on the checkpoint bucket: data rescued off a frozen node by `train.yml`'s failure branch (see Section 6) lands under three top-level prefixes — `rescue-unverified/`, `rescue-verified/`, and `rescue-logs/` — with unvalidated checkpoint data auto-expiring after 30 days and logs after 90, rather than accumulating indefinitely on the assumption someone remembers to clean it up. Verified checkpoint data is left alone, since someone may actually want to resume a run from it:

```python
# infra_data_platform/__main__.py (excerpt)
aws.s3.BucketLifecycleConfigurationV2(
    "checkpoint-bucket-lifecycle",
    bucket=checkpoint_bucket.id,
    rules=[
        aws.s3.BucketLifecycleConfigurationV2RuleArgs(
            id="expire-unverified-rescue-data",
            status="Enabled",
            # Classification has to be the leading path segment - S3
            # lifecycle rules only match literal string prefixes, so a
            # rule targeting "rescue/unverified/" would never match keys
            # like "rescue/<incident>/unverified/..." where a timestamp
            # sits in between.
            filter=aws.s3.BucketLifecycleConfigurationV2RuleFilterArgs(
                prefix="rescue-unverified/",
            ),
            expiration=aws.s3.BucketLifecycleConfigurationV2RuleExpirationArgs(days=30),
        )
        # A second rule (90-day expiration on rescue-logs/) is defined
        # alongside this one - see the full file.
    ],
)
```

Full implementation: [`infra_data_platform/__main__.py`](./infra_data_platform/__main__.py).

### Encrypting the AWS-Facing Hop of the Bridge

**Why:** A private circuit isn't the same claim as an encrypted one. Megaport's `Vxc` resource has no encryption property of its own — MACsec is a physical-link-layer feature configured on the AWS Direct Connect connection itself, not something Megaport's SDN exposes.

**How:** `aws.directconnect.Connection` is provisioned with `request_macsec=True` and `encryption_mode="must_encrypt"`, so AWS terminates and enforces encryption on its end; Megaport's circuit carries the already-encrypted frames transparently:

```python
# infra_data_platform/__main__.py (excerpt)
aws_dx_connection = aws.directconnect.Connection(
    "aws-coreweave-dx-connection",
    bandwidth="10Gbps",  # MACsec requires a dedicated 10Gbps+ connection
    location="EqDA2",
    request_macsec=True,
    encryption_mode="must_encrypt",
)
```

This is a real physical link, not something a single `pulumi up` fully activates: MACsec is only available at select Direct Connect locations, and `encryption_mode` only takes effect once the connection reaches an "Available" state after its physical cross-connect is installed at a colocation facility.

---

## 5. Ephemeral Compute Plane

**Why:** GPU compute is the most expensive layer of this architecture, and unlike the data platform, it should be provisioned just-in-time, not held on standby. It also depends on storage endpoints the data platform stack already owns (the lakehouse URL, the checkpoint bucket); duplicating that configuration here instead of reading it live would create two sources of truth that can quietly drift out of sync as the data stack evolves.

**How:** `infra_ml_compute` reads the data stack via `pulumi.StackReference`, built dynamically so it always points at the environment currently in use. This repo provisions a single representative training node to demonstrate the pattern — a full cluster deployment replicates this same `Pod` definition N times, each reading from the same `StackReference`:

```python
# infra_ml_compute/__main__.py (excerpt)
env = pulumi.get_stack()
data_platform = pulumi.StackReference(f"your-org/data-platform/{env}")

inbound_stream_endpoint = data_platform.get_output("lakehouse_url").apply(
    lambda url: f"{url}?mode=streaming&framework=mosaic_streaming"
)
```

The training pod itself is defined with Pulumi's typed Kubernetes SDK classes rather than raw dicts — a malformed field gets caught when the code is written, not when it's deployed:

```python
# infra_ml_compute/__main__.py (excerpt)
containers=[
    k8s.core.v1.ContainerArgs(
        name="mosaic-training-runner",
        resources=k8s.core.v1.ResourceRequirementsArgs(
            limits={"nvidia.com/gpu": "8", "cpu": "128", "memory": "1000Gi"},
        ),
        # ...
    )
]
```

Full implementation: [`infra_ml_compute/__main__.py`](./infra_ml_compute/__main__.py).

---

## 6. The Failure Trap

**Why:** Assume a 32-node (256-GPU) H100 cluster for a pretraining run. That cluster collectively bills between $1,500+/hr (on-demand) and $600/hr (aggressive committed-capacity discount) which is real cost. A blind teardown on any non-zero exit code destroys the exact NVMe contents a data scientist needs to recover a run, and the logs and live node state a devops engineer needs to debug a crash. Conversely, a blind *lack* of teardown on success leaves that idle cluster billing indefinitely.  Both of these scenarios need to be addressed.

**How:** The container entrypoint checks its own exit status before deciding whether to release the node. This blueprint provisions a single representative training node — a full 32-node cluster deployment replicates this same pod definition N times, with each node running this identical check independently:

```bash
python3 -m llm_train.launch --config training_config.yaml   # placeholders for model related assets
STATUS=$?
if [ $STATUS -eq 0 ]; then
    echo 'SUCCESS: Checkpoints safely pushed to AWS S3.' > /dev/termination-log
    exit 0
else
    echo 'CRITICAL FAILURE: Locking compute & NVMe states.' > /dev/termination-log
    while true; do sleep 3600; done   # holds the node open for inspection
fi
```

On success, the pipeline's next step runs `pulumi destroy` immediately. On failure, it copies the exact NVMe contents to S3, alerts, and leaves the node in this hold state until someone runs the manual cleanup workflow. Full context: [`infra_ml_compute/__main__.py`](./infra_ml_compute/__main__.py).

Several assumptions/principles underpin what happens on failure:

* **Identifying hang is external test script:** the trap fires on any non-zero exit — it assumes the training script itself can distinguish a true failure from ordinary slow-but-healthy progress (a long optimization step, a slow epoch) and only exits non-zero for the former. This blueprint doesn't implement that distinction; it lives in `training-model.yaml` and the training script's own error handling, neither of which is part of this repo.

* **Automate data capture:** Copying the NVMe contents is a non-destructive and reversible action and this is data various teams will likely want to access and evaluate. Rather than leave this very likely step to some downstream action - be it automated or manual - it makes sense to immediately and automatically execute this upon failure.

* **Scope of "failure":** the trap fires on any non-zero exit — it assumes the training script itself can distinguish a true failure from ordinary slow-but-healthy progress (a long optimization step, a slow epoch) and only exits non-zero for the former. This blueprint doesn't implement that distinction; it lives in `training-config.yaml` and the training script's own error handling, neither of which is part of this repo.
* **This is a data-preservation window, not a live debugging session.** By the time the trap fires, the training process has already exited — there's nothing running to attach a debugger to. The hold loop keeps the *node* alive so an engineer can retrieve forensic evidence (logs, local NVMe state) and diagnose root cause; any fix is deployed as a new run against the last good checkpoint, not applied to resume the frozen one in place. This repo also doesn't implement that resume path today — checkpoint frequency, and any logic to reload from one, live entirely in the training script's own config.
* **The rescue copy and the destroy are deliberately separate, with different triggers.** Copying data off the node is non-destructive and reversible, so it runs automatically the moment `train.yml` detects a crash — no one has to remember to trigger it, and an engineer isn't racing a clock to rescue data before deciding whether to tear the node down. Destroying the node is irreversible, so that step stays gated behind a human running `cleanup.yml` manually, after any live inspection they want to do. `train.yml`'s automatic rescue step assumes the training script writes a checkpoint marker: it only trusts a checkpoint file as verified if a matching `.done` marker exists next to it, written by the training script *after* a confirmed-complete flush. This blueprint doesn't implement that marker-writing logic — it assumes the data science team's training code does. Without it, every rescued checkpoint file is copied to `rescue-unverified/` rather than treated as safe to resume from, since a file copied mid-write can look complete without actually being valid. Container log output and any log files the training script writes to disk are captured separately under `rescue-logs/`, classified by directory location (`/mnt/local/logs/`) rather than filename pattern — a `.done` marker doesn't mean anything for a log file the way it does for a checkpoint shard, so this blueprint also assumes the training script writes its own logs to that specific subdirectory; without that convention, log files would have no `.done` marker either and would fall into `rescue-unverified/` alongside genuinely unverified checkpoint data.
* **This pattern doesn't extend to multi-node coordination.** Real distributed training requires all nodes to stay synchronized through a shared process group; if one node's failure trap fires, the others don't fail independently — they're already blocked waiting on it. Detecting and recovering from a single node's failure inside a live 32-node run needs a gang-scheduled job controller (e.g. Kubeflow's `PyTorchJob`), which this repo does not implement.

---

## 6a. TTL Kill-Clock: Bounding the Cost of a Silent Hang

**Why:** The failure trap in Section 6 only fires when the training script itself exits non-zero — a genuine crash. It does nothing for the harder, more expensive case: a process that never exits at all, stuck with no error to report. Nothing upstream — not the failure trap, not `train.yml`'s `kubectl logs -f` step — can distinguish that from a slow-but-healthy run, so nothing reclaims it, and the cluster keeps billing.

**How:** `pulumiservice.TtlSchedule` — a real Pulumi Cloud resource, not custom code — schedules an automatic destroy N hours after deploy, regardless of the pod's state at that point:

```python
# infra_ml_compute/__main__.py (excerpt)
ttl_schedule = pulumiservice.TtlSchedule(
    "training-node-ttl",
    organization=pulumi.get_organization(),
    project=pulumi.get_project(),
    stack=pulumi.get_stack(),
    timestamp=destroy_at,  # now + ttlHours, computed at deploy time
    delete_after_destroy=False,
)
```

`TtlSchedule` executes on Pulumi's own hosted runners (Pulumi Deployments) rather than this repo's GitHub Actions, so the kill-clock still fires even if the GitHub Actions run that created it has long since finished — which is also why `pulumiservice.DeploymentSettings` has to be configured first; see the comments in `infra_ml_compute/__main__.py` for the full prerequisite chain.

**What this deliberately doesn't do:** extend the clock for a run that's still legitimately healthy past that window. Doing that safely needs a health signal from inside the training process — a heartbeat file, a liveness check tied to real training progress — that this blueprint doesn't implement, the same category of assumption as the `.done` checkpoint marker in Section 6: it depends on data-science-side instrumentation this repo doesn't own. If you add that signal, the extension pattern is a second scheduled step that reads it and, when healthy, calls the Pulumi Cloud Schedules API's TTL update endpoint to push `destroy_at` forward using this resource's `schedule_id` output.

Full implementation: [`infra_ml_compute/__main__.py`](./infra_ml_compute/__main__.py).

**Interaction with the failure trap (Section 6) worth knowing:** the TTL kill-clock has no awareness of *why* a node is still running — it fires at its scheduled time whether the pod is healthy, hung, or deliberately held open by the failure trap for crash forensics. If an engineer doesn't run `cleanup.yml` before the TTL expires, Pulumi Cloud destroys the node anyway, taking the held-open NVMe state and live node access with it. The TTL bounds cost; it does not defer to an in-progress human investigation. If that's not the behavior you want, either set `ttlHours` generously enough to cover a realistic inspection window, or extend `cleanup.yml`/the TTL schedule to cancel or push back the clock as soon as a human starts investigating a crash - this repo doesn't implement that coordination.

---

## 7. Shift-Left Unit Verification

**Why:** A syntax error, a malformed resource key, or a missing variable shouldn't trigger an expensive hardware allocation to discover.

**How:** `pulumi.runtime.set_mocks()` intercepts every cloud API call, so `pytest` can validate the actual resource graph — including the exact GPU resource key CoreWeave expects — with zero cloud credentials and zero cost:

```python
# infra_ml_compute/tests/test_ml_compute.py (excerpt)
def test_gpu_resource_limits_are_correct(self):
    def check_limits(args):
        limits = args[0]["containers"][0]["resources"]["limits"]
        self.assertIn("nvidia.com/gpu", limits)
        self.assertEqual(limits["nvidia.com/gpu"], "8")
    return pulumi.Output.all(runner.training_pod.spec).apply(check_limits)
```

This test exists because an earlier draft of this stack used a malformed key (`"://nvidia.com"`) that would have failed at deploy time — and had a test that asserted the broken value instead of catching it. Full test suite: [`infra_ml_compute/tests/test_ml_compute.py`](./infra_ml_compute/tests/test_ml_compute.py), [`infra_data_platform/tests/test_data_platform.py`](./infra_data_platform/tests/test_data_platform.py).

### Shift-Left Policy Enforcement (Example)

**Why:** Unit tests validate a stack against *its own* expectations — they only catch what someone thought to assert. Policy-as-code validates a stack against organization-wide rules, evaluated automatically during `preview`/`up`, regardless of whether the person writing that particular stack knew the rule existed.

**How:** [`policy-pack-example/`](./policy-pack-example/) is a minimal `PolicyPack` wired into `train.yml` via `pulumi up --policy-pack ../policy-pack-example`. **This pack is illustrative, not prescriptive** — its two policies (S3 public-access blocking, GPU pod resource limits) deliberately check things this repo's stacks already do correctly, to demonstrate the mechanism working end-to-end. It is not a statement of what policies a real deployment of this architecture needs; a production policy pack would come from a pre-built compliance framework or your platform team, not a how-to repo. See the docstrings in [`policy-pack-example/__main__.py`](./policy-pack-example/__main__.py) for the reasoning behind each example policy.

---

## 8. GitOps Lifecycle Orchestration

A traditional `terraform apply` needs a long-running, active pipeline runner for the full duration of a multi-day training job — if that runner's connection drops mid-run, the teardown step never fires and the cluster bills indefinitely.

This pipeline gates deployment behind the unit-test job, then streams the training pod's actual logs to capture its real exit code rather than assuming success:

```text
THE PROGRAMMATIC GITOPS LIFECYCLE
===================================================================================
[ TRIGGER ] ────────► Manual GUI / Push to Main / Cron / Repository Webhook
                           │
                           ▼
[ PHASE 1 ] ────────► Shift-Left Validation (pytest + set_mocks)      [ Pass: $0 Spent ]
                           ▼
[ PHASE 2 ] ────────► Provision Ephemeral Stack (policy-checked `pulumi up`,
                       CoreWeave H100 Pod JIT Spin-up + TTL kill-clock attached)
                           ▼
[ PHASE 3 ] ────────► Stream Logs, Capture Real Exit Code
                           ├───► Exit 0 (Success) ──────► 'pulumi destroy' (Stop Billing)
                           └───► Exit != 0 (Crash) ─────► Alert (Slack/PagerDuty)
                                                              │
                                                              ▼
                                                    Auto-Rescue NVMe Data (non-destructive)
                                                              │
                                                              ▼
                                              Hold Node ──► Human runs cleanup.yml when ready
===================================================================================
```

Four trigger modes are wired into the pipeline, each suited to a different way training work gets kicked off:

| Trigger | Use case |
|---|---|
| `workflow_dispatch` | Operator or data scientist starts a run manually from the GitHub UI |
| `push` to `main` | Fires automatically when infra or training config changes merge |
| `schedule` / `cron` | Recurring nightly or weekend batch runs |
| `repository_dispatch` | An upstream orchestrator (Databricks Airflow/Dagster) triggers compute as soon as data prep finishes |

Full pipeline: [`ci_staged_gh/workflows/train.yml`](./ci_staged_gh/workflows/train.yml) — includes the automatic, non-destructive data rescue on failure. Manual force-teardown override: [`ci_staged_gh/workflows/cleanup.yml`](./ci_staged_gh/workflows/cleanup.yml) — destroy only, run by a human once ready. (This folder is renamed from `.github/` to `ci_staged_gh` to avoid invoking a GitHub Action from updates to the repo — see Section 10.)

---

## 9. Storage Strategy: How Mosaic Shards Are Handled

A common question when building cross-cloud AI pipelines is whether to provision a persistent cloud storage volume (e.g., CoreWeave Block Storage) to land training datasets. This blueprint deliberately avoids persistent secondary storage on CoreWeave for the training data path — but does provision CoreWeave's accelerated object storage (`ObjectStorageBucket`) as a hot local cache layer for the currently-running job.

PyTorch dataloaders (via `mosaicml-streaming`) can't stream directly into GPU VRAM from a WAN socket; they need a local filesystem path to stage, index, and decompress dataset shards. This blueprint mounts an ephemeral `emptyDir` local NVMe volume directly on the host chassis (`/mnt/local`), sized for the training footprint, backed by the accelerated cache bucket for throughput:

* **Zero storage idle cost** — no 24/7 block storage fees; base-state compute spend stays at $0/hour.
* **High throughput** — reads directly from local NVMe scratch space, matching GPU ingestion rates.
* **Automatic purge on teardown** — the moment `pulumi destroy` runs, the kernel wipes the local cache; zero data footprint remains off-cloud.

---

## 10. Repository Structure

```text
.
├── ci_staged_gh/                  # Rename to .github/ to activate - staged here to avoid kicking off a live GH Action.
│   └── workflows/
│       ├── train.yml              # Main GitOps pipeline: test-gate → policy-checked deploy → monitor → auto-rescue on failure → destroy/alert
│       └── cleanup.yml            # Manual, human-triggered force-teardown of a node held by the failure trap
├── ESC-env-def.yaml               # Pulumi ESC central environment definition
├── policy-pack-example/           # ILLUSTRATIVE example policy pack (not prescriptive - see Section 7)
│   ├── PulumiPolicy.yaml
│   ├── __main__.py
│   └── requirements.txt
├── infra_data_platform/           # PERMANENT DATA PLANE STACK
│   ├── Pulumi.yaml
│   ├── Pulumi.production.yaml
│   ├── __init__.py
│   ├── __main__.py                # AWS VPC, S3 buckets, Databricks workspace, Megaport bridge + MACsec
│   ├── requirements.txt
│   └── tests/
│       ├── __init__.py
│       └── test_data_platform.py
└── infra_ml_compute/                # EPHEMERAL GPU COMPUTE STACK
    ├── Pulumi.yaml
    ├── Pulumi.production.yaml
    ├── __init__.py
    ├── __main__.py                 # CoreWeave K8s provider, NVMe cache bucket, pod with failure trap, TTL kill-clock
    ├── requirements.txt
    └── tests/
        ├── __init__.py
        └── test_ml_compute.py
```

**Note** stack directories use underscores to ensure Python functionality.

---

## 11. Deployment Verification

To run a test deployment:

1. Deploy the permanent data plane:
   ```bash
   cd infra_data_platform
   pulumi stack select production
   pulumi up --yes
   ```

2. Run local unit tests for both stacks:
   ```bash
   PYTHONPATH=. pytest infra_data_platform/tests/
   PYTHONPATH=. pytest infra_ml_compute/tests/
   ```

3. Rename `ci_staged_gh/` to `.github/` then trigger `train.yml` via the Actions tab, a push commit, the scheduled timer, or an upstream webhook dispatch.


