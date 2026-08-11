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
* **Runaway Cost Prevention:** Programmatic failure traps and automated lifecycle teardowns to prevent $0/hour compute states from idling when jobs fail or complete.
* **A Real Cross-Cloud Network Path:** A Megaport private circuit between AWS and CoreWeave, provisioned as part of the permanent data plane — not assumed to exist.

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

Full implementation: [`infra_data_platform/__main__.py`](./infra_data_platform/__main__.py).

---

## 5. Ephemeral Compute Plane

**Why:** GPU nodes billing at $1,500+/hr should sit at $0/hr until an explicit execution request is made, and should read the permanent stack's outputs rather than duplicating that state.

**How:** `infra_ml_compute` reads the data stack via `pulumi.StackReference`, built dynamically so it always points at the environment currently in use:

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

**Why:** A naive teardown on any non-zero exit code destroys the exact NVMe logs and node state an engineer needs to debug a crash. A naive *lack* of teardown on success leaves an idle $1,500+/hr node billing indefinitely.

**How:** The container entrypoint checks its own exit status before deciding whether to release the node:

```bash
python3 -m llm_train.launch --config 70b_config.yaml
STATUS=$?
if [ $STATUS -eq 0 ]; then
    echo 'SUCCESS: Checkpoints safely pushed to AWS S3.' > /dev/termination-log
    exit 0
else
    echo 'CRITICAL FAILURE: Locking compute & NVMe states.' > /dev/termination-log
    while true; do sleep 3600; done   # holds the node open for inspection
fi
```

On success, the pipeline's next step runs `pulumi destroy` immediately. On failure, it alerts and leaves the node in this hold state until someone runs the manual cleanup workflow. Full context: [`infra_ml_compute/__main__.py`](./infra_ml_compute/__main__.py).

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
[ PHASE 2 ] ────────► Provision Ephemeral Stack (CoreWeave H100 Pod JIT Spin-up)
                           ▼
[ PHASE 3 ] ────────► Stream Logs, Capture Real Exit Code
                           ├───► Exit 0 (Success) ──────► 'pulumi destroy' (Stop Billing)
                           └───► Exit != 0 (Crash) ─────► Alert (Slack/PagerDuty) & Hold Node
===================================================================================
```

Four trigger modes are wired into the pipeline, each suited to a different way training work gets kicked off:

| Trigger | Use case |
|---|---|
| `workflow_dispatch` | Operator or data scientist starts a run manually from the GitHub UI |
| `push` to `main` | Fires automatically when infra or training config changes merge |
| `schedule` / `cron` | Recurring nightly or weekend batch runs |
| `repository_dispatch` | An upstream orchestrator (Databricks Airflow/Dagster) triggers compute as soon as data prep finishes |

Full pipeline: [`.github/workflows/train.yml`](./.github/workflows/train.yml). Manual force-teardown override: [`.github/workflows/cleanup.yml`](./.github/workflows/cleanup.yml).

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
├── ci_staged_gh/                  # Rename to .github/ - changed here to avoid kicking of gh action.
│   └── workflows/
│       ├── train.yml              # Main GitOps pipeline: test-gate → deploy → monitor → destroy/alert
│       └── cleanup.yml            # Manual force teardown as only way to release node frozen post failure trap
├── ESC-env-def.yaml               # Pulumi ESC central environment definition
├── infra_data_platform/           # PERMANENT DATA PLANE STACK
│   ├── Pulumi.yaml
│   ├── Pulumi.production.yaml
│   ├── __init__.py
│   ├── __main__.py                # AWS VPC, S3 buckets, Databricks workspace, Megaport bridge
│   ├── requirements.txt
│   └── tests/
│       ├── __init__.py
│       └── test_data_platform.py
└── infra_ml_compute/                # EPHEMERAL GPU COMPUTE STACK
    ├── Pulumi.yaml
    ├── Pulumi.production.yaml
    ├── __init__.py
    ├── __main__.py                 # CoreWeave K8s provider, NVMe cache bucket, pod with failure trap
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

3. Trigger `.github/workflows/train.yml` via the Actions tab, a push commit, the scheduled timer, or an upstream webhook dispatch.

