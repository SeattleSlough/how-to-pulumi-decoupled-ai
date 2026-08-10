# The Decoupled AI Factory: Bridging Enterprise Data Lakes and Ephemeral GPU Compute with Pulumi

## The Evolution of Enterprise AI Architecture

To optimize for cost, security, and flexibility, infrastructure and data teams supporting production-grade AI systems implement an architecture that co-locates data and compute in the same cloud provider environment. The industry move towards the interoperable lakehouse with its open-sourced solutions such as Iceberg, Polaris, and Unity have, in theory, enhanced the value of this architecture.

The top Data Intelligence platforms in the market today are Databricks and Snowflake, with the former being the primary choice of data scientists, and by extension, is the leading platform for AI-related workloads. These platforms offer a robust and powerful set of capabilities and make money by charging for compute time based on the combination of the underlying cost of using the cloud provider’s compute and a fee from the Data Intelligence provider. As such, compute time comes at a premium. For heavy AI compute, that pricing model can create tension between the needs of data scientists and the desire of the business to stay within set budgets.

To address this tension, for massive training or inference runs, companies are increasingly employing a decoupled AI stack, moving away from a monolithic architecture to one that is modular. In short, among other things, the decoupled AI stack arbitrages compute by moving it outside the hyperscaler’s cloud. Companies maintain all the benefits of their Data Intelligence platform and gain access to cheap, bare-metal GPUs provided by specialized AI clouds like CoreWeave and Lambda Labs.

It’s an elegant solution, but it introduces a major obstacle for platform engineers: bridging completely independent cloud ecosystems without relying on brittle, manually maintained glue code.

This guide uses an imagined company stack of AWS, Databricks, and CoreWeave leveraging GitHub for its GitOps pipelines to demonstrate how Pulumi acts as a unified infrastructure control plane to solve this challenge. By turning cross-cloud networking, data governance links, and zero-trust credentials into programmable software objects, Pulumi provides the deterministic foundation that allows modern GitOps pipelines to automate this multi-cloud handshake safely, securely, and efficiently.

***View the complete implementation repository at: [//github.com](https://github.com)***

---

## 1. The Architectural Friction: Storage Perimeters vs. Ephemeral Hardware

The decoupled AI stack mixes enterprise data lakes in one cloud with specialized raw compute in another, an architecture that requires managing two distinct data streams simultaneously.

From the perspective of the AWS environment, the architecture looks like this:

```text
               ┌────────────────────────────────────────┐
               │        AWS / DATABRICKS PERIMETER      │
               │                                        │
               │   ┌──────────────┐      ┌──────────┐   │
               │   │ Training Data│      │S3 Model  │   │
               │   │ (Delta Lake) │      │Repository│   │
               │   └──────┬───────┘      └────▲─────┘   │
               └──────────┼───────────────────┼─────────┘
                          │                   │
            1. OUTBOUND   │                   │   2. INBOUND
           Micro-Batches  │                   │  Checkpoint Sync
           (Streaming API)│                   │  (Boto3 / AWS CLI)
                          ▼                   │
               ┌──────────────────────────────┴─────────┐
               │         COREWEAVE GPU CLUSTER          │
               │                                        │
               │      [ NVIDIA H100 Training Pod ]      │
               └────────────────────────────────────────┘
```

### The Data Handoff (Outbound Stream)
GPUs cannot natively digest a giant SQL database or relational table format. Because of this, data engineering pods use Databricks Spark clusters inside AWS to flatten datasets into an open-source, binary shard format called MDS (Mosaic Data Shards).

The core requirement is that these files cannot be permanently copied over to CoreWeave. Instead, the training container must treat that S3 storage directory like a streaming API, pulling tiny, fast micro-batches over an encrypted network directly into RAM, feeding the GPU, and immediately wiping the cache locally.

### The Intellectual Property Loop (Inbound Sync)
As the model runs, it periodically dumps mathematical snapshots of its learning progress, known as checkpoints. Because the specialized compute nodes are temporary, a background daemon (like the AWS CLI or boto3) must continuously push these model weights back across the cross-cloud tunnel into a secure, permanent AWS S3 repository before the servers are shut down.

---

## 2. The Blast Radius: The Cost of Getting It Wrong

This architecture is designed to optimize AI compute spend but it isn’t without potentially significant risk. In a decoupled AI stack, a multi-cloud infrastructure failure can result in immediate financial and architectural damage.

### Financial Damage
Operational costs can stack up during an infrastructure hang, the magnitude of which depends on when the failure strikes:

| Cost Component | Weekend Hang (48 Hours) | Weekday Crash (2 Hours) |
| :--- | :--- | :--- |
| **Wasted GPU Compute** | \$75,632.64 | \$3,151.36 |
| **Engineering Interruption Labor** | \$0.00 | \$2,000.00 |
| **AWS Data Egress Re-Run Tax** | \$3,500.00 | \$3,500.00 |
| **Total Incident Loss** | **\$79,132.64** | **\$8,651.36** |

*(Economic data calculated utilizing CoreWeave’s standard rate of [\$49.24 per hour for an 8-GPU NVIDIA HGX H100 node](https://coreweave.com), mapping a 32-node training cluster workload at \$1,575.68/hr, alongside a standard \$70/TB AWS S3 external cross-cloud data egress penalty to reload a 50TB dataset epoch).*

### Architectural Damage
* **State Engine Desynchronization:** Broken pipeline connections orphan active cross-cloud network tunnels and bare-metal nodes, leaving behind "ghost infrastructure" that corrupts state registries and causes immediate resource collisions.
* **Cache Extermination and Latency Penalties:** Naive automated tear-downs erase high-speed local chassis NVMe data buffers, forcing subsequent runs into a "cold-start" loop that wastes hours re-streaming terabytes of data.
* **Checkpoint Poisoning and Forensic Friction:** Undetected infrastructure faults cause corrupted training snapshots to upload into permanent storage buckets, degrading data lineage and requiring days of manual verification.

It’s therefore critical that a company uses an IaC solution that can avoid these pitfalls.

---

## 3. The Challenge with Legacy IaC (Terraform or OpenTofu)

If a platform engineering team tries to build this type of decoupled architecture using static, domain-specific configuration languages like Terraform or OpenTofu, they run into massive operational roadblocks:

* **Brittle State File Chaining:** Terraform isolates state configurations rigidly. Because Databricks and CoreWeave live in completely different cloud ecosystems, engineers cannot cleanly pass live properties from one provider to another without fracturing their repository design. So, they are forced to maintain split codebases and write fragile shell wrappers to scrape JSON variables out of one state file and manually paste them into another.
* **Static Secret Spillage:** Passing an ephemeral data-access token over to a CoreWeave runner via Terraform requires rendering it into plaintext variables or writing it permanently inside a static Terraform state file sitting in a storage bucket. This creates an immediate (and likely intolerable) security exposure risk for an enterprise lakehouse.
* **Total Application Blindness:** Terraform can build a cluster, but it cannot listen to the processes running inside it. Creating a cost kill switch or a failure-based alert freeze requires building highly complex, external orchestration workarounds outside the infrastructure tool. If an external custom tooling network-times out mid-run, the destroy step is never called, leaving a high-cost GPU cluster idling at the company's expense.
* **The Imperative Pipeline Blockade (GitOps Inversion):** Traditional IaC tools require a synchronous, blocking push command (`terraform apply`) to build infrastructure. When integrated into a GitOps pipeline, the automation runner must sit active and idling for the entire duration of a multi-day AI training loop just to execute the subsequent cleanup task. If the runner hits a routine execution timeout and disconnects mid-run, the execution thread is severed. Because the tool cannot decouple resource creation from application lifetimes, the teardown hook never fires—orphaning the high-cost GPU cluster and leaving it billing at peak rates indefinitely.

---

## 4. The Pulumi Cross-Cloud Pipeline
Pulumi treats cloud providers as software objects within a single program, eliminating the need to bridge distinct infrastructure environments with manual glue code. Using Databricks as the secure foundation for data governance and preparation, Pulumi acts as the unified cross-cloud control plane to coordinate the downstream compute infrastructure across a structured, chronological lifecycle:

```text
THE PIPELINE EXECUTION SEQUENCE
====================================================================================================
[ LOCAL CODE REPO ] ──► PUSH TO MAIN BRANCH
                             │
                             ▼
[ PHASE 1: UNIT TEST ] ──► Run Local Mocks & Validate Logic (pytest) ──► [ FAIL ] ──► HALT WORKFLOW
                             │ (Passes: $0 Spent)
                             ▼
[ PHASE 2: SECURITY ]  ──► Open OIDC Boundary ──► Fetch AWS/CoreWeave Session Tokens (Pulumi ESC)
                             │
                             ▼
[ PHASE 3: PROVISION ] ──► Query Permanent Data Stack Outputs (Pulumi Stack References)
                             │
                             └──► Spin up Just-in-Time H100 GPU Clusters on CoreWeave
                                  Inject Accelerated Local Storage Caches (LOTA™ NVMe)
                                  Launch Model Training Loop
                                  │
                                  ▼
[ PHASE 4: EXECUTION ] ──► Track Runtime Status Codes via GitHub Actions Runner
                                  │
                                  ├───► [ IF EXIT 0: CLEAN SUCCESS ]
                                  │     Execute 'pulumi destroy' ──► Terminate Hardware & Stop Billing
                                  │
                                  └───► [ IF EXIT != 0: CRITICAL FAILURE ]
                                        Trigger Bash Error Trap ──► Fire Slack / PagerDuty Alerts
                                        Lock Pod in Infinite Sleep Hold ──► Freeze Local NVMe Caches
====================================================================================================
```

### Phase 1: Shift-Left Infrastructure: Zero-Cost Architectural Validation
Before any resource is invoked, Pulumi allows platform teams to execute programmatic zero-cost unit tests using standard testing frameworks like `pytest`. By leveraging `pulumi.runtime.set_mocks()`, the pipeline intercepts API calls to AWS, Databricks, and CoreWeave, simulating the entire multi-cloud configuration in memory.

These tests run in milliseconds on local developer machines or PR-validation runners without requiring cloud credentials, requiring zero secret tokens, and costing zero dollars. This phase mathematically verifies that the error trap is properly armed, VPC CIDR segments don't overlap, and exact hardware strings are free of syntax typos.

```python
# infra-ml-compute/tests/test_ml_compute.py (Highlight)
import unittest
import pulumi

# Assert that the custom failure-handling script contains our debug hold loop
@pulumi.runtime.test
def test_error_trap_contains_infinite_sleep(self):
    def check_bash_shield(pod_spec):
        bash_args = pod_spec["containers"]["args"]
        self.assertIn("while true; do sleep 3600; done", bash_args)
        self.assertIn("Locking compute & NVMe states.", bash_args)
    return pulumi.Output.all(runner.training_pod.spec).apply(check_bash_shield)
```

### Phase 2: Zero-Trust Identity Federation via Pulumi ESC
Instead of generating static, long-lived access tokens that sit exposed in code, the platform uses Pulumi ESC (Environments, Secrets, and Configuration). Pulumi ESC opens a secure OpenID Connect (OIDC) trust boundary between GitHub Actions, AWS, and CoreWeave.

At runtime, ESC dynamically fetches short-lived, read-only temporary session credentials from AWS Secrets Manager and handles the token exchange with CoreWeave's API surface. This restricts down the generated MDS/Parquet streaming directories to a single, time-bound training execution window.

### Phase 3: Just-in-Time Ephemeral Compute Plane Provisioning
To eliminate idle infrastructure costs, the CoreWeave environment sits at $0/hour until a pipeline run is requested. Pulumi separates this architecture cleanly into two decoupled codebases using Pulumi Stack References:
* **The Permanent Data Plane Stack:** Your core AWS network, Databricks workspace, Unity Catalog, and Megaport cloud exchange connection are managed as a permanent asset that is never destroyed.
* **The Ephemeral Compute Plane Stack:** Using a Stack Reference, this codebase "peeks" into the outputs of the live data plane to pull the target S3 asset routes. Pulumi then provisions the raw NVIDIA H100 bare-metal GPU clusters on CoreWeave, mounts the node's local NVMe chassis array, and securely injects the streaming and checkpoint credentials straight into the container as encrypted `pulumi.Output` secrets.

```python
# infra-ml-compute/__main__.py (Highlight)
import pulumi
import pulumi_kubernetes as k8s

# Read permanent state outputs via a Multi-Project Stack Reference
data_platform = pulumi.StackReference("your-org/data-platform/production")

inbound_stream = data_platform.get_output("lakehouse_url").apply(
    lambda url: f"{url}?mode=streaming&framework=mosaic_streaming"
)

# Initialize the CoreWeave Kubernetes Provider natively
coreweave_provider = k8s.Provider("cw-engine", kubeconfig=config.require("kubeconfig"))
```

### Phase 4: GitOps Orchestration & Automated Error Trapping
While Pulumi handles declarative infrastructure creation rather than long-running application workflow loops, it maps seamlessly into a programmatic GitOps deployment structure. By combining a bare Kubernetes Pod specification with an environment wrapper, Pulumi deploys an automated runtime containment layer.
The pipeline tracks the container's execution state via GitHub Actions to automate the infrastructure lifecycle:
* **Clean Success:** If the training script completes its training matrix cleanly and syncs all final weights to S3, it exits with `status 0`. The runner captures this code and instantly executes `pulumi destroy` on the compute stack, turning off the high-cost GPU billing clock.
* **Resilient Failure Lock:** If the training script hits an out-of-memory (OOM) exception or a hardware fault, a naive, immediate automated teardown would delete the exact local node logs needed to diagnose the crash. The wrapper container intercepts the failure, enters an infinite sleep hold loop to keep the physical hardware node active, and triggers real-time payloads out to a notification endpoint (in this case, Slack/PagerDuty). This locks the internal `/mnt/local` NVMe flash memory arrays in place, avoiding the architectural damage from failure and enabling engineers to jump into a live debugging terminal before using a manual override pipeline to clean the space.

```python
# infra-ml-compute/__main__.py (Container Configuration)
"containers": [{
    "name": "mosaic-training-runner",
    "image": "ghcr.io/your-org/mosaic-flash-attention:latest",
    "command": ["/bin/bash", "-c"],
    "args": [
        """
        python3 -m llm_train.launch --config 70b_config.yaml;
        if [ $? -eq 0 ]; then
            echo 'SUCCESS: Weights pushed to S3.' > /dev/termination-log; exit 0
        else
            echo 'CRITICAL FAILURE: Locking NVMe states.' > /dev/termination-log
            while true; do sleep 3600; done # Infinite loop blocks teardown
        fi
        """
    ],
    "volumeMounts": [{"mountPath": "/mnt/local", "name": "local-nvme-scratch"}]
}]
```

---

## 5. The Imperative for an Infrastructure-as-Software Defined AI Stack
A decoupled AI stack solves a major tension within enterprises: it allows data teams to run massive foundational training and inference workloads without forcing the business to absorb premium hyperscaler GPU markups (or curtail work to avoid them). However, if an enterprise allows its multi-cloud architecture to be dictated by the rigid, file-based limitations of legacy Infrastructure as Code, they simply trade away cloud provider premiums for an unmanageable tax of manual glue code, security exposures, and orphaned compute costs.

Legacy, push-based DSL tools cannot bridge this cross-cloud chasm safely. They are inherently blind to application lifecycles and incapable of handling the asynchronous relationship between a permanent data lakehouse and a short-lived GPU cluster.

By unifying the multi-cloud infrastructure plane into a strongly typed software layer, Pulumi provides the programmatic tools required to orchestrate this modern architecture at scale. Leveraging **Pulumi ESC** for dynamic, zero-trust token federation and **Pulumi Stack References** to decouple independent cloud lifecycles transforms infrastructure from a static constraint into an active competitive advantage.

Ultimately, companies that harness a software-defined infrastructure control plane can convert raw infrastructure savings directly into deeper model iterations, faster time-to-market, and compounding business ROI. As model complexity accelerates, the ability to automate a secure, cost-controlled AI factory will separate the enterprises that merely experiment with AI from those that dominate the market.

===

## Next Steps for Review
The implementation files supporting this whitepaper framework are structured cleanly into the following repository code blueprint modules:

```text
.
├── documentation/                 # rename to .github/
│   └── templates/                 # rename to workflows/
│       ├── cleanup.yml            # Manual "Run Workflow" force-teardown override
│       └── train.yml              # Main CI/CD pipeline (Tests -> Deploy -> Monitor -> Trap/Destroy)
│
├── infra-data-platform/           # --- PERMANENT COLD PLANE ---
│   ├── Pulumi.yaml                # Core project naming and Python definition
│   ├── Pulumi.production.yaml     # Environment configuration pointing to central ESC
│   ├── __main__.py                # Main logic: AWS VPC, S3 Lakehouse, Databricks, Megaport Bridge
│   ├── requirements.txt           # Dependencies: pulumi-aws, pulumi-databricks, pulumi-megaport
│   └── tests/
│       ├── __init__.py            # (Empty file) Package initialization marker
│       └── test_data_platform.py  # Unit tests verifying VPC ranges and Unity Catalog paths
│
|── infra-ml-compute/              # --- EPHEMERAL GPU PLANE ---
|   ├── Pulumi.yaml                # Core project naming and Python definition
|   ├── Pulumi.production.yaml     # Environment configuration pointing to central ESC and data stack
|   ├── __main__.py                # Main logic: CoreWeave K8s Provider, LOTA Cache, Pod with NVMe Trap
|   ├── requirements.txt           # Dependencies: pulumi-kubernetes
|   └── tests/
|       ├── __init__.py            # (Empty file) Package initialization marker
|       └── test_ml_compute.py     # Unit tests verifying H100 counts, volume mounts, and bash traps
|
|── ESC-env-def.yaml               # structured centralized config file for namespace managed by Pulumi ESC
```
