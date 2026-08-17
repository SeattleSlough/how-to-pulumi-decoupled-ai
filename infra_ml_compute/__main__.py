# File: infra_ml_compute/__main__.py
import datetime
import pulumi
import pulumi_kubernetes as k8s
import pulumi_pulumiservice as pulumiservice

# 1. READ PERMANENT STATE VIA STACK REFERENCE
# Built dynamically from the current stack name (not hardcoded to
# "production") and pointed at "data-platform" - the actual Pulumi project
# name declared in infra_data_platform/Pulumi.yaml, not the directory name.
config = pulumi.Config()
env = pulumi.get_stack()
data_platform = pulumi.StackReference(f"your-org/data-platform/{env}")

inbound_stream_endpoint = data_platform.get_output("lakehouse_url").apply(
    lambda url: f"{url}?mode=streaming&framework=mosaic_streaming"
)
outbound_sync_target = data_platform.get_output("checkpoint_bucket_id").apply(
    lambda id: f"s3://{id}/models/training-model/checkpoints/"
)

# 2. INITIALIZE KUBERNETES WITH DYNAMIC ESC CREDENTIALS
cw_kubeconfig = config.require_secret("coreweaveKubeconfig")
coreweave_provider = k8s.Provider("coreweave-k8s-engine", kubeconfig=cw_kubeconfig)

gpu_namespace = k8s.core.v1.Namespace(
    "coreweave-ai-podspace",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="ephemeral-training-runners"),
    opts=pulumi.ResourceOptions(provider=coreweave_provider),
)

# 3. COREWEAVE ACCELERATED LOCAL CACHE (LOTA NVMe object storage)
# api_version reflects CoreWeave's actual CRD group/version convention.
coreweave_storage_bucket = k8s.apiextensions.CustomResource(
    "lota-nvme-bucket",
    api_version="objectstorage.coreweave.com/v1alpha1",
    kind="ObjectStorageBucket",
    metadata=k8s.meta.v1.ObjectMetaArgs(
        name="training-hot-data-cache",
        namespace=gpu_namespace.metadata["name"],
    ),
    spec={"size": "50Ti", "region": "us-east-1"},
    opts=pulumi.ResourceOptions(provider=coreweave_provider),
)

# 4. EPHEMERAL TRAINING POD WITH FAILURE TRAP
# Uses Pulumi's typed Kubernetes SDK classes (PodSpecArgs, ContainerArgs,
# etc.) for compile-time type checking to avoid
# "typo caught at deploy time, not build time" errors.
training_pod = k8s.core.v1.Pod(
    "h100-8x-training-node",
    metadata=k8s.meta.v1.ObjectMetaArgs(
        name="training-model",
        namespace=gpu_namespace.metadata["name"],
        labels={"app": "llm-pretraining", "tier": "compute"},
    ),
    spec=k8s.core.v1.PodSpecArgs(
        restart_policy="Never",
        containers=[
            k8s.core.v1.ContainerArgs(
                name="mosaic-training-runner",
                image="ghcr.io/your-org/mosaic-flash-attention:latest",
                resources=k8s.core.v1.ResourceRequirementsArgs(
                    limits={"nvidia.com/gpu": "8", "cpu": "128", "memory": "1000Gi"},
                    requests={"nvidia.com/gpu": "8"},
                ),
                env=[
                    k8s.core.v1.EnvVarArgs(
                        name="DATABRICKS_INBOUND_STREAM_URL", value=inbound_stream_endpoint
                    ),
                    k8s.core.v1.EnvVarArgs(
                        name="AWS_OUTBOUND_CHECKPOINT_BUCKET", value=outbound_sync_target
                    ),
                ],
                # On failure, hold the node and its local NVMe
                # cache open for inspection instead of tearing it down.
                command=["/bin/bash", "-c"],
                args=[
                    """
                    python3 -m llm_train.launch --config training-config.yaml
                    STATUS=$?
                    if [ $STATUS -eq 0 ]; then
                        echo 'SUCCESS: Checkpoints safely pushed to AWS S3.' > /dev/termination-log
                        exit 0
                    else
                        echo 'CRITICAL FAILURE: Locking compute & NVMe states.' > /dev/termination-log
                        while true; do sleep 3600; done
                    fi
                    """
                ],
                volume_mounts=[
                    k8s.core.v1.VolumeMountArgs(name="local-nvme-scratch", mount_path="/mnt/local")
                ],
            )
        ],
        volumes=[
            k8s.core.v1.VolumeArgs(
                name="local-nvme-scratch",
                empty_dir=k8s.core.v1.EmptyDirVolumeSourceArgs(medium="", size_limit="2Ti"),
            )
        ],
    ),
    opts=pulumi.ResourceOptions(provider=coreweave_provider, depends_on=[coreweave_storage_bucket]),
)

# 5. TTL KILL-CLOCK (bounds the cost of a silent hang)
# The failure trap above only fires when the training script itself exits
# non-zero - a genuine crash. It does nothing for the harder case: a
# process that never exits at all (stuck in a bad state with no error to
# report). Nothing upstream of this - not the failure trap, not train.yml's
# `kubectl logs -f` step - has any way to distinguish that from a slow but
# healthy run, so nothing reclaims it. This is a hard, fixed-duration
# kill-clock: the stack is scheduled for automatic destroy N hours from
# whenever this deploy runs, no matter what state the pod is in then.
#
# What this deliberately does NOT do: extend the clock for a run that's
# still legitimately healthy and working past that window. Doing that
# safely requires a health signal from inside the training process itself
# (a heartbeat file, a liveness probe tied to real training progress, a
# custom metric) that this blueprint doesn't implement - the same category
# of assumption as the checkpoint `.done` marker in Section 6: it depends
# on data-science-side instrumentation this repo doesn't own. If you add
# that heartbeat, the extension pattern is a second scheduled step
# (Kubernetes CronJob, or a script call) that reads the health signal and,
# if healthy, calls the Pulumi Cloud Schedules API's TTL update endpoint
# (POST .../deployments/ttl/schedules/{scheduleID}) to push destroy_at
# forward using this schedule's id output below.
ttl_hours = config.get_int("ttlHours") or 8  # override via `pulumi config set ttlHours <n>`
destroy_at = (
    datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=ttl_hours)
).strftime("%Y-%m-%dT%H:%M:%S.000Z")

# TtlSchedule executes on Pulumi's own hosted runners (Pulumi Deployments),
# not this repo's GitHub Actions - a separate execution path from train.yml
# and cleanup.yml, needed so the kill-clock still fires even if the GitHub
# Actions run that created it has long since finished. Deployment settings
# (which repo/branch/work-dir Pulumi Deployments should check out to run
# the eventual `pulumi destroy`) have to exist before a TTL schedule can be
# attached - without this, TtlSchedule creation fails outright.
deployment_settings = pulumiservice.DeploymentSettings(
    "training-node-deployment-settings",
    organization=pulumi.get_organization(),
    project=pulumi.get_project(),
    stack=pulumi.get_stack(),
    source_context={
        "git": {
            "repo_url": "https://github.com/your-org/how-to-pulumi-decoupled-ai.git",
            "branch": "refs/heads/main",
        },
    },
    operation_context={
        "pre_run_commands": [],
        "environment_variables": {},
    },
)

ttl_schedule = pulumiservice.TtlSchedule(
    "training-node-ttl",
    organization=pulumi.get_organization(),
    project=pulumi.get_project(),
    stack=pulumi.get_stack(),
    timestamp=destroy_at,
    # Do not also delete the stack record itself - just the resources. The
    # stack (and its Drift/TTL history) should persist for the next run.
    delete_after_destroy=False,
    opts=pulumi.ResourceOptions(depends_on=[training_pod, deployment_settings]),
)

pulumi.export("active_compute_pod", training_pod.metadata.apply(lambda m: m.get("name") if m else None))
pulumi.export("ttl_destroy_at", destroy_at)
