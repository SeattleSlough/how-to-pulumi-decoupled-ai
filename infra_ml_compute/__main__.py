# File: infra_ml_compute/__main__.py
import pulumi
import pulumi_kubernetes as k8s

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

pulumi.export("active_compute_pod", training_pod.metadata.apply(lambda m: m.get("name") if m else None))