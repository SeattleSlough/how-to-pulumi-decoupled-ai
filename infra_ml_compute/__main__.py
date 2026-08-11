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
    lambda id: f"s3://{id}/models/llama-3-custom-70b/checkpoints/"
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
# api_version reflects CoreWeave's actual CRD group/version convention -
# the placeholder "://coreweave.com" in the earlier draft was a stripped
# scheme prefix and would not resolve as a valid Kubernetes API group.
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
# etc.) rather than raw dicts - this is what actually gets you the
# compile-time type checking the paper argues for. A raw-dict spec (as in
# the earlier draft) loses that benefit entirely and reintroduces the
# "typo caught at deploy time, not build time" problem Pulumi is meant to
# solve.
training_pod = k8s.core.v1.Pod(
    "h100-8x-training-node",
    metadata=k8s.meta.v1.ObjectMetaArgs(
        name="llama3-70b-trainer",
        namespace=gpu_namespace.metadata["name"],
        labels={"app": "llm-pretraining", "tier": "compute"},
    ),
    spec=k8s.core.v1.PodSpecArgs(
        restart_policy="Never",
        containers=[
            k8s.core.v1.ContainerArgs(
                name="mosaic-training-runner",
                image="ghcr.io/your-org/mosaic-flash-attention:latest",
                # Correct CoreWeave/NVIDIA device-plugin resource key.
                # The earlier draft used "://nvidia.com", a malformed key
                # that would fail scheduling - and its own unit test
                # asserted that broken key was present, which is a good
                # example of why shift-left tests must validate against a
                # spec, not just mirror whatever the implementation wrote.
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
                # THE SHIELD: on failure, hold the node and its local NVMe
                # cache open for inspection instead of tearing it down.
                command=["/bin/bash", "-c"],
                args=[
                    """
                    python3 -m llm_train.launch --config 70b_config.yaml
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
