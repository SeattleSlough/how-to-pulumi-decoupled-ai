# File: infra-ml-compute/__main__.py
import pulumi
import pulumi_kubernetes as k8s

# 1. READ CONFIGURATIONS INJECTED BY PULUMI ESC
config = pulumi.Config()
# Fetch the raw kubeconfig dynamically fetched from AWS Secrets Manager via ESC
esc_cw_secrets = config.require_object("coreweave")
cw_kubeconfig = esc_cw_secrets.get("kubeconfig")

# 2. READ PERMANENT STATE VIA STACK REFERENCE
data_platform_ref = pulumi.StackReference("your-org/data-platform/production")

inbound_stream_endpoint = data_platform_ref.get_output("lakehouse_url").apply(
    lambda url: f"{url}?mode=streaming&framework=mosaic_streaming"
)
outbound_sync_target = data_platform_ref.get_output("checkpoint_bucket_id").apply(
    lambda id: f"s3://{id}/models/llama-3-custom-70b/checkpoints/"
)

# 3. INITIALIZE KUBERNETES WITH DYNAMIC ESC CREDENTIALS
coreweave_provider = k8s.Provider("coreweave-k8s-engine", kubeconfig=cw_kubeconfig)

gpu_namespace = k8s.core.v1.Namespace("coreweave-ai-podspace",
    metadata={"name": "ephemeral-training-runners"},
    opts=pulumi.ResourceOptions(provider=coreweave_provider)
)

# Local Accelerated Cache Bucket on CoreWeave
coreweave_storage_bucket = k8s.apiextensions.CustomResource("lota-nvme-bucket",
    api_version="://coreweave.com",
    kind="ObjectStorageBucket",
    metadata={"name": "training-hot-data-cache", "namespace": gpu_namespace.metadata["name"]},
    spec={"size": "50Ti", "region": "us-east-1"},
    opts=pulumi.ResourceOptions(provider=coreweave_provider)
)

# Ephemeral Pod Containing the NVMe Error Trap
training_pod = k8s.core.v1.Pod("h100-8x-training-node",
    metadata={
        "namespace": gpu_namespace.metadata["name"],
        "name": "llama3-70b-trainer",
        "labels": {"app": "llm-pretraining", "tier": "compute"}
    },
    spec={
        "restartPolicy": "Never",
        "containers": [{
            "name": "mosaic-training-runner",
            "image": "ghcr.io/your-org/mosaic-flash-attention:latest",
            "resources": {"limits": {"://nvidia.com": "8", "cpu": "128", "memory": "1000Gi"}},
            "env": [
                {"name": "DATABRICKS_INBOUND_STREAM_URL", "value": inbound_stream_endpoint},
                {"name": "AWS_OUTBOUND_CHECKPOINT_BUCKET", "value": outbound_sync_target}
            ],
            # THE SHIELD: Continuous loop preserves the node and NVMe storage if training crashes
            "command": ["/bin/bash", "-c"],
            "args": [
                """
                python3 -m llm_train.launch --config 70b_config.yaml;
                STATUS=$?
                if [ $STATUS -eq 0 ]; then
                    echo 'SUCCESS: Checkpoints safely pushed to AWS S3.' > /dev/termination-log
                    exit 0
                else
                    echo 'CRITICAL FAILURE: Training script crashed. Locking compute & NVMe states.' > /dev/termination-log
                    while true; do sleep 3600; done
                fi
                """
            ],
            "volumeMounts": [{"mountPath": "/mnt/local", "name": "local-nvme-scratch"}]
        }],
        "volumes": [{"name": "local-nvme-scratch", "emptyDir": {"medium": ""}}]
    },
    opts=pulumi.ResourceOptions(provider=coreweave_provider)
)

pulumi.export("active_compute_pod", training_pod.metadata.apply(lambda m: m.get("name") if m else None))
