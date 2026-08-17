# File: infra_data_platform/__main__.py
import pulumi
import pulumi_aws as aws
import pulumi_databricks as databricks
import pulumi_megaport as megaport

# 1. READ CONFIGURATION VALUES INJECTED BY PULUMI ESC
config = pulumi.Config()
databricks_id = config.require("databricks-account-id")
megaport_token = config.require_secret("megaportToken")

# 2. PERMANENT NETWORK & STORAGE INFRASTRUCTURE
databricks_vpc = aws.ec2.Vpc(
    "databricks-vpc",
    cidr_block="10.0.0.0/16",
    enable_dns_hostnames=True,
)

lakehouse_bucket = aws.s3.BucketV2("enterprise-lakehouse", bucket="enterprise-ai-training-lakehouse")
checkpoint_bucket = aws.s3.BucketV2("model-checkpoints", bucket="enterprise-ai-model-checkpoints")

# Both buckets carry customer training data and model weights - block public
# access explicitly rather than relying on account-level defaults.
for _name, _bucket in (
    ("lakehouse-private", lakehouse_bucket),
    ("checkpoint-private", checkpoint_bucket),
):
    aws.s3.BucketPublicAccessBlock(
        _name,
        bucket=_bucket.id,
        block_public_acls=True,
        block_public_policy=True,
        ignore_public_acls=True,
        restrict_public_buckets=True,
    )

# Cost-control lifecycle policy: cleanup.yml rescues local NVMe data off a
# frozen node into checkpoint_bucket under rescue/unverified/<pod-name>/
# before teardown (see infra_ml_compute failure trap). That data has not
# been validated by the training pipeline's own checkpoint marker, so it
# should not accumulate indefinitely - auto-expire it after 30 days rather
# than requiring someone to remember to clean it up manually.
aws.s3.BucketLifecycleConfigurationV2(
    "checkpoint-bucket-lifecycle",
    bucket=checkpoint_bucket.id,
    rules=[
        aws.s3.BucketLifecycleConfigurationV2RuleArgs(
            id="expire-unverified-rescue-data",
            status="Enabled",
            filter=aws.s3.BucketLifecycleConfigurationV2RuleFilterArgs(
                prefix="rescue/unverified/",
            ),
            expiration=aws.s3.BucketLifecycleConfigurationV2RuleExpirationArgs(
                days=30,
            ),
        )
    ],
)

# 3. DATABRICKS WORKSPACE + UNITY CATALOG EXTERNAL LOCATION
databricks_workspace = databricks.MwsWorkspaces(
    "enterprise-workspace",
    account_id=databricks_id,
    aws_region="us-east-1",
    workspace_name="ai-compute-hub",
)

external_data_volume = databricks.ExternalLocation(
    "ai-training-volume",
    name="production-ml-training-data",
    url=lakehouse_bucket.id.apply(lambda id: f"s3://{id}/gold_layer/llm_tokens/"),
    credential_name="aws-storage-iam-role",
)

# 4. CROSS-CLOUD NETWORK BRIDGE (AWS <-> CoreWeave)
# This is the physical private circuit the architecture diagram's network
# arrow depends on. Without it, the compute stack has no defined path back
# to this stack's storage - it's provisioned here, once, as a permanent
# asset, and referenced (not recreated) by the ephemeral stack.
megaport_provider = megaport.Provider("mp-provider", api_token=megaport_token)

cross_cloud_bridge = megaport.Vxc(
    "aws-coreweave-private-pipe",
    rate_limit=10000,
    a_end_mcr_id="your-aws-direct-connect-gateway-id",
    b_end_mcr_id="coreweave-datacenter-pop-id",
    opts=pulumi.ResourceOptions(provider=megaport_provider),
)

# ENCRYPTION FOR THE AWS-FACING HOP OF THE BRIDGE
# Megaport's Vxc resource has no encryption property of its own - MACsec is
# a physical-link-layer feature configured on the AWS Direct Connect
# connection itself, not something Megaport's SDN exposes. This is the
# concrete mechanism behind this stack's encrypted-network requirement: AWS
# terminates and enforces the encryption; Megaport's circuit just carries
# the already-encrypted frames transparently between the two connection
# points. request_macsec provisions the connection with MACsec capability;
# encryption_mode="must_encrypt" enforces encryption rather than merely
# allowing it. Note this is a real physical link: MACsec requires a
# dedicated 10Gbps+ connection, is only available at select Direct Connect
# locations, and encryption_mode only takes effect once the connection
# reaches an "Available" state after its physical cross-connect is
# installed - this isn't something a single `pulumi up` completes
# end-to-end without a real colocation facility behind it.
aws_dx_connection = aws.directconnect.Connection(
    "aws-coreweave-dx-connection",
    name="aws-coreweave-macsec-link",
    bandwidth="10Gbps",
    location="EqDA2",  # placeholder Direct Connect location - replace with yours
    request_macsec=True,
    encryption_mode="must_encrypt",
)

# STACK EXPORTS
# NOTE: project name here ("data-platform") must match the `name:` field in
# this stack's Pulumi.yaml - the compute stack's StackReference resolves
# against the Pulumi project name, not the directory name.
pulumi.export("lakehouse_url", external_data_volume.url)
pulumi.export("checkpoint_bucket_id", checkpoint_bucket.id)
pulumi.export("databricks_url", databricks_workspace.workspace_url)
pulumi.export("cross_cloud_bridge_id", cross_cloud_bridge.id)
