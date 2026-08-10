# File: infra-data-platform/__main__.py
import pulumi
import pulumi_aws as aws
import pulumi_databricks as databricks
import pulumi_megaport as megaport

# 1. READ CONFIGURATION VALUES INJECTED BY PULUMI ESC
config = pulumi.Config()
# 'global' is the object namespace we defined in our ESC YAML environment file
esc_global = config.require_object("global")
databricks_id = esc_global.get("databricks-account-id")

# Fetch the decrypted Megaport token mapped to the environment via AWS Secrets Manager
# ESC makes this available automatically under the 'aws' block
esc_aws_secrets = config.require_object("aws")
megaport_token = esc_aws_secrets.get("secrets", {}).get("token")

# 2. PERMANENT DATA INFRASTRUCTURE
databricks_vpc = aws.ec2.Vpc("databricks-vpc",
    cidr_block="10.0.0.0/16",
    enable_dns_hostnames=True
)

lakehouse_bucket = aws.s3.BucketV2("enterprise-lakehouse", bucket="enterprise-ai-training-lakehouse")
checkpoint_bucket = aws.s3.BucketV2("model-checkpoints", bucket="enterprise-ai-model-checkpoints")

databricks_workspace = databricks.MwsWorkspaces("enterprise-workspace",
    account_id=databricks_id,
    aws_region="us-east-1",
    workspace_name="ai-compute-hub"
)

external_data_volume = databricks.ExternalLocation("ai-training-volume",
    name="production-ml-training-data",
    url=lakehouse_bucket.id.apply(lambda id: f"s3://{id}/gold_layer/llm_tokens/"),
    credential_name="aws-storage-iam-role"
)

# 3. PERMANENT NETWORK INFRASTRUCTURE (Utilizing the ESC secret token)
# Megaport provider handles the network plumbing over a private circuit
megaport_provider = megaport.Provider("mp-provider", api_token=megaport_token)

cross_cloud_bridge = megaport.Vxc("aws-coreweave-private-pipe",
    rate_limit=10000,
    a_end_mcr_id="your-aws-direct-connect-gateway-id",
    b_end_mcr_id="coreweave-datacenter-pop-id",
    opts=pulumi.ResourceOptions(provider=megaport_provider)
)

# STACK EXPORTS
pulumi.export("lakehouse_url", external_data_volume.url)
pulumi.export("checkpoint_bucket_id", checkpoint_bucket.id)
pulumi.export("databricks_url", databricks_workspace.workspace_url)
