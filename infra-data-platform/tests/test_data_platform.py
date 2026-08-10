# File: infra-data-platform/tests/test_data_platform.py
import unittest
import pulumi

# 1. DEFINE AWS & DATABRICKS INFRASTRUCTURE MOCKS
class DataPlatformMocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        outputs = args.inputs
        # Assign mock platform resource IDs natively
        if args.kind == "aws:ec2/vpc:Vpc":
            outputs["id"] = "vpc-mock-12345"
        elif args.kind == "aws:s3/bucketV2:BucketV2":
            outputs["id"] = args.inputs.get("bucket", args.name)
        elif args.kind == "databricks:index/mwsWorkspaces:MwsWorkspaces":
            outputs["workspace_url"] = "https://databricks.com"
        elif args.kind == "databricks:index/externalLocation:ExternalLocation":
            outputs["url"] = args.inputs.get("url")

        return [args.name + "_id", outputs]

    def call(self, args: pulumi.runtime.MockCallArgs):
        return {}

# Register infrastructure mocks with the engine
pulumi.runtime.set_mocks(
    DataPlatformMocks(),
    project="data-platform",
    stack="production",
    preview=False
)

# Populate configuration mock parameters derived from central ESC layouts
pulumi.runtime.set_config("global", {"databricks-account-id": "mock-db-id-000"})
pulumi.runtime.set_config("aws", {"secrets": {"token": "mock-megaport-token"}})

# 2. IMPORT THE TARGET SOURCE CODE
import infra_data_platform.__main__ as data_platform

# 3. CONSTRUCT ASSERTI0N CASES
class DataPlatformTests(unittest.TestCase):

    @pulumi.runtime.test
    def test_vpc_subnet_allocation(self):
        """Verifies corporate networks utilize safe internal private CIDR block parameters."""
        def check_cidr(cidr_block):
            self.assertEqual(cidr_block, "10.0.0.0/16")
        return data_platform.databricks_vpc.cidr_block.apply(check_cidr)

    @pulumi.runtime.test
    def test_unity_catalog_storage_binding(self):
        """Validates that Unity Catalog only links to the targeted enterprise gold token layers."""
        def check_catalog_url(url):
            self.assertTrue(url.startswith("s3://"))
            self.assertIn("/gold_layer/llm_tokens/", url)
        return data_platform.external_data_volume.url.apply(check_catalog_url)

    @pulumi.runtime.test
    def test_s3_perimeter_naming_conventions(self):
        """Prevents deployment failures caused by accidental bucket configuration mismatches."""
        def check_bucket_names(args):
            lakehouse_id, checkpoint_id = args
            self.assertEqual(lakehouse_id, "enterprise-ai-training-lakehouse")
            self.assertEqual(checkpoint_id, "enterprise-ai-model-checkpoints")
        return pulumi.Output.all(
            data_platform.lakehouse_bucket.bucket,
            data_platform.checkpoint_bucket.bucket
        ).apply(check_bucket_names)
