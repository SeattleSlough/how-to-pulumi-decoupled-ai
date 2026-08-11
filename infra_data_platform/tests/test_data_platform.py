# File: infra_data_platform/tests/test_data_platform.py
import unittest
import pulumi


class DataPlatformMocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        outputs = args.inputs
        if args.kind == "aws:ec2/vpc:Vpc":
            outputs["id"] = "vpc-mock-12345"
        elif args.kind == "aws:s3/bucketV2:BucketV2":
            outputs["id"] = args.inputs.get("bucket", args.name)
        elif args.kind == "databricks:index/mwsWorkspaces:MwsWorkspaces":
            outputs["workspaceUrl"] = "https://databricks.com"
        elif args.kind == "databricks:index/externalLocation:ExternalLocation":
            outputs["url"] = args.inputs.get("url")
        elif args.kind == "megaport:index/vxc:Vxc":
            outputs["id"] = "vxc-mock-67890"
        return [args.name + "_id", outputs]

    def call(self, args: pulumi.runtime.MockCallArgs):
        return {}


pulumi.runtime.set_mocks(
    DataPlatformMocks(),
    project="data-platform",
    stack="production",
    preview=False,
)

# Config values normally supplied by the ESC environment's pulumiConfig block
pulumi.runtime.set_config("databricks-account-id", "mock-db-id-000")
pulumi.runtime.set_config("megaportToken", "mock-megaport-token")

# Import runs the declarative setup against the mocks above.
# NOTE: this import only resolves if infra_data_platform is an underscored
# directory with an __init__.py - the earlier hyphenated directory name
# (infra-data-platform) made this import invalid Python regardless of what
# the test file itself did right.
import infra_data_platform.__main__ as data_platform


class DataPlatformTests(unittest.TestCase):
    @pulumi.runtime.test
    def test_vpc_subnet_allocation(self):
        """Verifies the VPC uses the expected internal private CIDR block."""
        def check_cidr(cidr_block):
            self.assertEqual(cidr_block, "10.0.0.0/16")
        return data_platform.databricks_vpc.cidr_block.apply(check_cidr)

    @pulumi.runtime.test
    def test_unity_catalog_storage_binding(self):
        """Validates Unity Catalog only links to the targeted gold-layer path."""
        def check_catalog_url(url):
            self.assertTrue(url.startswith("s3://"))
            self.assertIn("/gold_layer/llm_tokens/", url)
        return data_platform.external_data_volume.url.apply(check_catalog_url)

    @pulumi.runtime.test
    def test_s3_perimeter_naming_conventions(self):
        """Prevents deployment failures caused by accidental bucket name drift."""
        def check_bucket_names(args):
            lakehouse_id, checkpoint_id = args
            self.assertEqual(lakehouse_id, "enterprise-ai-training-lakehouse")
            self.assertEqual(checkpoint_id, "enterprise-ai-model-checkpoints")
        return pulumi.Output.all(
            data_platform.lakehouse_bucket.bucket,
            data_platform.checkpoint_bucket.bucket,
        ).apply(check_bucket_names)

    @pulumi.runtime.test
    def test_databricks_workspace_provisioned(self):
        """Ensures the workspace itself - not just the external location - exists."""
        def check_workspace(name):
            self.assertEqual(name, "ai-compute-hub")
        return data_platform.databricks_workspace.workspace_name.apply(check_workspace)
