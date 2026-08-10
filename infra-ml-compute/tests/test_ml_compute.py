import unittest
import pulumi

# 1. DEFINE THE ENGINE MOCK
# This intercepts Pulumi's attempts to call AWS or Kubernetes, returning fake data instead.
class MyMocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        # Return mock resource states and populate fake IDs
        outputs = args.inputs
        if args.kind == "Pod":
            outputs["metadata"] = {"name": args.name, "namespace": args.inputs.get("namespace")}
        return [args.name + "_id", outputs]

    def call(self, args: pulumi.runtime.MockCallArgs):
        # Mocks stack references and external data lookups
        if args.token == "pulumi:pulumi:getStackReference":
            return {
                "outputs": {
                    "lakehouse_url": "s3://mock-lakehouse-bucket/gold_layer/",
                    "checkpoint_bucket_id": "mock-checkpoint-bucket"
                }
            }
        return {}

# Register the mocks before importing our main code fabric
pulumi.runtime.set_mocks(
    MyMocks(),
    project="compute-runner",
    stack="production",
    preview=False
)

# Set up mock configuration values normally provided by Pulumi ESC
pulumi.runtime.set_config("compute-runner:upstream-data-project", "your-org/data-platform/production")
pulumi.runtime.set_config("coreweave:kubeconfig", "fake-kubeconfig-data")

# 2. IMPORT THE RUNNER CODE
# Importing it runs the declarative setup against our mocks above
import infra_ml_compute.__main__ as runner

# 3. DEFINE THE UNIT TESTS
class ComputeRunnerTests(unittest.TestCase):

    @pulumi.runtime.test
    def test_gpu_resource_limits_are_correct(self):
        """Verifies that exactly 8 H100 GPUs are requested using the exact CoreWeave string."""
        def check_limits(args):
            pod_spec = args[0]
            container = pod_spec["containers"][0]
            limits = container["resources"]["limits"]

            # Ensure we are using the official CoreWeave key, not a generalized string
            self.assertIn("://nvidia.com", limits)
            self.assertEqual(limits["://nvidia.com"], "8")

        return pulumi.Output.all(runner.training_pod.spec).apply(check_limits)

    @pulumi.runtime.test
    def test_error_trap_contains_infinite_sleep(self):
        """Ensures the error trap shield includes the infinite sleep command to protect NVMe data."""
        def check_bash_shield(args):
            pod_spec = args[0]
            container = pod_spec["containers"][0]
            bash_args = container["args"][0]

            # Assert that the custom failure handling script contains our debug hold loop
            self.assertIn("while true; do sleep 3600; done", bash_args)
            self.assertIn("Locking compute & NVMe states.", bash_args)

        return pulumi.Output.all(runner.training_pod.spec).apply(check_bash_shield)

    @pulumi.runtime.test
    def test_local_nvme_volume_mount_exists(self):
        """Ensures the high-speed local NVMe storage directory is correctly attached to the container."""
        def check_volumes(args):
            pod_spec = args[0]
            container = pod_spec["containers"][0]
            mounts = container["volumeMounts"]

            # Verify the physical disk inside the node chassis maps to our training folder
            self.assertEqual(mounts[0]["mountPath"], "/mnt/local")
            self.assertEqual(mounts[0]["name"], "local-nvme-scratch")

        return pulumi.Output.all(runner.training_pod.spec).apply(check_volumes)
