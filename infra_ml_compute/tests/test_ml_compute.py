# File: infra_ml_compute/tests/test_ml_compute.py
import unittest
import pulumi


class ComputeMocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        outputs = args.inputs
        if args.kind == "kubernetes:core/v1:Pod":
            outputs["metadata"] = {"name": args.name, "namespace": args.inputs.get("metadata", {}).get("namespace")}
        return [args.name + "_id", outputs]

    def call(self, args: pulumi.runtime.MockCallArgs):
        if args.token == "pulumi:pulumi:getStackReference":
            return {
                "outputs": {
                    "lakehouse_url": "s3://mock-lakehouse-bucket/gold_layer/",
                    "checkpoint_bucket_id": "mock-checkpoint-bucket",
                }
            }
        return {}


pulumi.runtime.set_mocks(
    ComputeMocks(),
    project="compute-runner",
    stack="production",
    preview=False,
)

pulumi.runtime.set_config("coreweaveKubeconfig", "fake-kubeconfig-data")

# See the note in infra_data_platform/tests/test_data_platform.py - this
# import requires the underscored directory name plus an __init__.py.
import infra_ml_compute.__main__ as runner


class ComputeRunnerTests(unittest.TestCase):
    @pulumi.runtime.test
    def test_gpu_resource_limits_are_correct(self):
        """Verifies exactly 8 H100 GPUs are requested using the real device-plugin key.

        The earlier draft asserted "://nvidia.com" here, which locked in a
        malformed resource key instead of catching it - a mistake would have
        shipped straight through "shift-left validation." This checks
        against the correct key so a future typo actually fails the test.
        """
        def check_limits(args):
            pod_spec = args[0]
            container = pod_spec["containers"][0]
            limits = container["resources"]["limits"]
            self.assertIn("nvidia.com/gpu", limits)
            self.assertEqual(limits["nvidia.com/gpu"], "8")
        return pulumi.Output.all(runner.training_pod.spec).apply(check_limits)

    @pulumi.runtime.test
    def test_error_trap_contains_infinite_sleep(self):
        """Ensures the failure trap holds the node open instead of tearing it down."""
        def check_bash_shield(args):
            pod_spec = args[0]
            container = pod_spec["containers"][0]
            bash_args = container["args"][0]
            self.assertIn("while true; do sleep 3600; done", bash_args)
            self.assertIn("Locking compute & NVMe states.", bash_args)
        return pulumi.Output.all(runner.training_pod.spec).apply(check_bash_shield)

    @pulumi.runtime.test
    def test_local_nvme_volume_mount_exists(self):
        """Ensures the local NVMe scratch volume is correctly mounted."""
        def check_volumes(args):
            pod_spec = args[0]
            container = pod_spec["containers"][0]
            mounts = container["volumeMounts"]
            self.assertEqual(mounts[0]["mountPath"], "/mnt/local")
            self.assertEqual(mounts[0]["name"], "local-nvme-scratch")
        return pulumi.Output.all(runner.training_pod.spec).apply(check_volumes)
