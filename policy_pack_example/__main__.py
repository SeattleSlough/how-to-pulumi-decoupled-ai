# File: policy-pack-example/__main__.py
#
# EXAMPLE POLICY PACK - NOT a statement of which policies this architecture
# actually needs. The two policies below are deliberately simple and check
# things this repo's own stacks already do correctly.
# They exist to show the pattern working end-to-end, not to
# define your organization's real compliance posture. A production policy
# pack would come from a pre-built compliance framework pack or your own
# platform team, not from a how-to repo.
from pulumi_policy import (
    EnforcementLevel,
    PolicyPack,
    ResourceValidationPolicy,
)


def s3_blocks_public_access_validator(resource, report_violation):
    """Requires S3 buckets to explicitly block public access.

    infra_data_platform already does this for both buckets it creates
    (see the BucketPublicAccessBlock loop in __main__.py) - this policy
    exists to demonstrate that the check runs automatically as part of
    `pulumi preview`/`up`, not to be relied on as the only thing enforcing
    it. Shown here at advisory vs mandatory so a real violation reports
    without blocking anything.
    """
    if resource.resource_type == "aws:s3/bucketPublicAccessBlock:BucketPublicAccessBlock":
        props = resource.props
        required_true = (
            "blockPublicAcls",
            "blockPublicPolicy",
            "ignorePublicAcls",
            "restrictPublicBuckets",
        )
        missing = [key for key in required_true if not props.get(key)]
        if missing:
            report_violation(
                f"S3 bucket public-access-block settings must all be true; "
                f"missing/false: {', '.join(missing)}."
            )


def gpu_pod_requires_resource_limits_validator(resource, report_violation):
    """Requires GPU training pods to declare explicit resource limits.

    infra_ml_compute's training_pod already sets nvidia.com/gpu, cpu, and
    memory limits - see the resource_limits test in
    infra_ml_compute/tests/test_ml_compute.py, which checks the same thing
    at the pytest/mocks layer. This policy checks the same property at the
    infrastructure-policy layer instead, to show the two mechanisms are
    complementary, not redundant: pytest validates your own resource graph
    against your own expectations before anything deploys; a policy pack
    validates configuration against an organization-wide rule regardless of
    who wrote the Pulumi program or whether they knew the rule existed.
    """
    if resource.resource_type == "kubernetes:core/v1:Pod":
        containers = (resource.props.get("spec") or {}).get("containers") or []
        for container in containers:
            limits = (container.get("resources") or {}).get("limits")
            if not limits:
                report_violation(
                    f"Container '{container.get('name', '<unnamed>')}' must "
                    f"declare resource limits."
                )


s3_blocks_public_access = ResourceValidationPolicy(
    name="s3-blocks-public-access",
    description="S3 buckets must block public access explicitly.",
    validate=s3_blocks_public_access_validator,
)

gpu_pod_requires_resource_limits = ResourceValidationPolicy(
    name="gpu-pod-requires-resource-limits",
    description="GPU training pods must declare explicit resource limits.",
    validate=gpu_pod_requires_resource_limits_validator,
)

PolicyPack(
    name="decoupled-ai-example-policies",
    enforcement_level=EnforcementLevel.ADVISORY,
    policies=[
        s3_blocks_public_access,
        gpu_pod_requires_resource_limits,
    ],
)

