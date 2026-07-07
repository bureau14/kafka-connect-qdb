#!/usr/bin/env python3
"""Buildkite dynamic pipeline generator for kafka-connect-qdb.

Step templates in steps/*.yml define nearly-complete Buildkite steps with
{placeholder} variables. This script loads them, substitutes variables, and
applies shared QuasarDB CI conventions for platform env, Docker, artifacts, and
pipeline validation.

Usage:
    python3 pipeline.py           # emit pipeline YAML to stdout
    python3 pipeline.py check     # validate without emitting
"""

from __future__ import annotations

import dataclasses
import os
import sys
from pathlib import Path

from buildkite_sdk import CommandStep, Pipeline

sys.path.insert(0, str(Path(__file__).parent / "tools"))
from qdb_pipeline import (  # noqa: E402
    Platform,
    apply_docker,
    get_git_ref,
    load_template,
    merge_env,
    select_platforms,
    set_artifact_plugin_options,
    validate_pipeline,
)

STEPS_DIR = Path(__file__).parent / "steps"

# Quasardb-specific toolchain overlays on top of shared infrastructure platforms.
_LINUX = dict(
    docker_image="bureau14/builder:rhel7",
    docker_volumes=("/var/lib/ccache:/var/lib/ccache",),
)
_WIN = dict()
_FREEBSD = dict()
_MACOS = dict()

_OS_OVERLAY = {"linux": _LINUX, "windows": _WIN, "freebsd": _FREEBSD, "macos": _MACOS}
PLATFORMS: list[Platform] = [
    dataclasses.replace(p, **_OS_OVERLAY.get(p.os, {}))
    for p in select_platforms(
        "freebsd-amd64-haswell",
        "linux-amd64-core2",
        "windows-amd64-core2",
        "macos-aarch64",
    )
]

BUILD_TYPES = ["Release"]

# Environment variable layering: global → step → os → os+step → platform.
GLOBAL_ENV: dict[str, str] = {
    "JAVA_PATH": "$$QDB_CICD_AGENT_JAVA_PATH",
    "JAVA_HOME": "$$QDB_CICD_AGENT_JAVA_HOME",
}

STEP_ENV: dict[str, dict[str, str]] = {}
OS_ENV: dict[str, dict[str, str]] = {
    "linux": {},
    "freebsd": {},
    "macos": {},
    "windows": {},
}
OS_STEP_ENV: dict[str, dict[str, str]] = {}

def _env(p: Platform, step_name: str, build_type: str) -> dict[str, str]:
    """Compose the full environment dict for one step."""
    return merge_env(
        GLOBAL_ENV,
        STEP_ENV.get(step_name, {}),
        OS_ENV.get(p.os, {}),
        OS_STEP_ENV.get(f"{p.os}/{step_name}", {}),
        {"CMAKE_BUILD_TYPE": build_type},
        platform=p,
    )


def generate_pipeline() -> Pipeline:
    """Load templates, expand across platforms × build_types, overlay env and docker."""
    pipeline = Pipeline()
    git_ref = get_git_ref()
    variants = []

    for p in PLATFORMS:
        for bt in BUILD_TYPES:
            slug = p.slug(bt.lower())
            variants.append(slug)
            qdb_dependency_slug = p.slug("release")

            tvars = {
                "slug": slug,
                "queue": (
                    f"{p.queue_os}-{p.arch}"
                    if p.os == "macos"
                    else f"default-{p.queue_os}-{p.arch}"
                ),
                "name": slug.replace("-", " ").title(),
            }

            artifact_vars_per_step = {
                "upload": {"variant": slug, "git-ref": git_ref},
                "promote": {"variant": slug, "git-ref": git_ref},
                "download": {
                    "git-ref": git_ref,
                    "by_project": {
                        "quasardb-build": {
                            "variant": qdb_dependency_slug,
                            "git-ref": git_ref,
                        },
                    },
                },
            }

            step = load_template(STEPS_DIR / "_build.yml", **tvars)
            env = _env(p, "build", bt)
            env.update(step.get("env") or {})
            step["env"] = env
            apply_docker(step, p.docker_image, p.docker_volumes)
            set_artifact_plugin_options(step, artifact_vars_per_step)
            pipeline.add_step(CommandStep.from_dict(step))

    step = load_template(STEPS_DIR / "_test_report.yml")
    step["depends_on"] = [f"build-{variant}" for variant in variants]
    pipeline.add_step(CommandStep.from_dict(step))

    return pipeline


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "generate"

    try:
        pipeline = generate_pipeline()
    except Exception as e:
        print(f"[FAIL] Pipeline generation failed: {e}", file=sys.stderr)
        sys.exit(1)

    if command == "generate":
        print(pipeline.to_yaml())
    elif command == "check":
        errors = validate_pipeline(pipeline)
        if errors:
            for e in errors:
                print(f"[FAIL] {e}", file=sys.stderr)
            sys.exit(1)
        print(f"[OK] Pipeline valid: {len(pipeline.steps)} steps")
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        print("Usage: pipeline.py [generate|check]", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
