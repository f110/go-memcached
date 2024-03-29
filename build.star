job(
    name = "test_all",
    command = "test",
    all_revision = True,
    github_status = True,
    targets = [
        "//...",
    ],
    platforms = [
        "@io_bazel_rules_go//go/toolchain:linux_amd64",
    ],
    cpu_limit = "2000m",
    memory_limit = "4096Mi",
    event = ["push", "pull_request"],
)
