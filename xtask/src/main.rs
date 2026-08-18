use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

const REQUIRED_RULES: [&str; 18] = [
    "00-overview.md",
    "01-project-boundary.md",
    "02-workspace-crate-rules.md",
    "03-naming-api-rules.md",
    "04-type-domain-modeling-rules.md",
    "05-ownership-borrowing-rules.md",
    "06-error-panic-rules.md",
    "07-concurrency-sync-rules.md",
    "08-async-tokio-rules.md",
    "09-state-machine-rules.md",
    "10-persistence-wal-snapshot-rules.md",
    "11-network-protocol-rules.md",
    "12-consensus-raft-rules.md",
    "13-safety-unsafe-ffi-rules.md",
    "14-dependency-supply-chain-rules.md",
    "15-testing-verification-rules.md",
    "16-performance-memory-rules.md",
    "17-observability-operations-rules.md",
];

const REQUIRED_SKILLS: [(&str, &str); 2] = [
    (
        ".agents/rust_dev_rules/skills/rust-production-engineering/SKILL.md",
        "name: rust-production-engineering",
    ),
    (
        ".agents/rust_dev_rules/skills/rust-distributed-broker/SKILL.md",
        "name: rust-distributed-broker",
    ),
];

#[derive(Debug)]
struct HarnessError(String);

impl fmt::Display for HarnessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for HarnessError {}

type HarnessResult<T> = Result<T, Box<dyn Error>>;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> HarnessResult<()> {
    let command = env::args().nth(1).unwrap_or_else(|| "help".to_owned());
    let root = workspace_root()?;

    match command.as_str() {
        "rules" => verify_rules(&root),
        "check" => run_check(&root),
        "test" => run_tests(&root),
        "ci" => run_ci(&root),
        "cutover" => run_cutover_checks(&root),
        "extended" => run_extended_checks(&root),
        "perf" => run_performance_checks(&root),
        "doctor" => run_doctor(&root),
        "deps" => run_dependency_checks(&root),
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        other => Err(HarnessError(format!(
            "unknown xtask command {other:?}; run `cargo xtask help`"
        ))
        .into()),
    }
}

fn workspace_root() -> HarnessResult<PathBuf> {
    let mut candidate = env::current_dir()?;
    loop {
        let cargo_toml = candidate.join("Cargo.toml");
        let xtask_manifest = candidate.join("xtask/Cargo.toml");
        if cargo_toml.is_file() && xtask_manifest.is_file() {
            let cargo_toml = fs::read_to_string(&cargo_toml)?;
            if cargo_toml.contains("[workspace]") {
                return Ok(candidate);
            }
        }
        if !candidate.pop() {
            break;
        }
    }
    Err(HarnessError(
        "xtask workspace root could not be resolved from the current directory".to_owned(),
    )
    .into())
}

fn verify_rules(root: &Path) -> HarnessResult<()> {
    let rules_dir = root.join(".agents/rust_dev_rules/rules");
    let mut actual_markdown = BTreeSet::new();

    for entry in fs::read_dir(&rules_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            return Err(HarnessError("rule filename is not valid UTF-8".to_owned()).into());
        };
        if Path::new(file_name)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("md"))
        {
            actual_markdown.insert(file_name.to_owned());
        }
    }

    let mut expected_markdown = BTreeSet::new();
    expected_markdown.insert("README.md".to_owned());
    for rule in REQUIRED_RULES {
        expected_markdown.insert(rule.to_owned());
    }

    if actual_markdown != expected_markdown {
        let missing = expected_markdown
            .difference(&actual_markdown)
            .cloned()
            .collect::<Vec<_>>();
        let unexpected = actual_markdown
            .difference(&expected_markdown)
            .cloned()
            .collect::<Vec<_>>();
        return Err(HarnessError(format!(
            "Rust rule set mismatch: missing={missing:?}, unexpected={unexpected:?}"
        ))
        .into());
    }

    verify_rule_document(&rules_dir.join("README.md"))?;
    for rule in REQUIRED_RULES {
        verify_rule_document(&rules_dir.join(rule))?;
    }

    for (relative_path, expected_name) in REQUIRED_SKILLS {
        verify_skill(root, relative_path, expected_name)?;
    }

    verify_broker_profile(root)?;
    verify_workspace_policy(root)?;
    verify_dependency_policy(root)?;

    println!(
        "rust rules: ok ({} numbered rules, {} mandatory skills)",
        REQUIRED_RULES.len(),
        REQUIRED_SKILLS.len()
    );
    Ok(())
}

fn verify_rule_document(path: &Path) -> HarnessResult<()> {
    let content = fs::read_to_string(path)?;
    if content.len() < 256 {
        return Err(HarnessError(format!(
            "rule document is unexpectedly small: {}",
            path.display()
        ))
        .into());
    }
    if !content.starts_with("# ") {
        return Err(HarnessError(format!(
            "rule document must start with an H1: {}",
            path.display()
        ))
        .into());
    }
    if !content.contains("\n## ") {
        return Err(HarnessError(format!(
            "rule document must contain at least one H2 section: {}",
            path.display()
        ))
        .into());
    }
    Ok(())
}

fn verify_skill(root: &Path, relative_path: &str, expected_name: &str) -> HarnessResult<()> {
    let path = root.join(relative_path);
    let content = fs::read_to_string(&path)?;
    if !content.starts_with("---\n") || !content.contains(expected_name) {
        return Err(HarnessError(format!(
            "mandatory Rust skill has invalid frontmatter: {}",
            path.display()
        ))
        .into());
    }
    Ok(())
}

fn verify_broker_profile(root: &Path) -> HarnessResult<()> {
    let path = root.join(".agents/rust_dev_rules/BROKER_PROFILE.md");
    let content = fs::read_to_string(&path)?;
    let required_references = [
        ".agents/rust_dev_rules/rules/00-overview.md",
        ".agents/rust_dev_rules/skills/rust-production-engineering/SKILL.md",
        ".agents/rust_dev_rules/skills/rust-distributed-broker/SKILL.md",
    ];
    for reference in required_references {
        if !content.contains(reference) {
            return Err(
                HarnessError(format!("BROKER_PROFILE.md must reference {reference}")).into(),
            );
        }
    }
    Ok(())
}

fn verify_workspace_policy(root: &Path) -> HarnessResult<()> {
    let cargo_toml = fs::read_to_string(root.join("Cargo.toml"))?;
    let required_fragments = [
        "resolver = \"3\"",
        "[workspace.lints.rust]",
        "unsafe_code = \"forbid\"",
        "[workspace.lints.clippy]",
    ];
    for fragment in required_fragments {
        if !cargo_toml.contains(fragment) {
            return Err(HarnessError(format!(
                "Cargo.toml is missing required Rust policy fragment: {fragment}"
            ))
            .into());
        }
    }
    Ok(())
}

fn verify_dependency_policy(root: &Path) -> HarnessResult<()> {
    let deny_toml = fs::read_to_string(root.join("deny.toml"))?;
    for fragment in [
        "[advisories]",
        "[licenses]",
        "[bans]",
        "wildcards = \"deny\"",
        "[sources]",
        "unknown-registry = \"deny\"",
        "unknown-git = \"deny\"",
    ] {
        if !deny_toml.contains(fragment) {
            return Err(HarnessError(format!(
                "deny.toml is missing required dependency policy fragment: {fragment}"
            ))
            .into());
        }
    }
    Ok(())
}

fn verify_production_cutover(root: &Path) -> HarnessResult<()> {
    for retired_path in [
        "pyproject.toml",
        "uv.lock",
        ".python-version",
        ".agents/MANIFEST.json",
        ".agents/verify_manifest.py",
    ] {
        let path = root.join(retired_path);
        if path.exists() {
            return Err(HarnessError(format!(
                "retired Python Broker artifact reappeared: {}",
                path.display()
            ))
            .into());
        }
    }

    for retired_directory in ["src", "tests", "benchmarks"] {
        let directory = root.join(retired_directory);
        if directory.exists() && directory_contains_files(&directory)? {
            return Err(HarnessError(format!(
                "retired Python Broker directory contains files: {}",
                directory.display()
            ))
            .into());
        }
    }

    let runtime_manifest = fs::read_to_string(root.join("crates/agent-broker-runtime/Cargo.toml"))?;
    if !runtime_manifest.contains("[[bin]]")
        || !runtime_manifest.contains("name = \"agentbrokerd\"")
    {
        return Err(HarnessError(
            "Rust agent-broker-runtime must own the agentbrokerd binary".to_owned(),
        )
        .into());
    }

    let makefile = fs::read_to_string(root.join("Makefile"))?;
    if !makefile.contains("process_e2e: rust_process_e2e") {
        return Err(HarnessError(
            "Agent Broker Makefile must route generic process_e2e to Rust".to_owned(),
        )
        .into());
    }
    for required in [
        "process_e2e: rust_process_e2e",
        "ci: cutover rust_process_e2e",
        "cargo test -p agent-broker-runtime --test process_restart_e2e",
    ] {
        if !makefile.contains(required) {
            return Err(HarnessError(format!(
                "Rust-only Makefile is missing canonical fragment: {required}"
            ))
            .into());
        }
    }
    if makefile.contains("reference_")
        || makefile.contains("PYTHON")
        || makefile.contains("python")
        || makefile.contains("uv ")
    {
        return Err(HarnessError(
            "Rust-only Makefile must not retain Python reference execution paths".to_owned(),
        )
        .into());
    }

    verify_python_client_sdk_boundary(root)?;

    println!(
        "production cutover: ok (Rust Broker authority; retired Python Broker absent; client SDK isolated)"
    );
    Ok(())
}

fn verify_python_client_sdk_boundary(root: &Path) -> HarnessResult<()> {
    let sdk_manifest_path = root.join("sdks/python/pyproject.toml");
    let sdk_manifest = fs::read_to_string(&sdk_manifest_path).map_err(|error| {
        HarnessError(format!(
            "Python client SDK manifest is required at {}: {error}",
            sdk_manifest_path.display()
        ))
    })?;
    for fragment in [
        "name = \"herterarchy-broker-sdk\"",
        "Typed Python client SDK for the Rust Heterarchy Agent Broker",
        "dependencies = []",
        "where = [\"src\"]",
    ] {
        if !sdk_manifest.contains(fragment) {
            return Err(HarnessError(format!(
                "Python client SDK manifest is missing required client-only fragment: {fragment}"
            ))
            .into());
        }
    }
    if sdk_manifest.contains("[project.scripts]") || sdk_manifest.contains("agentbrokerd") {
        return Err(HarnessError(
            "Python client SDK must not define Broker executable authority".to_owned(),
        )
        .into());
    }

    let package_root = root.join("sdks/python/src/agent_broker");
    for required in [
        "__init__.py",
        "client.py",
        "cluster.py",
        "errors.py",
        "models.py",
        "protocol.py",
        "standalone.py",
    ] {
        let path = package_root.join(required);
        if !path.is_file() {
            return Err(HarnessError(format!(
                "Python client SDK is missing required boundary file: {}",
                path.display()
            ))
            .into());
        }
    }
    Ok(())
}

fn directory_contains_files(directory: &Path) -> HarnessResult<bool> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_file() {
            return Ok(true);
        }
        if file_type.is_dir() && directory_contains_files(&entry.path())? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn run_check(root: &Path) -> HarnessResult<()> {
    run_cargo(root, "rustfmt", &["fmt", "--all", "--", "--check"])?;
    run_cargo(
        root,
        "cargo check",
        &["check", "--workspace", "--all-targets"],
    )?;
    run_cargo(
        root,
        "clippy",
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
    )
}

fn run_tests(root: &Path) -> HarnessResult<()> {
    run_cargo(
        root,
        "cargo test",
        &["test", "--workspace", "--all-targets"],
    )
}

fn run_ci(root: &Path) -> HarnessResult<()> {
    verify_rules(root)?;
    verify_production_cutover(root)?;
    run_git_hygiene(root)?;
    run_check(root)?;
    run_tests(root)
}

fn run_git_hygiene(root: &Path) -> HarnessResult<()> {
    let whitespace = Command::new("git")
        .args(["diff", "--check", "HEAD", "--", "."])
        .current_dir(root)
        .status()?;
    if !whitespace.success() {
        return Err(
            HarnessError(format!("git diff --check failed with status {whitespace}")).into(),
        );
    }

    let protected = Command::new("git")
        .args([
            "diff",
            "--quiet",
            "HEAD",
            "--",
            "compatibility",
            "fuzz/seeds",
        ])
        .current_dir(root)
        .status()?;
    if protected.success() {
        let protected_status = Command::new("git")
            .args([
                "status",
                "--porcelain",
                "--untracked-files=all",
                "--",
                "compatibility",
                "fuzz/seeds",
            ])
            .current_dir(root)
            .output()?;
        if !protected_status.status.success() {
            return Err(HarnessError(format!(
                "protected-tree git status failed with status {}",
                protected_status.status
            ))
            .into());
        }
        if protected_status.stdout.is_empty() {
            return Ok(());
        }
        return Err(HarnessError(
            "frozen compatibility/** or fuzz/seeds/** contains tracked or untracked changes"
                .to_owned(),
        )
        .into());
    }
    if protected.code() == Some(1) {
        return Err(HarnessError(
            "frozen compatibility/** or fuzz/seeds/** changed relative to HEAD".to_owned(),
        )
        .into());
    }
    Err(HarnessError(format!(
        "protected-tree git diff failed with status {protected}"
    ))
    .into())
}

fn run_cutover_checks(root: &Path) -> HarnessResult<()> {
    run_ci(root)?;
    run_performance_checks(root)
}

fn run_performance_checks(root: &Path) -> HarnessResult<()> {
    run_cargo(
        root,
        "release snapshot transport RSS budget",
        &[
            "test",
            "--release",
            "-p",
            "agent-broker-consensus",
            "snapshot_binary_transfer_peak_rss_is_bounded",
            "--",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ],
    )?;
    run_cargo(
        root,
        "release perf build",
        &[
            "build",
            "--release",
            "-p",
            "agent-broker-runtime",
            "--bin",
            "agentbrokerd",
            "--example",
            "perf_probe",
        ],
    )?;
    run_cargo(
        root,
        "release perf probe",
        &[
            "run",
            "--release",
            "-p",
            "agent-broker-runtime",
            "--example",
            "perf_probe",
        ],
    )
}

fn verify_fuzz_seed_corpus(root: &Path) -> HarnessResult<()> {
    let mut protocol_expected = verify_line_seed_set(
        root,
        "compatibility/wire-v1/request_frames.ndjson",
        "fuzz/seeds/protocol_v1",
        "request",
    )?;
    protocol_expected.extend(verify_line_seed_set(
        root,
        "compatibility/wire-v1/response_frames.ndjson",
        "fuzz/seeds/protocol_v1",
        "response",
    )?);
    let protocol_actual = regular_file_names(&root.join("fuzz/seeds/protocol_v1"))?;
    if protocol_actual != protocol_expected {
        return Err(HarnessError(format!(
            "protocol fuzz seed set mismatch: expected={protocol_expected:?}, actual={protocol_actual:?}"
        ))
        .into());
    }

    let journal_expected = verify_line_seed_set(
        root,
        "compatibility/storage-v1/journal.ndjson",
        "fuzz/seeds/journal_v1",
        "mutation",
    )?;
    let journal_actual = regular_file_names(&root.join("fuzz/seeds/journal_v1"))?;
    if journal_actual != journal_expected {
        return Err(HarnessError(format!(
            "journal fuzz seed set mismatch: expected={journal_expected:?}, actual={journal_actual:?}"
        ))
        .into());
    }

    let snapshot = fs::read(root.join("compatibility/storage-v1/snapshot.json"))?;
    let snapshot_seed =
        fs::read(root.join("fuzz/seeds/snapshot_v1/python-reference-snapshot.json"))?;
    if snapshot != snapshot_seed {
        return Err(
            HarnessError("snapshot fuzz seed drifted from Python corpus".to_owned()).into(),
        );
    }
    let snapshot_files = regular_file_names(&root.join("fuzz/seeds/snapshot_v1"))?;
    let expected_snapshot = BTreeSet::from(["python-reference-snapshot.json".to_owned()]);
    if snapshot_files != expected_snapshot {
        return Err(HarnessError(format!(
            "snapshot fuzz seed set mismatch: actual={snapshot_files:?}"
        ))
        .into());
    }

    println!("fuzz seeds: ok (Python executable corpora are byte-aligned)");
    Ok(())
}

fn verify_line_seed_set(
    root: &Path,
    source_relative: &str,
    seed_relative: &str,
    prefix: &str,
) -> HarnessResult<BTreeSet<String>> {
    let source = fs::read(root.join(source_relative))?;
    let frames = source
        .split_inclusive(|byte| *byte == b'\n')
        .filter(|frame| !frame.is_empty())
        .collect::<Vec<_>>();
    if frames.is_empty() || frames.iter().any(|frame| !frame.ends_with(b"\n")) {
        return Err(HarnessError(format!(
            "compatibility corpus must contain newline-framed records: {source_relative}"
        ))
        .into());
    }

    let seed_directory = root.join(seed_relative);
    let mut expected_files = BTreeSet::new();
    for (index, frame) in frames.iter().enumerate() {
        let file_name = format!("{prefix}-{:02}.ndjson", index + 1);
        let seed = fs::read(seed_directory.join(&file_name))?;
        if seed != *frame {
            return Err(HarnessError(format!(
                "fuzz seed drifted from compatibility corpus: {seed_relative}/{file_name}"
            ))
            .into());
        }
        expected_files.insert(file_name);
    }
    Ok(expected_files)
}

fn regular_file_names(directory: &Path) -> HarnessResult<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return Err(HarnessError(format!(
                "fuzz seed filename is not valid UTF-8: {}",
                entry.path().display()
            ))
            .into());
        };
        names.insert(name);
    }
    Ok(names)
}

fn run_extended_checks(root: &Path) -> HarnessResult<()> {
    verify_fuzz_seed_corpus(root)?;
    run_cargo(
        root,
        "fuzz rustfmt",
        &[
            "fmt",
            "--manifest-path",
            "fuzz/Cargo.toml",
            "--all",
            "--",
            "--check",
        ],
    )?;
    run_nightly_cargo(root, "fuzz build", &["fuzz", "build"])?;
    for target in ["protocol_v1", "snapshot_v1", "journal_v1"] {
        let mutable_corpus = prepare_fuzz_runtime_corpus(root, target)?;
        run_nightly_cargo(
            root,
            "fuzz smoke",
            &[
                "fuzz",
                "run",
                target,
                mutable_corpus.as_str(),
                "--",
                "-max_total_time=2",
            ],
        )?;
    }
    let v3_corpus = prepare_empty_fuzz_runtime_corpus(root, "protocol_v3")?;
    run_nightly_cargo(
        root,
        "fuzz smoke",
        &[
            "fuzz",
            "run",
            "protocol_v3",
            v3_corpus.as_str(),
            "--",
            "-max_total_time=2",
        ],
    )?;
    verify_fuzz_seed_corpus(root)?;
    println!("fuzz seeds: immutable after cargo-fuzz smoke runs");
    Ok(())
}

fn prepare_fuzz_runtime_corpus(root: &Path, target: &str) -> HarnessResult<String> {
    let seed_directory = root.join("fuzz/seeds").join(target);
    let corpus_directory = root.join("fuzz/corpus").join(target);
    fs::create_dir_all(&corpus_directory)?;

    for entry in fs::read_dir(&seed_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        fs::copy(entry.path(), corpus_directory.join(entry.file_name()))?;
    }

    Ok(corpus_directory.to_string_lossy().into_owned())
}

fn prepare_empty_fuzz_runtime_corpus(root: &Path, target: &str) -> HarnessResult<String> {
    let corpus_directory = root.join("fuzz/corpus").join(target);
    fs::create_dir_all(&corpus_directory)?;
    Ok(corpus_directory.to_string_lossy().into_owned())
}

fn run_doctor(root: &Path) -> HarnessResult<()> {
    println!("workspace: {}", root.display());
    print_tool_version(root, "rustc", &["--version"])?;
    print_tool_version(root, "cargo", &["--version"])?;
    print_optional_cargo_subcommand(root, "fuzz")?;
    print_optional_cargo_subcommand(root, "deny")?;
    print_optional_cargo_subcommand(root, "nextest")?;
    print_optional_program(root, "rustup", &["--version"]);
    print_optional_rustup_toolchain(root, "nightly");
    Ok(())
}

fn run_dependency_checks(root: &Path) -> HarnessResult<()> {
    verify_dependency_policy(root)?;
    if !cargo_subcommand_available(root, "deny")? {
        return Err(HarnessError(
            "cargo-deny is not installed; install it explicitly before running `cargo xtask deps`"
                .to_owned(),
        )
        .into());
    }
    run_cargo(root, "cargo-deny", &["deny", "check"])
}

fn run_nightly_cargo(root: &Path, label: &str, cargo_args: &[&str]) -> HarnessResult<()> {
    println!("==> {label} (nightly)");
    let home = env::var_os("HOME")
        .ok_or_else(|| HarnessError("HOME is unavailable for the rustup proxy path".to_owned()))?;
    let proxy_dir = PathBuf::from(home).join(".cargo/bin");
    let cargo_proxy = proxy_dir.join("cargo");
    let rustc_proxy = proxy_dir.join("rustc");
    if !cargo_proxy.is_file() || !rustc_proxy.is_file() {
        return Err(HarnessError(format!(
            "rustup cargo/rustc proxies are missing under {}",
            proxy_dir.display()
        ))
        .into());
    }

    let current_path = env::var_os("PATH").unwrap_or_default();
    let mut search_path = vec![proxy_dir];
    search_path.extend(env::split_paths(&current_path));
    let nightly_path = env::join_paths(search_path)?;

    let status = Command::new(&cargo_proxy)
        .args(cargo_args)
        .current_dir(root)
        .env("PATH", nightly_path)
        .env("RUSTUP_TOOLCHAIN", "nightly")
        .env("RUSTUP_AUTO_INSTALL", "0")
        .status()
        .map_err(|error| {
            HarnessError(format!(
                "nightly fuzz tooling is unavailable through the rustup proxy: {error}"
            ))
        })?;
    if status.success() {
        return Ok(());
    }
    Err(HarnessError(format!(
        "{label} failed under the explicit nightly toolchain with status {status}"
    ))
    .into())
}

fn run_cargo(root: &Path, label: &str, args: &[&str]) -> HarnessResult<()> {
    println!("==> {label}");
    let status = Command::new("cargo")
        .args(args)
        .current_dir(root)
        .status()?;
    if status.success() {
        return Ok(());
    }
    Err(HarnessError(format!("{label} failed with status {status}")).into())
}

fn print_tool_version(root: &Path, program: &str, args: &[&str]) -> HarnessResult<()> {
    let output = Command::new(program)
        .args(args)
        .current_dir(root)
        .output()?;
    if !output.status.success() {
        return Err(HarnessError(format!(
            "{program} version check failed with status {}",
            output.status
        ))
        .into());
    }
    let version = String::from_utf8_lossy(&output.stdout);
    println!("{program}: {}", version.trim());
    Ok(())
}

fn print_optional_cargo_subcommand(root: &Path, subcommand: &str) -> HarnessResult<()> {
    let available = cargo_subcommand_available(root, subcommand)?;
    println!(
        "cargo-{subcommand}: {}",
        if available {
            "available"
        } else {
            "not installed"
        }
    );
    Ok(())
}

fn cargo_subcommand_available(root: &Path, subcommand: &str) -> HarnessResult<bool> {
    let output = Command::new("cargo")
        .args([subcommand, "--version"])
        .current_dir(root)
        .output()?;
    Ok(output.status.success())
}

fn print_optional_rustup_toolchain(root: &Path, toolchain: &str) {
    let output = Command::new("rustup")
        .args(["run", toolchain, "rustc", "--version"])
        .current_dir(root)
        .env("RUSTUP_AUTO_INSTALL", "0")
        .output();
    match output {
        Ok(output) if output.status.success() => {
            let version = String::from_utf8_lossy(&output.stdout);
            println!("rustup-{toolchain}: {}", version.trim());
        }
        Ok(_) | Err(_) => println!("rustup-{toolchain}: not installed"),
    }
}

fn print_optional_program(root: &Path, program: &str, args: &[&str]) {
    match Command::new(program).args(args).current_dir(root).output() {
        Ok(output) if output.status.success() => {
            let version = String::from_utf8_lossy(&output.stdout);
            println!("{program}: {}", version.trim());
        }
        Ok(_) | Err(_) => println!("{program}: not installed"),
    }
}

fn print_help() {
    println!(
        "Agent Broker repository tasks:\n\
         \n\
         cargo xtask rules   Validate the split Rust rule set and mandatory skills.\n\
         cargo xtask check   Run rustfmt, cargo check, and strict Clippy.\n\
         cargo xtask test    Run workspace tests and rustdoc tests.\n\
         cargo xtask ci      Run rules + production-cutover policy + check + test.\n\
         cargo xtask cutover Run Rust production CI + release performance gate.\n\
         cargo xtask extended Verify golden seeds + cargo-fuzz build + bounded smoke fuzzing.\n\
         cargo xtask perf    Run release-profile Broker performance regression gates.\n\
         cargo xtask doctor  Report Rust and optional tooling availability.\n\
         cargo xtask deps    Run cargo-deny when explicitly installed."
    );
}
