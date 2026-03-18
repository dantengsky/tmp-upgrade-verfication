#!/usr/bin/env python3
"""Databend Upgrade Test - Main Orchestrator

Usage: ./run.py [options] [phase...]

Phases: download, meta-setup, meta-upgrade, query-setup, compat-test, all
Options:
  --initial-meta VER    Initial meta version (default: 1.2.307)
  --work-dir DIR        Working directory (default: ./work)
  --cleanup             Remove work dir before start
  --skip-download       Skip download phase
  --skip-experimental-grants  Skip new grant object type permission tests
  --mode MODE           Deploy mode: local (default) or distributed
  --meta-hosts IPs      Comma-separated meta host IPs (distributed mode)
  --query-hosts IPs     Comma-separated query host IPs (distributed mode)
  --help                Show this help
"""

import argparse
import os
import platform
import re
import signal
import subprocess
import sys
import time
from datetime import datetime

import yaml

from remote import LocalHost, RemoteHost, check_ssh_connectivity, distribute_binaries

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Load .env file (project root = parent of scripts/)
_env_file = os.path.join(os.path.dirname(SCRIPT_DIR), ".env")
if os.path.isfile(_env_file):
    with open(_env_file) as _f:
        for _line in _f:
            _line = _line.strip()
            if not _line or _line.startswith("#"):
                continue
            if "=" in _line:
                _k, _v = _line.split("=", 1)
                _k = _k.strip()
                _v = _v.strip().strip('"').strip("'")
                if _k not in os.environ:  # don't override existing env
                    os.environ[_k] = _v

# ============================================================================
# Configuration
# ============================================================================

DOWNLOAD_BASE_URL = os.environ.get(
    "DOWNLOAD_BASE_URL", ""
)

# Load version URLs from versions.yaml
_versions_file = os.path.join(os.path.dirname(SCRIPT_DIR), "versions.yaml")
with open(_versions_file) as _f:
    _versions = yaml.safe_load(_f)

if not DOWNLOAD_BASE_URL:
    DOWNLOAD_BASE_URL = _versions.get("download_base_url", "")

META_URLS = {
    ver: f"{DOWNLOAD_BASE_URL}/{filename}"
    for ver, filename in _versions.get("meta", {}).items()
}

QUERY_URLS = {
    ver: f"{DOWNLOAD_BASE_URL}/{filename}"
    for ver, filename in _versions.get("query", {}).items()
}


def _env(name, default):
    return os.environ.get(name, default)


class Config:
    def __init__(self):
        self.initial_meta = _env("INITIAL_META_VERSION", "1.2.307")
        self.intermediate_meta = _env("INTERMEDIATE_META_VERSION", "1.2.768")
        self.target_meta = _env("TARGET_META_VERSION", "1.2.879")
        self.old_query = _env("OLD_QUERY_VERSION", "1.2.636")
        self.new_query = _env("NEW_QUERY_VERSION", "1.2.887")

        self.work_dir = _env("WORK_DIR", os.path.join(SCRIPT_DIR, "work"))
        self.meta_cluster_size = int(_env("META_CLUSTER_SIZE", "3"))
        self.meta_grpc_port_base = int(_env("META_GRPC_PORT_BASE", "9191"))
        self.meta_raft_port_base = int(_env("META_RAFT_PORT_BASE", "28101"))
        self.meta_admin_port_base = int(_env("META_ADMIN_PORT_BASE", "28201"))
        self.meta_listen_host = _env("META_LISTEN_HOST", "127.0.0.1")

        self.query_a = {
            "mysql_host": _env("QUERY_A_MYSQL_HOST", "127.0.0.1"),
            "mysql_port": _env("QUERY_A_MYSQL_PORT", "3307"),
            "http_port": _env("QUERY_A_HTTP_PORT", "8081"),
            "flight_port": _env("QUERY_A_FLIGHT_PORT", "9091"),
            "admin_port": _env("QUERY_A_ADMIN_PORT", "8071"),
            "metric_port": _env("QUERY_A_METRIC_PORT", "7071"),
            "cluster_id": _env("QUERY_A_CLUSTER_ID", "cluster_old"),
            "clickhouse_port": _env("QUERY_A_CLICKHOUSE_PORT", "8124"),
            "flight_sql_port": _env("QUERY_A_FLIGHT_SQL_PORT", "8900"),
        }
        self.query_b = {
            "mysql_host": _env("QUERY_B_MYSQL_HOST", "127.0.0.1"),
            "mysql_port": _env("QUERY_B_MYSQL_PORT", "3308"),
            "http_port": _env("QUERY_B_HTTP_PORT", "8082"),
            "flight_port": _env("QUERY_B_FLIGHT_PORT", "9092"),
            "admin_port": _env("QUERY_B_ADMIN_PORT", "8072"),
            "metric_port": _env("QUERY_B_METRIC_PORT", "7072"),
            "cluster_id": _env("QUERY_B_CLUSTER_ID", "cluster_new"),
            "clickhouse_port": _env("QUERY_B_CLICKHOUSE_PORT", "8125"),
            "flight_sql_port": _env("QUERY_B_FLIGHT_SQL_PORT", "8901"),
        }

        self.tenant_id = _env("TENANT_ID", "upgrade_test")
        self.storage_type = _env("STORAGE_TYPE", "s3")
        self.s3_bucket = os.environ.get("S3_BUCKET", "")
        self.s3_root = _env("S3_ROOT", "upgrade_test_data")
        self.s3_access_key_id = os.environ.get("S3_ACCESS_KEY_ID", "")
        self.s3_secret_access_key = os.environ.get("S3_SECRET_ACCESS_KEY", "")
        self.s3_endpoint_url = _env("S3_ENDPOINT_URL", "")

        self.meta_start_timeout = int(_env("META_START_TIMEOUT", "30"))
        self.query_start_timeout = int(_env("QUERY_START_TIMEOUT", "30"))
        self.mysql_cmd = _env("MYSQL_CMD", "mysql")
        self.skip_experimental_grants = _env("SKIP_EXPERIMENTAL_GRANT_TESTS", "0") == "1"

        # Distributed mode settings
        self.deploy_mode = _env("DEPLOY_MODE", "local")
        self.meta_hosts = [h.strip() for h in _env("META_HOSTS", "").split(",") if h.strip()]
        self.query_hosts = [h.strip() for h in _env("QUERY_HOSTS", "").split(",") if h.strip()]
        self.ssh_user = _env("SSH_USER", "") or None
        self.ssh_key = _env("SSH_KEY", "") or None
        self.remote_work_dir = _env("REMOTE_WORK_DIR", "")

    @property
    def bin_dir(self):
        return os.path.join(self.work_dir, "bin")

    @property
    def data_dir(self):
        return os.path.join(self.work_dir, "data")

    @property
    def log_dir(self):
        return os.path.join(self.work_dir, "logs")

    @property
    def conf_dir(self):
        return os.path.join(self.work_dir, "conf")

    @property
    def port_old(self):
        return self.query_a["mysql_port"]

    @property
    def port_new(self):
        return self.query_b["mysql_port"]

    def meta_endpoints(self):
        eps = []
        for i in range(1, self.meta_cluster_size + 1):
            if self.deploy_mode == "distributed":
                host_ip = meta_host_objs[i - 1].ip
                eps.append(f'"{host_ip}:{self.meta_grpc_port_base}"')
            else:
                eps.append(f'"{self.meta_listen_host}:{self.meta_grpc_port_base + i - 1}"')
        return ", ".join(eps)

    @property
    def query_old_endpoint(self):
        """Return (host, port) for connecting to an OLD query node."""
        if self.deploy_mode == "distributed":
            return (self.query_hosts[0], self.query_a["mysql_port"])
        return ("127.0.0.1", self.query_a["mysql_port"])

    @property
    def query_new_endpoint(self):
        """Return (host, port) for connecting to a NEW query node."""
        if self.deploy_mode == "distributed":
            return (self.query_hosts[0], self.query_b["mysql_port"])
        return ("127.0.0.1", self.query_b["mysql_port"])


cfg = Config()

# Host objects — initialized in main() based on deploy_mode
meta_host_objs = []
query_host_objs = []


def _validate_s3_env():
    missing = [v for v in ("S3_BUCKET", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY")
               if not os.environ.get(v)]
    if missing:
        print(f"ERROR: Required environment variables not set: {', '.join(missing)}", file=sys.stderr)
        print("Set them in your environment or in a .env file. See .env.example.", file=sys.stderr)
        sys.exit(1)


# ============================================================================
# Logging
# ============================================================================

RED = "\033[0;31m"
GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
BLUE = "\033[0;34m"
CYAN = "\033[0;36m"
BOLD = "\033[1m"
NC = "\033[0m"

_pass = _fail = _known = _skip = 0
_results = []       # [{status, desc, detail, suite, yaml_file}]
_current_suite = ""
_current_yaml_file = ""
_start_time = None


def log_info(msg):
    print(f"{BLUE}[INFO]{NC} {msg}", flush=True)


def log_warn(msg):
    print(f"{YELLOW}[WARN]{NC} {msg}", flush=True)


def log_error(msg):
    print(f"{RED}[ERROR]{NC} {msg}", flush=True)


def log_pass(msg):
    global _pass
    print(f"{GREEN}[PASS]{NC} {msg}", flush=True)
    _pass += 1
    _results.append({"status": "PASS", "desc": msg, "detail": "", "suite": _current_suite, "yaml_file": _current_yaml_file})


def log_fail(msg):
    global _fail
    print(f"{RED}[FAIL]{NC} {msg}", flush=True)
    _fail += 1
    detail = ""
    m = re.search(r"\((.+)\)\s*$", msg)
    if m:
        detail = m.group(1)
    _results.append({"status": "FAIL", "desc": msg, "detail": detail, "suite": _current_suite, "yaml_file": _current_yaml_file})


def log_known(msg):
    global _known
    print(f"{YELLOW}[KNOWN]{NC} {msg}", flush=True)
    _known += 1
    _results.append({"status": "KNOWN", "desc": msg, "detail": "", "suite": _current_suite, "yaml_file": _current_yaml_file})


def log_skip(msg):
    global _skip
    print(f"{YELLOW}[SKIP]{NC} {msg}", flush=True)
    _skip += 1
    _results.append({"status": "SKIP", "desc": msg, "detail": "", "suite": _current_suite, "yaml_file": _current_yaml_file})


def log_section(msg):
    print(f"\n{BOLD}{CYAN}======== {msg} ========{NC}\n", flush=True)


def log_summary():
    print()
    log_section("Test Summary")
    print(f"  {GREEN}PASS:  {_pass}{NC}")
    print(f"  {RED}FAIL:  {_fail}{NC}")
    print(f"  {YELLOW}KNOWN: {_known}{NC}  (known incompatibilities)")
    print(f"  {YELLOW}SKIP:  {_skip}{NC}")
    print()
    if _fail > 0:
        log_error("Some tests FAILED.")
        return False
    log_info("All tests passed.")
    return True


def generate_report():
    end_time = datetime.now()
    timestamp = end_time.strftime("%Y%m%d-%H%M%S")
    report_path = os.path.join(cfg.work_dir, f"report-{timestamp}.md")

    # Machine info with graceful fallback
    hostname = platform.node() or "N/A"
    os_info = platform.platform() or "N/A"
    python_ver = platform.python_version()

    cpu_cores = "N/A"
    try:
        with open("/proc/cpuinfo") as f:
            cpu_cores = str(sum(1 for line in f if line.startswith("processor")))
    except Exception:
        pass

    total_mem = "N/A"
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    total_mem = f"{kb / 1024 / 1024:.1f} GB"
                    break
    except Exception:
        pass

    duration = end_time - _start_time if _start_time else None
    duration_str = str(duration).split(".")[0] if duration else "N/A"
    start_str = _start_time.strftime("%Y-%m-%d %H:%M:%S") if _start_time else "N/A"
    end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

    verdict = "PASS" if _fail == 0 else "FAIL"

    # Git commit info — try git command first, fall back to reading .git/ files
    commit_hash = ""
    commit_url = ""
    repo_root = os.path.dirname(SCRIPT_DIR)
    try:
        r = subprocess.run(["git", "-C", repo_root, "rev-parse", "HEAD"],
                           capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            commit_hash = r.stdout.strip()
        r2 = subprocess.run(["git", "-C", repo_root, "remote", "get-url", "origin"],
                            capture_output=True, text=True, timeout=5)
        if r2.returncode == 0:
            remote = r2.stdout.strip()
    except Exception:
        # git not installed — read .git files directly
        remote = ""
        try:
            head_file = os.path.join(repo_root, ".git", "HEAD")
            with open(head_file) as f:
                head = f.read().strip()
            if head.startswith("ref: "):
                ref_path = os.path.join(repo_root, ".git", head[5:])
                with open(ref_path) as f:
                    commit_hash = f.read().strip()
            else:
                commit_hash = head
        except Exception:
            pass
        try:
            config_file = os.path.join(repo_root, ".git", "config")
            with open(config_file) as f:
                in_origin = False
                for line in f:
                    line = line.strip()
                    if line == '[remote "origin"]':
                        in_origin = True
                    elif line.startswith("["):
                        in_origin = False
                    elif in_origin and line.startswith("url = "):
                        remote = line[6:].strip()
                        break
        except Exception:
            pass
    if remote:
        if remote.startswith("git@github.com:"):
            remote = "https://github.com/" + remote[len("git@github.com:"):].removesuffix(".git")
        elif remote.endswith(".git"):
            remote = remote.removesuffix(".git")
        if commit_hash:
            commit_url = f"{remote}/commit/{commit_hash}"

    lines = []
    lines.append(f"# Databend Upgrade Compatibility Test Report")
    lines.append(f"")
    lines.append(f"Generated: {end_str}")
    lines.append(f"")
    lines.append(f"## Test Environment")
    lines.append(f"")
    lines.append(f"| Item | Value |")
    lines.append(f"|------|-------|")
    lines.append(f"| Hostname | {hostname} |")
    lines.append(f"| OS / Kernel | {os_info} |")
    lines.append(f"| CPU Cores | {cpu_cores} |")
    lines.append(f"| Memory | {total_mem} |")
    lines.append(f"| Python | {python_ver} |")
    lines.append(f"")
    lines.append(f"## Test Configuration")
    lines.append(f"")
    lines.append(f"| Item | Value |")
    lines.append(f"|------|-------|")
    lines.append(f"| Meta Version Chain | v{cfg.initial_meta} → v{cfg.intermediate_meta} → v{cfg.target_meta} |")
    lines.append(f"| OLD Query | v{cfg.old_query} (port {cfg.port_old}) |")
    lines.append(f"| NEW Query | v{cfg.new_query} (port {cfg.port_new}) |")
    lines.append(f"| Meta Cluster Size | {cfg.meta_cluster_size} |")
    lines.append(f"| Deploy Mode | {cfg.deploy_mode} |")
    lines.append(f"| Storage | {cfg.storage_type} / {cfg.s3_bucket or 'N/A'} |")
    if cfg.deploy_mode == "distributed":
        lines.append(f"| Meta Hosts | {', '.join(h.ip for h in meta_host_objs)} |")
        lines.append(f"| Query Hosts | {', '.join(h.ip for h in query_host_objs)} |")
    if commit_hash:
        if commit_url:
            lines.append(f"| Test Script | [{commit_hash[:8]}]({commit_url}) |")
        else:
            lines.append(f"| Test Script | {commit_hash[:8]} |")
    lines.append(f"")
    lines.append(f"## Execution Summary")
    lines.append(f"")
    lines.append(f"| Item | Value |")
    lines.append(f"|------|-------|")
    lines.append(f"| Start Time | {start_str} |")
    lines.append(f"| End Time | {end_str} |")
    lines.append(f"| Duration | {duration_str} |")
    lines.append(f"| PASS | {_pass} |")
    lines.append(f"| FAIL | {_fail} |")
    lines.append(f"| KNOWN | {_known} |")
    lines.append(f"| SKIP | {_skip} |")
    lines.append(f"| **Verdict** | **{verdict}** |")
    lines.append(f"")

    # Detailed results grouped by suite
    lines.append(f"## Detailed Results")
    lines.append(f"")

    # Group by suite preserving order, track yaml file per suite
    suites = []
    suite_yaml = {}
    seen = set()
    for r in _results:
        s = r["suite"] or "(ungrouped)"
        if s not in seen:
            suites.append(s)
            seen.add(s)
            yf = r.get("yaml_file", "")
            if yf:
                rel = os.path.relpath(yf, repo_root)
                suite_yaml[s] = rel

    # Build blob base URL for linking to yaml files
    blob_base = ""
    if commit_hash and commit_url:
        # commit_url is .../commit/<hash>, change to .../blob/<hash>
        blob_base = commit_url.replace("/commit/", "/blob/")

    idx = 0
    for suite in suites:
        yaml_link = ""
        if blob_base and suite in suite_yaml:
            yaml_url = f"{blob_base}/{suite_yaml[suite]}"
            yaml_link = f" ([source]({yaml_url}))"
        lines.append(f"### {suite}{yaml_link}")
        lines.append(f"")
        lines.append(f"| # | Status | Description | Detail |")
        lines.append(f"|---|--------|-------------|--------|")
        for r in _results:
            s = r["suite"] or "(ungrouped)"
            if s != suite:
                continue
            idx += 1
            desc = r["desc"].replace("|", "\\|")
            detail = r["detail"].replace("|", "\\|")
            lines.append(f"| {idx} | {r['status']} | {desc} | {detail} |")
        lines.append(f"")

    # Known incompatibilities section
    known_items = [r for r in _results if r["status"] == "KNOWN"]
    if known_items:
        lines.append(f"## Known Incompatibilities")
        lines.append(f"")
        for r in known_items:
            lines.append(f"- {r['desc']}")
        lines.append(f"")

    with open(report_path, "w") as f:
        f.write("\n".join(lines))

    return report_path



# ============================================================================
# SQL Helpers
# ============================================================================

def run_sql(port, sql, user="root", password="", host=None):
    if host is None:
        host = "127.0.0.1"
    args = [cfg.mysql_cmd, f"-h{host}", f"-P{port}", f"-u{user}", "--batch", "-N"]
    if password:
        args.append(f"-p{password}")
    args.extend(["-e", sql])
    r = subprocess.run(args, capture_output=True, text=True)
    out = r.stdout + r.stderr
    lines = [l for l in out.splitlines()
             if not l.startswith("[Warning]") and not l.startswith("mysql: [Warning]")]
    return r.returncode, "\n".join(lines).strip()


def _is_error(rc, out):
    return rc != 0 or any(l.lstrip().upper().startswith("ERROR") for l in out.splitlines())


def expect_ok(port, sql, desc, user="root", password="", host=None):
    rc, out = run_sql(port, sql, user, password, host)
    if not _is_error(rc, out):
        log_pass(desc)
    else:
        log_fail(f"{desc}  (rc={rc}, output: {out})")


def expect_fail(port, sql, desc, user="root", password="", host=None):
    rc, out = run_sql(port, sql, user, password, host)
    if _is_error(rc, out):
        log_pass(f"{desc} (expected failure)")
    else:
        log_fail(f"{desc}  (expected failure but succeeded: {out})")


def expect_eq(port, sql, expected, desc, user="root", password="", host=None):
    rc, out = run_sql(port, sql, user, password, host)
    trimmed = out.strip()
    if rc == 0 and trimmed == expected:
        log_pass(desc)
    else:
        log_fail(f"{desc}  (expected: '{expected}', got: '{trimmed}', rc={rc})")


def expect_contains(port, sql, pattern, desc, user="root", password="", host=None):
    rc, out = run_sql(port, sql, user, password, host)
    if rc == 0 and re.search(pattern, out, re.IGNORECASE):
        log_pass(desc)
    else:
        log_fail(f"{desc}  (pattern '{pattern}' not found in: {out})")


def expect_known_fail(port, sql, desc, user="root", password="", host=None):
    rc, out = run_sql(port, sql, user, password, host)
    if _is_error(rc, out):
        log_known(f"{desc} (confirmed incompatible)")
    else:
        log_pass(f"{desc} (unexpectedly works!)")


def expect_fail_contains(port, sql, pattern, desc, user="root", password="", host=None):
    rc, out = run_sql(port, sql, user, password, host)
    if _is_error(rc, out) and re.search(pattern, out, re.IGNORECASE):
        log_pass(f"{desc} (expected failure with '{pattern}')")
    elif _is_error(rc, out):
        log_fail(f"{desc}  (failed but pattern '{pattern}' not found in: {out})")
    else:
        log_fail(f"{desc}  (expected failure but succeeded: {out})")


def wait_for_query(port, user="root", password="", timeout=None, host=None):
    if timeout is None:
        timeout = cfg.query_start_timeout
    for _ in range(timeout):
        rc, _ = run_sql(port, "SELECT 1", user, password, host)
        if rc == 0:
            return True
        time.sleep(1)
    return False



# ============================================================================
# Download
# ============================================================================

def _download_and_extract(url, dest_dir, component_name, binary_prefix):
    """Download tarball, extract, flatten bin/, normalize binary names."""
    tarball = dest_dir + ".tar.gz"
    log_info(f"Downloading {component_name} ...")
    rc = subprocess.call(["curl", "-fSL", "--retry", "3", "-o", tarball, url],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if rc != 0:
        log_error(f"Failed to download {component_name}")
        return False
    os.makedirs(dest_dir, exist_ok=True)
    rc = subprocess.call(["tar", "-xzf", tarball, "-C", dest_dir])
    if rc != 0:
        log_error(f"Failed to extract {component_name}")
        return False
    # Flatten nested bin/ directory
    nested_bin = os.path.join(dest_dir, "bin")
    if os.path.isdir(nested_bin):
        for f in os.listdir(nested_bin):
            os.rename(os.path.join(nested_bin, f), os.path.join(dest_dir, f))
        os.rmdir(nested_bin)
    # Normalize binary names via symlink
    for canonical in [f"databend-{binary_prefix}", f"databend-{binary_prefix}ctl"]:
        target = os.path.join(dest_dir, canonical)
        if not os.path.exists(target):
            for f in os.listdir(dest_dir):
                if f.startswith(canonical) and not f.endswith(".sh"):
                    os.symlink(f, target)
                    break
    # chmod +x
    for f in os.listdir(dest_dir):
        if f.startswith(f"databend-{binary_prefix}"):
            os.chmod(os.path.join(dest_dir, f), 0o755)
    os.remove(tarball)
    log_info(f"{component_name} ready at {dest_dir}")
    return True


def download_all():
    log_section("Downloading Artifacts")
    os.makedirs(cfg.bin_dir, exist_ok=True)
    for ver, url in META_URLS.items():
        d = os.path.join(cfg.bin_dir, f"meta-{ver}")
        if os.path.isfile(os.path.join(d, "databend-meta")) and os.access(os.path.join(d, "databend-meta"), os.X_OK):
            log_info(f"Meta {ver} already downloaded, skipping.")
            continue
        if not _download_and_extract(url, d, f"meta {ver}", "meta"):
            return False
    for ver, url in QUERY_URLS.items():
        d = os.path.join(cfg.bin_dir, f"query-{ver}")
        if os.path.isfile(os.path.join(d, "databend-query")) and os.access(os.path.join(d, "databend-query"), os.X_OK):
            log_info(f"Query {ver} already downloaded, skipping.")
            continue
        if not _download_and_extract(url, d, f"query {ver}", "query"):
            return False
    log_info("All artifacts downloaded.")
    if cfg.deploy_mode == "distributed":
        log_section("Distributing Binaries to Remote Hosts")
        remote_bin = os.path.join(_remote_work_dir(), "bin")
        if not distribute_binaries(cfg.bin_dir, meta_host_objs, remote_bin):
            return False
        if not distribute_binaries(cfg.bin_dir, query_host_objs, remote_bin):
            return False
        log_info("Binary distribution complete.")
    return True



# ============================================================================
# Meta Cluster Management
# ============================================================================

def _meta_bin(ver):
    return os.path.join(cfg.bin_dir, f"meta-{ver}", "databend-meta")


def _remote_work_dir():
    """Return the work directory path for remote hosts."""
    if cfg.remote_work_dir:
        return cfg.remote_work_dir
    if cfg.deploy_mode == "distributed":
        return "/tmp/databend-upgrade-test/work"
    return cfg.work_dir


def _pid_alive(pid_file):
    if not os.path.isfile(pid_file):
        return False, 0
    pid = int(open(pid_file).read().strip())
    try:
        os.kill(pid, 0)
        return True, pid
    except OSError:
        return False, pid


def _gen_meta_config(node_id, meta_ver):
    if cfg.deploy_mode == "distributed":
        grpc_port = cfg.meta_grpc_port_base
        raft_port = cfg.meta_raft_port_base
        admin_port = cfg.meta_admin_port_base
        host_obj = meta_host_objs[node_id - 1]
        advertise_host = host_obj.ip
        listen_host = "0.0.0.0"
    else:
        grpc_port = cfg.meta_grpc_port_base + node_id - 1
        raft_port = cfg.meta_raft_port_base + node_id - 1
        admin_port = cfg.meta_admin_port_base + node_id - 1
        advertise_host = cfg.meta_listen_host
        listen_host = cfg.meta_listen_host

    work_dir = _remote_work_dir()
    raft_dir = os.path.join(work_dir, "data", f"meta-{node_id}")
    log_dir = os.path.join(work_dir, "logs", f"meta-{node_id}")
    conf_dir = os.path.join(work_dir, "conf")

    if cfg.deploy_mode == "distributed":
        host_obj.makedirs(raft_dir)
        host_obj.makedirs(log_dir)
        host_obj.makedirs(conf_dir)
    else:
        os.makedirs(raft_dir, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)
        os.makedirs(cfg.conf_dir, exist_ok=True)

    grpc_listen = "0.0.0.0" if cfg.deploy_mode == "distributed" else advertise_host
    content = (
        f'admin_api_address = "0.0.0.0:{admin_port}"\n'
        f'grpc_api_address = "{grpc_listen}:{grpc_port}"\n'
        f'grpc_api_advertise_host = "{advertise_host}"\n\n'
        f"[raft_config]\nid = {node_id}\n"
        f'raft_dir = "{raft_dir}"\n'
        f"raft_api_port = {raft_port}\n"
        f'raft_listen_host = "{listen_host}"\n'
        f'raft_advertise_host = "{advertise_host}"\n\n'
        f'[log]\n[log.file]\ndir = "{log_dir}"\nlevel = "INFO"\n'
    )

    if cfg.deploy_mode == "distributed":
        conf_path = os.path.join(conf_dir, f"meta-{node_id}.toml")
        host_obj.write_file(conf_path, content)
    else:
        conf_path = os.path.join(cfg.conf_dir, f"meta-{node_id}.toml")
        with open(conf_path, "w") as f:
            f.write(content)
    return conf_path


def start_meta_node(node_id, meta_ver, extra_args=None):
    if extra_args is None:
        extra_args = []
    conf = _gen_meta_config(node_id, meta_ver)

    if cfg.deploy_mode == "distributed":
        host_obj = meta_host_objs[node_id - 1]
        work_dir = _remote_work_dir()
        pid_file = os.path.join(work_dir, "data", f"meta-{node_id}.pid")
        log_file = os.path.join(work_dir, "logs", f"meta-{node_id}", "startup.log")
        bin_dir = os.path.join(work_dir, "bin")
        meta_bin = os.path.join(bin_dir, f"meta-{meta_ver}", "databend-meta")

        alive, pid = host_obj.is_alive(pid_file)
        if alive:
            log_info(f"Meta node {node_id} already running on {host_obj.ip} (pid {pid})")
            return True

        log_info(f"Starting meta node {node_id} (v{meta_ver}) on {host_obj.ip} {' '.join(extra_args)}")
        cmd = f"{meta_bin} --config-file {conf} {' '.join(extra_args)}"
        host_obj.start_daemon(cmd, log_file, pid_file)

        admin_port = cfg.meta_admin_port_base
        url = f"http://{host_obj.ip}:{admin_port}/v1/health"
        for _ in range(cfg.meta_start_timeout):
            try:
                r = subprocess.run(["curl", "-sf", url], capture_output=True, timeout=2)
                if r.returncode == 0:
                    log_info(f"Meta node {node_id} is ready on {host_obj.ip}")
                    return True
            except Exception:
                pass
            time.sleep(1)
        log_error(f"Meta node {node_id} failed to start on {host_obj.ip} within {cfg.meta_start_timeout}s")
        return False
    else:
        pid_file = os.path.join(cfg.data_dir, f"meta-{node_id}.pid")
        log_file = os.path.join(cfg.log_dir, f"meta-{node_id}", "startup.log")

        alive, pid = _pid_alive(pid_file)
        if alive:
            log_info(f"Meta node {node_id} already running (pid {pid})")
            return True

        log_info(f"Starting meta node {node_id} (v{meta_ver}) {' '.join(extra_args)}")
        with open(log_file, "w") as lf:
            proc = subprocess.Popen(
                [_meta_bin(meta_ver), "--config-file", conf] + extra_args,
                stdout=lf, stderr=lf)
        with open(pid_file, "w") as f:
            f.write(str(proc.pid))

        admin_port = cfg.meta_admin_port_base + node_id - 1
        url = f"http://{cfg.meta_listen_host}:{admin_port}/v1/health"
        for _ in range(cfg.meta_start_timeout):
            try:
                r = subprocess.run(["curl", "-sf", url], capture_output=True, timeout=2)
                if r.returncode == 0:
                    log_info(f"Meta node {node_id} is ready (pid {proc.pid})")
                    return True
            except Exception:
                pass
            time.sleep(1)
        log_error(f"Meta node {node_id} failed to start within {cfg.meta_start_timeout}s")
        return False



def _stop_process(pid_file, label):
    alive, pid = _pid_alive(pid_file)
    if not alive:
        if os.path.isfile(pid_file):
            os.remove(pid_file)
        return
    log_info(f"Stopping {label} (pid {pid})")
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass
    for _ in range(10):
        try:
            os.kill(pid, 0)
        except OSError:
            break
        time.sleep(1)
    else:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
    if os.path.isfile(pid_file):
        os.remove(pid_file)


def stop_meta_node(node_id):
    if cfg.deploy_mode == "distributed":
        host_obj = meta_host_objs[node_id - 1]
        work_dir = _remote_work_dir()
        pid_file = os.path.join(work_dir, "data", f"meta-{node_id}.pid")
        host_obj.stop_daemon(pid_file, f"meta node {node_id}")
    else:
        _stop_process(os.path.join(cfg.data_dir, f"meta-{node_id}.pid"),
                      f"meta node {node_id}")


def start_meta_cluster(meta_ver):
    log_section(f"Starting Meta Cluster (v{meta_ver}, {cfg.meta_cluster_size} nodes)")
    if cfg.deploy_mode == "distributed":
        if not start_meta_node(1, meta_ver, ["--single"]):
            return False
        join_addr = f"{meta_host_objs[0].ip}:{cfg.meta_grpc_port_base}"
        for i in range(2, cfg.meta_cluster_size + 1):
            if not start_meta_node(i, meta_ver, ["--join", join_addr]):
                return False
    else:
        if not start_meta_node(1, meta_ver, ["--single"]):
            return False
        join_addr = f"{cfg.meta_listen_host}:{cfg.meta_grpc_port_base}"
        for i in range(2, cfg.meta_cluster_size + 1):
            if not start_meta_node(i, meta_ver, ["--join", join_addr]):
                return False
    log_info(f"Meta cluster (v{meta_ver}) is up with {cfg.meta_cluster_size} nodes.")
    return True


def stop_meta_cluster():
    log_info("Stopping meta cluster...")
    for i in range(1, cfg.meta_cluster_size + 1):
        stop_meta_node(i)


def rolling_upgrade_meta(target_ver, verify_cb=None):
    log_section(f"Rolling Upgrade Meta → v{target_ver}")
    for i in range(1, cfg.meta_cluster_size + 1):
        log_info(f"--- Upgrading meta node {i} to v{target_ver} ---")
        stop_meta_node(i)
        time.sleep(2)
        # Find a running peer to join
        join_args = []
        for j in range(1, cfg.meta_cluster_size + 1):
            if j != i:
                if cfg.deploy_mode == "distributed":
                    join_args = ["--join", f"{meta_host_objs[j-1].ip}:{cfg.meta_grpc_port_base}"]
                else:
                    join_args = ["--join", f"{cfg.meta_listen_host}:{cfg.meta_grpc_port_base + j - 1}"]
                break
        if not start_meta_node(i, target_ver, join_args):
            return False
        time.sleep(3)
        # Verify cluster health
        healthy = False
        for j in range(1, cfg.meta_cluster_size + 1):
            if cfg.deploy_mode == "distributed":
                ap = cfg.meta_admin_port_base
                h = meta_host_objs[j-1].ip
            else:
                ap = cfg.meta_admin_port_base + j - 1
                h = cfg.meta_listen_host
            url = f"http://{h}:{ap}/v1/cluster/status"
            r = subprocess.run(["curl", "-sf", url], capture_output=True, text=True)
            if r.returncode == 0:
                log_info(f"Cluster status after upgrading node {i}: {r.stdout[:200]}")
                healthy = True
                break
        if not healthy:
            log_error(f"No healthy meta node found after upgrading node {i}")
            return False
        if verify_cb and not verify_cb():
            log_error(f"Verification failed after upgrading meta node {i}")
            return False
        log_info(f"Meta node {i} upgraded to v{target_ver} successfully.")
    log_info(f"Meta cluster fully upgraded to v{target_ver}.")
    return True



# ============================================================================
# Query Node Management
# ============================================================================

def _query_bin(ver):
    return os.path.join(cfg.bin_dir, f"query-{ver}", "databend-query")


def _gen_query_config(label, version, host_idx=None):
    if cfg.deploy_mode == "distributed" and host_idx is not None:
        host_obj = query_host_objs[host_idx]
        is_old = label.startswith("old")
        q = cfg.query_a if is_old else cfg.query_b
        work_dir = _remote_work_dir()
        data_path = os.path.join(work_dir, "data", "shared-storage")
        log_path = os.path.join(work_dir, "logs", f"query-{label}")
        cache_path = os.path.join(work_dir, "data", f"query-{label}-cache")
        conf_dir = os.path.join(work_dir, "conf")
        for d in [log_path, cache_path, conf_dir]:
            host_obj.makedirs(d)
        endpoints = cfg.meta_endpoints()
        flight_host = host_obj.ip
        bind_host = "0.0.0.0"
        s3_endpoint_line = ""
        if cfg.s3_endpoint_url:
            s3_endpoint_line = f'endpoint_url = "{cfg.s3_endpoint_url}"\n'
        content = f"""[query]
tenant_id = "{cfg.tenant_id}"
cluster_id = "{q['cluster_id']}"
mysql_handler_host = "{bind_host}"
mysql_handler_port = {q['mysql_port']}
http_handler_host = "{bind_host}"
http_handler_port = {q['http_port']}
flight_api_address = "{flight_host}:{q['flight_port']}"
admin_api_address = "{bind_host}:{q['admin_port']}"
metric_api_address = "{bind_host}:{q['metric_port']}"
clickhouse_http_handler_host = "{bind_host}"
clickhouse_http_handler_port = {q['clickhouse_port']}
flight_sql_handler_host = "{bind_host}"
flight_sql_handler_port = {q['flight_sql_port']}

[[query.users]]
name = "root"
auth_type = "no_password"

[log]
[log.file]
dir = "{log_path}"
level = "INFO"

[meta]
endpoints = [{endpoints}]

[storage]
type = "{cfg.storage_type}"

[storage.s3]
bucket = "{cfg.s3_bucket}"
root = "{cfg.s3_root}"
access_key_id = "{cfg.s3_access_key_id}"
secret_access_key = "{cfg.s3_secret_access_key}"
{s3_endpoint_line}
[storage.fs]
data_path = "{data_path}/data"

[cache]
data_cache_storage = "none"
"""
        conf_path = os.path.join(conf_dir, f"query-{label}.toml")
        host_obj.write_file(conf_path, content)
        return conf_path

    # Local mode — original logic
    q = cfg.query_a if label == "a" else cfg.query_b
    data_path = os.path.join(cfg.data_dir, "shared-storage")
    log_path = os.path.join(cfg.log_dir, f"query-{label}")
    cache_path = os.path.join(cfg.data_dir, f"query-{label}-cache")
    for d in [data_path, log_path, cache_path, cfg.conf_dir]:
        os.makedirs(d, exist_ok=True)
    endpoints = cfg.meta_endpoints()
    h = q["mysql_host"]
    conf_path = os.path.join(cfg.conf_dir, f"query-{label}.toml")
    s3_endpoint_line = ""
    if cfg.s3_endpoint_url:
        s3_endpoint_line = f'endpoint_url = "{cfg.s3_endpoint_url}"\n'
    with open(conf_path, "w") as f:
        f.write(f"""[query]
tenant_id = "{cfg.tenant_id}"
cluster_id = "{q['cluster_id']}"
mysql_handler_host = "{h}"
mysql_handler_port = {q['mysql_port']}
http_handler_host = "{h}"
http_handler_port = {q['http_port']}
flight_api_address = "{h}:{q['flight_port']}"
admin_api_address = "{h}:{q['admin_port']}"
metric_api_address = "{h}:{q['metric_port']}"
clickhouse_http_handler_host = "{h}"
clickhouse_http_handler_port = {q['clickhouse_port']}
flight_sql_handler_host = "{h}"
flight_sql_handler_port = {q['flight_sql_port']}

[[query.users]]
name = "root"
auth_type = "no_password"

[log]
[log.file]
dir = "{log_path}"
level = "INFO"

[meta]
endpoints = [{endpoints}]

[storage]
type = "{cfg.storage_type}"

[storage.s3]
bucket = "{cfg.s3_bucket}"
root = "{cfg.s3_root}"
access_key_id = "{cfg.s3_access_key_id}"
secret_access_key = "{cfg.s3_secret_access_key}"
{s3_endpoint_line}
[storage.fs]
data_path = "{data_path}/data"

[cache]
data_cache_storage = "none"
""")
    return conf_path


def start_query_node(label, version, host_idx=None):
    if cfg.deploy_mode == "distributed" and host_idx is not None:
        host_obj = query_host_objs[host_idx]
        conf = _gen_query_config(label, version, host_idx)
        work_dir = _remote_work_dir()
        pid_file = os.path.join(work_dir, "data", f"query-{label}.pid")
        log_file = os.path.join(work_dir, "logs", f"query-{label}", "startup.log")
        bin_dir = os.path.join(work_dir, "bin")
        query_bin = os.path.join(bin_dir, f"query-{version}", "databend-query")
        is_old = label.startswith("old")
        q = cfg.query_a if is_old else cfg.query_b
        mysql_port = q["mysql_port"]

        alive, pid = host_obj.is_alive(pid_file)
        if alive:
            log_info(f"Query {label} already running on {host_obj.ip} (pid {pid})")
            return True

        log_info(f"Starting query {label} (v{version}) on {host_obj.ip} mysql port {mysql_port}")
        cmd = f"{query_bin} --config-file {conf}"
        host_obj.start_daemon(cmd, log_file, pid_file)

        if wait_for_query(mysql_port, host=host_obj.ip):
            log_info(f"Query {label} (v{version}) is ready on {host_obj.ip}")
            return True
        log_error(f"Query {label} failed to start on {host_obj.ip} within {cfg.query_start_timeout}s")
        return False

    # Local mode — original logic
    conf = _gen_query_config(label, version)
    pid_file = os.path.join(cfg.data_dir, f"query-{label}.pid")
    log_file = os.path.join(cfg.log_dir, f"query-{label}", "startup.log")
    mysql_port = cfg.query_a["mysql_port"] if label == "a" else cfg.query_b["mysql_port"]

    alive, pid = _pid_alive(pid_file)
    if alive:
        log_info(f"Query {label} already running (pid {pid})")
        return True

    log_info(f"Starting query {label} (v{version}) on mysql port {mysql_port}")
    with open(log_file, "w") as lf:
        proc = subprocess.Popen(
            [_query_bin(version), "--config-file", conf],
            stdout=lf, stderr=lf)
    with open(pid_file, "w") as f:
        f.write(str(proc.pid))

    if wait_for_query(mysql_port):
        log_info(f"Query {label} (v{version}) is ready (pid {proc.pid})")
        return True
    log_error(f"Query {label} failed to start within {cfg.query_start_timeout}s")
    return False


def stop_query_node(label, host_idx=None):
    if cfg.deploy_mode == "distributed" and host_idx is not None:
        host_obj = query_host_objs[host_idx]
        work_dir = _remote_work_dir()
        pid_file = os.path.join(work_dir, "data", f"query-{label}.pid")
        host_obj.stop_daemon(pid_file, f"query {label}")
    else:
        _stop_process(os.path.join(cfg.data_dir, f"query-{label}.pid"),
                      f"query {label}")


def stop_all_query():
    if cfg.deploy_mode == "distributed":
        for i in range(len(query_host_objs)):
            stop_query_node(f"old-{i+1}", i)
            stop_query_node(f"new-{i+1}", i)
    else:
        stop_query_node("a")
        stop_query_node("b")



# ============================================================================
# YAML Test Runner
# ============================================================================

def _expand_seq_values(template, start, end):
    return ", ".join(template.replace("{i}", str(i)) for i in range(start, end + 1))


def _expand_sql_generate(raw):
    m = re.search(r'\{seq_values\(\s*(\d+)\s*,\s*(\d+)\s*,\s*"([^"]+)"\s*\)\}', raw)
    if not m:
        return raw
    start, end, template = int(m.group(1)), int(m.group(2)), m.group(3)
    return raw[: m.start()] + _expand_seq_values(template, start, end) + raw[m.end() :]


EXPECT_DISPATCH = {
    "ok":             lambda p, sql, t: expect_ok(p, sql, t["desc"], t["user"], t["pw"], t.get("host")),
    "eq":             lambda p, sql, t: expect_eq(p, sql, str(t.get("value", "")), t["desc"], t["user"], t["pw"], t.get("host")),
    "fail":           lambda p, sql, t: expect_fail(p, sql, t["desc"], t["user"], t["pw"], t.get("host")),
    "known_fail":     lambda p, sql, t: expect_known_fail(p, sql, t["desc"], t["user"], t["pw"], t.get("host")),
    "contains":       lambda p, sql, t: expect_contains(p, sql, t.get("pattern", ""), t["desc"], t["user"], t["pw"], t.get("host")),
    "fail_contains":  lambda p, sql, t: expect_fail_contains(p, sql, t.get("pattern", ""), t["desc"], t["user"], t["pw"], t.get("host")),
}

def run_yaml_test(yaml_file):
    global _current_suite, _current_yaml_file
    with open(yaml_file) as f:
        doc = yaml.safe_load(f)

    name = doc.get("name", "")
    _current_suite = name or os.path.basename(yaml_file)
    _current_yaml_file = yaml_file
    db = doc.get("database", "")
    cleanup_port = doc.get("cleanup_port", "OLD")
    skip_env = doc.get("skip_env", "")

    if skip_env and os.environ.get(skip_env, "0") == "1":
        log_skip(f"{name} skipped ({skip_env}=1)")
        return
    if cfg.skip_experimental_grants and skip_env == "SKIP_EXPERIMENTAL_GRANT_TESTS":
        log_skip(f"{name} skipped (--skip-experimental-grants)")
        return

    old_host, old_port = cfg.query_old_endpoint
    new_host, new_port = cfg.query_new_endpoint

    if name:
        log_section(name)
    if db:
        expect_ok(old_port, f"CREATE DATABASE IF NOT EXISTS {db}", f"Create {db} db", host=old_host)

    for t in doc.get("tests", []):
        sql = t.get("sql") or _expand_sql_generate(t.get("sql_generate", ""))
        sql = " ".join(sql.splitlines())

        if t.get("port", "OLD") == "NEW":
            p, h = new_port, new_host
        else:
            p, h = old_port, old_host
        sleep_before = int(t.get("sleep_before", 0))
        if sleep_before > 0:
            time.sleep(sleep_before)

        ctx = {
            "desc": t.get("desc", ""),
            "user": t.get("user") or "root",
            "pw": t.get("password") or "",
            "value": t.get("value", ""),
            "pattern": t.get("pattern", ""),
            "host": h,
        }
        expect = t.get("expect", "ok")
        handler = EXPECT_DISPATCH.get(expect)
        if handler:
            handler(p, sql, ctx)
        else:
            log_error(f"Unknown expect type '{expect}' in {yaml_file} ({ctx['desc']})")

    if db:
        if cleanup_port == "NEW":
            cp, ch = new_port, new_host
        else:
            cp, ch = old_port, old_host
        expect_ok(cp, f"DROP DATABASE {db}", f"Cleanup {db}", host=ch)


# ============================================================================
# Phases
# ============================================================================

def phase_download(skip):
    if skip:
        log_info("Skipping download (--skip-download)")
        if cfg.deploy_mode == "distributed":
            log_section("Distributing Binaries to Remote Hosts")
            remote_bin = os.path.join(_remote_work_dir(), "bin")
            if not distribute_binaries(cfg.bin_dir, meta_host_objs, remote_bin):
                return False
            if not distribute_binaries(cfg.bin_dir, query_host_objs, remote_bin):
                return False
            log_info("Binary distribution complete.")
        return True
    return download_all()


def phase_meta_setup():
    log_section("Phase: Meta Cluster Setup")
    return start_meta_cluster(cfg.initial_meta)


def _verify_query_during_meta_upgrade():
    old_host, old_port = cfg.query_old_endpoint
    if not wait_for_query(old_port, timeout=10, host=old_host):
        log_error("Query A not reachable during meta upgrade verification")
        return False
    rc, out = run_sql(old_port, "SELECT 'meta_upgrade_ok'", host=old_host)
    if rc == 0 and out.strip() == "meta_upgrade_ok":
        log_pass("Query A works during meta rolling upgrade")
        return True
    log_fail(f"Query A broken during meta rolling upgrade: {out}")
    return False


def phase_meta_upgrade():
    log_section("Phase: Meta Rolling Upgrade")
    log_info(f"Upgrade path: v{cfg.initial_meta} → v{cfg.intermediate_meta} → v{cfg.target_meta}")

    old_host, old_port = cfg.query_old_endpoint

    log_info(f"Starting query A (v{cfg.old_query}) for upgrade verification...")
    if cfg.deploy_mode == "distributed":
        if not start_query_node("old-1", cfg.old_query, 0):
            return False
    else:
        if not start_query_node("a", cfg.old_query):
            return False
    expect_ok(old_port, "SELECT 'pre_upgrade_ok'", "Query A works before meta upgrade", host=old_host)

    expect_ok(old_port, "CREATE DATABASE IF NOT EXISTS upgrade_check", "Create upgrade_check db", host=old_host)
    expect_ok(old_port,
              "CREATE TABLE IF NOT EXISTS upgrade_check.survive (id INT, val VARCHAR)",
              "Create survival table", host=old_host)
    expect_ok(old_port,
              "INSERT INTO upgrade_check.survive VALUES (1,'before_upgrade')",
              "Insert pre-upgrade data", host=old_host)

    # Rolling upgrade: initial → intermediate
    if not rolling_upgrade_meta(cfg.intermediate_meta, _verify_query_during_meta_upgrade):
        return False
    expect_eq(old_port, "SELECT val FROM upgrade_check.survive WHERE id=1",
              "before_upgrade", "Data survived intermediate upgrade", host=old_host)

    # Rolling upgrade: intermediate → target
    if not rolling_upgrade_meta(cfg.target_meta, _verify_query_during_meta_upgrade):
        return False
    expect_eq(old_port, "SELECT val FROM upgrade_check.survive WHERE id=1",
              "before_upgrade", "Data survived final upgrade", host=old_host)

    log_info(f"Meta upgrade complete. Cluster is at v{cfg.target_meta}.")
    return True


def phase_query_setup():
    log_section("Phase: Query Clusters Setup")
    old_host, old_port = cfg.query_old_endpoint
    new_host, new_port = cfg.query_new_endpoint

    if cfg.deploy_mode == "distributed":
        for i in range(len(query_host_objs)):
            if not start_query_node(f"old-{i+1}", cfg.old_query, i):
                return False
            if not start_query_node(f"new-{i+1}", cfg.new_query, i):
                return False
    else:
        if not start_query_node("a", cfg.old_query):
            return False
        if not start_query_node("b", cfg.new_query):
            return False

    expect_ok(old_port, "SELECT 'old_ok'", f"OLD (v{cfg.old_query}) connected", host=old_host)
    expect_ok(new_port, "SELECT 'new_ok'", f"NEW (v{cfg.new_query}) connected", host=new_host)

    expect_eq(old_port, "SELECT val FROM upgrade_check.survive WHERE id=1",
              "before_upgrade", "OLD sees pre-upgrade data", host=old_host)
    expect_eq(new_port, "SELECT val FROM upgrade_check.survive WHERE id=1",
              "before_upgrade", "NEW sees pre-upgrade data", host=new_host)

    expect_ok(old_port, "DROP DATABASE IF EXISTS upgrade_check", "Cleanup upgrade_check", host=old_host)
    return True


def phase_compat_test():
    log_section("Phase: Cross-Version Compatibility Tests")
    old_host, old_port = cfg.query_old_endpoint
    new_host, new_port = cfg.query_new_endpoint
    log_info(f"OLD = v{cfg.old_query} ({old_host}:{old_port})")
    log_info(f"NEW = v{cfg.new_query} ({new_host}:{new_port})")

    tests_dir = os.path.join(SCRIPT_DIR, "tests")
    for name in [
        "test_basic_crud",
        "test_ddl",
        "test_data_types",
        "test_cross_version_rw",
        "test_complex_queries",
        "test_permissions_safe",
        "test_permissions_new_grant_objects",
        "test_snapshot_statistics",
        "test_replace_into",
        "test_time_travel",
        "test_flashback",
    ]:
        run_yaml_test(os.path.join(tests_dir, f"{name}.yaml"))


def cleanup_processes():
    log_info("Cleaning up processes...")
    try:
        stop_all_query()
    except Exception:
        pass
    try:
        stop_meta_cluster()
    except Exception:
        pass


# ============================================================================
# Main
# ============================================================================

def main():
    global _start_time, meta_host_objs, query_host_objs
    _start_time = datetime.now()

    parser = argparse.ArgumentParser(
        description="Databend Upgrade Compatibility Test",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("phases", nargs="*", default=["all"],
                        help="Phases to run: download, meta-setup, meta-upgrade, query-setup, compat-test, all")
    parser.add_argument("--initial-meta", dest="initial_meta")
    parser.add_argument("--work-dir", dest="work_dir")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-experimental-grants", dest="skip_experimental_grants", action="store_true")
    parser.add_argument("--mode", choices=["local", "distributed"],
                        help="Deploy mode: local (default) or distributed")
    parser.add_argument("--meta-hosts", dest="meta_hosts",
                        help="Comma-separated meta host IPs (distributed mode)")
    parser.add_argument("--query-hosts", dest="query_hosts",
                        help="Comma-separated query host IPs (distributed mode)")
    args = parser.parse_args()

    _validate_s3_env()

    if args.initial_meta:
        cfg.initial_meta = args.initial_meta
    if args.work_dir:
        cfg.work_dir = args.work_dir
    if args.skip_experimental_grants:
        cfg.skip_experimental_grants = True
    if args.mode:
        cfg.deploy_mode = args.mode
    if args.meta_hosts:
        cfg.meta_hosts = [h.strip() for h in args.meta_hosts.split(",") if h.strip()]
    if args.query_hosts:
        cfg.query_hosts = [h.strip() for h in args.query_hosts.split(",") if h.strip()]

    # Initialize host objects
    if cfg.deploy_mode == "distributed":
        assert len(cfg.meta_hosts) >= cfg.meta_cluster_size, \
            f"Need at least {cfg.meta_cluster_size} meta hosts, got {len(cfg.meta_hosts)}"
        assert len(cfg.query_hosts) >= 1, \
            "Need at least 1 query host for distributed mode"
        meta_host_objs = [RemoteHost(ip, cfg.ssh_user, cfg.ssh_key) for ip in cfg.meta_hosts[:cfg.meta_cluster_size]]
        query_host_objs = [RemoteHost(ip, cfg.ssh_user, cfg.ssh_key) for ip in cfg.query_hosts]
        if not check_ssh_connectivity(meta_host_objs + query_host_objs):
            log_error("SSH connectivity check failed")
            sys.exit(1)
    else:
        localhost = LocalHost()
        meta_host_objs = [localhost] * cfg.meta_cluster_size
        query_host_objs = [localhost]

    phases = set(args.phases)

    def should_run(phase):
        return "all" in phases or phase in phases

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))
    import atexit
    atexit.register(cleanup_processes)

    log_section("Databend Upgrade Compatibility Test")
    log_info(f"Deploy mode:        {cfg.deploy_mode}")
    log_info(f"Initial meta:       v{cfg.initial_meta}")
    log_info(f"Intermediate meta:  v{cfg.intermediate_meta}")
    log_info(f"Target meta:        v{cfg.target_meta}")
    log_info(f"Old query:          v{cfg.old_query}")
    log_info(f"New query:          v{cfg.new_query}")
    log_info(f"Work dir:           {cfg.work_dir}")
    if cfg.deploy_mode == "distributed":
        log_info(f"Meta hosts:         {', '.join(h.ip for h in meta_host_objs)}")
        log_info(f"Query hosts:        {', '.join(h.ip for h in query_host_objs)}")
        log_info(f"Remote work dir:    {_remote_work_dir()}")
    print()

    if args.cleanup:
        log_warn(f"Cleaning up work directory (preserving binaries): {cfg.work_dir}")
        cleanup_processes()
        import shutil
        for d in [cfg.data_dir, cfg.log_dir, cfg.conf_dir]:
            if os.path.isdir(d):
                shutil.rmtree(d)
        if cfg.deploy_mode == "distributed":
            rwd = _remote_work_dir()
            for host_obj in meta_host_objs + query_host_objs:
                for sub in ["data", "logs", "conf"]:
                    host_obj.run_cmd(f"rm -rf {os.path.join(rwd, sub)}", timeout=10)
                log_info(f"Cleaned remote work dir on {host_obj.ip}")

    for d in [cfg.work_dir, cfg.bin_dir, cfg.data_dir, cfg.log_dir, cfg.conf_dir]:
        os.makedirs(d, exist_ok=True)

    if should_run("download"):
        if not phase_download(args.skip_download):
            log_error("Download phase failed")
            sys.exit(1)
    if should_run("meta-setup"):
        if not phase_meta_setup():
            log_error("Meta setup phase failed")
            sys.exit(1)
    if should_run("meta-upgrade"):
        if not phase_meta_upgrade():
            log_error("Meta upgrade phase failed")
            sys.exit(1)
    if should_run("query-setup"):
        if not phase_query_setup():
            log_error("Query setup phase failed")
            sys.exit(1)
    if should_run("compat-test"):
        phase_compat_test()

    ok = log_summary()
    report_path = generate_report()
    log_info(f"Report saved to: {report_path}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
