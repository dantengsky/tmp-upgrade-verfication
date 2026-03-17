#!/usr/bin/env python3
"""Databend Upgrade Test - Main Orchestrator

Usage: ./run.py [options] [phase...]

Phases: download, meta-setup, meta-upgrade, query-setup, compat-test, all
Options:
  --initial-meta VER    Initial meta version (default: 1.2.307)
  --work-dir DIR        Working directory (default: ./work)
  --cleanup             Remove work dir before start
  --skip-download       Skip download phase
  --skip-dangerous      Skip new grant object type permission tests
  --help                Show this help
"""

import argparse
import os
import re
import signal
import subprocess
import sys
import time

import yaml

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ============================================================================
# Configuration
# ============================================================================

DOWNLOAD_BASE_URL = os.environ.get(
    "DOWNLOAD_BASE_URL", "https://repo.databend.com/databend/yf"
)

META_URLS = {
    "1.2.292": f"{DOWNLOAD_BASE_URL}/meta-srv-v1.2.292.tar.gz",
    "1.2.307": f"{DOWNLOAD_BASE_URL}/meta-srv-v1.2.307.tar.gz",
    "1.2.768": f"{DOWNLOAD_BASE_URL}/meta-srv-v1.2.768.tar.gz",
    "1.2.879": f"{DOWNLOAD_BASE_URL}/meta-srv-v1.2.879.tar.gz",
}

QUERY_URLS = {
    "1.2.636": f"{DOWNLOAD_BASE_URL}/databend-query-v1.2.636-rc8.6-93d82fc91f.tar.gz",
    "1.2.887": f"{DOWNLOAD_BASE_URL}/databend-query-v1.2.887-nightly-107e2a1327.tar.gz",
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
        self.skip_dangerous = _env("SKIP_DANGEROUS_PERM_TESTS", "0") == "1"

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
            eps.append(f'"{self.meta_listen_host}:{self.meta_grpc_port_base + i - 1}"')
        return ", ".join(eps)


cfg = Config()


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


def log_fail(msg):
    global _fail
    print(f"{RED}[FAIL]{NC} {msg}", flush=True)
    _fail += 1


def log_known(msg):
    global _known
    print(f"{YELLOW}[KNOWN]{NC} {msg}", flush=True)
    _known += 1


def log_skip(msg):
    global _skip
    print(f"{YELLOW}[SKIP]{NC} {msg}", flush=True)
    _skip += 1


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



# ============================================================================
# SQL Helpers
# ============================================================================

def run_sql(port, sql, user="root", password=""):
    args = [cfg.mysql_cmd, "-h127.0.0.1", f"-P{port}", f"-u{user}", "--batch", "-N"]
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


def expect_ok(port, sql, desc, user="root", password=""):
    rc, out = run_sql(port, sql, user, password)
    if not _is_error(rc, out):
        log_pass(desc)
    else:
        log_fail(f"{desc}  (rc={rc}, output: {out})")


def expect_fail(port, sql, desc, user="root", password=""):
    rc, out = run_sql(port, sql, user, password)
    if _is_error(rc, out):
        log_pass(f"{desc} (expected failure)")
    else:
        log_fail(f"{desc}  (expected failure but succeeded: {out})")


def expect_eq(port, sql, expected, desc, user="root", password=""):
    rc, out = run_sql(port, sql, user, password)
    trimmed = out.strip()
    if rc == 0 and trimmed == expected:
        log_pass(desc)
    else:
        log_fail(f"{desc}  (expected: '{expected}', got: '{trimmed}', rc={rc})")


def expect_contains(port, sql, pattern, desc, user="root", password=""):
    rc, out = run_sql(port, sql, user, password)
    if rc == 0 and re.search(pattern, out, re.IGNORECASE):
        log_pass(desc)
    else:
        log_fail(f"{desc}  (pattern '{pattern}' not found in: {out})")


def expect_known_fail(port, sql, desc, user="root", password=""):
    rc, out = run_sql(port, sql, user, password)
    if _is_error(rc, out):
        log_known(f"{desc} (confirmed incompatible)")
    else:
        log_pass(f"{desc} (unexpectedly works!)")


def expect_fail_contains(port, sql, pattern, desc, user="root", password=""):
    rc, out = run_sql(port, sql, user, password)
    if _is_error(rc, out) and re.search(pattern, out, re.IGNORECASE):
        log_pass(f"{desc} (expected failure with '{pattern}')")
    elif _is_error(rc, out):
        log_fail(f"{desc}  (failed but pattern '{pattern}' not found in: {out})")
    else:
        log_fail(f"{desc}  (expected failure but succeeded: {out})")


def wait_for_query(port, user="root", password="", timeout=None):
    if timeout is None:
        timeout = cfg.query_start_timeout
    for _ in range(timeout):
        rc, _ = run_sql(port, "SELECT 1", user, password)
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
    return True



# ============================================================================
# Meta Cluster Management
# ============================================================================

def _meta_bin(ver):
    return os.path.join(cfg.bin_dir, f"meta-{ver}", "databend-meta")


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
    grpc_port = cfg.meta_grpc_port_base + node_id - 1
    raft_port = cfg.meta_raft_port_base + node_id - 1
    admin_port = cfg.meta_admin_port_base + node_id - 1
    raft_dir = os.path.join(cfg.data_dir, f"meta-{node_id}")
    log_dir = os.path.join(cfg.log_dir, f"meta-{node_id}")
    os.makedirs(raft_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(cfg.conf_dir, exist_ok=True)
    h = cfg.meta_listen_host
    conf = os.path.join(cfg.conf_dir, f"meta-{node_id}.toml")
    with open(conf, "w") as f:
        f.write(f'admin_api_address = "{h}:{admin_port}"\n')
        f.write(f'grpc_api_address = "{h}:{grpc_port}"\n\n')
        f.write(f"[raft_config]\nid = {node_id}\n")
        f.write(f'raft_dir = "{raft_dir}"\n')
        f.write(f"raft_api_port = {raft_port}\n")
        f.write(f'raft_listen_host = "{h}"\nraft_advertise_host = "{h}"\n\n')
        f.write(f"[log]\n[log.file]\ndir = \"{log_dir}\"\nlevel = \"INFO\"\n")
    return conf


def start_meta_node(node_id, meta_ver, extra_args=None):
    if extra_args is None:
        extra_args = []
    conf = _gen_meta_config(node_id, meta_ver)
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
    _stop_process(os.path.join(cfg.data_dir, f"meta-{node_id}.pid"),
                  f"meta node {node_id}")


def start_meta_cluster(meta_ver):
    log_section(f"Starting Meta Cluster (v{meta_ver}, {cfg.meta_cluster_size} nodes)")
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
    h = cfg.meta_listen_host
    for i in range(1, cfg.meta_cluster_size + 1):
        log_info(f"--- Upgrading meta node {i} to v{target_ver} ---")
        stop_meta_node(i)
        time.sleep(2)
        # Find a running peer to join
        join_args = []
        for j in range(1, cfg.meta_cluster_size + 1):
            if j != i:
                join_args = ["--join", f"{h}:{cfg.meta_grpc_port_base + j - 1}"]
                break
        if not start_meta_node(i, target_ver, join_args):
            return False
        time.sleep(3)
        # Verify cluster health
        healthy = False
        for j in range(1, cfg.meta_cluster_size + 1):
            ap = cfg.meta_admin_port_base + j - 1
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


def _gen_query_config(label, version):
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


def start_query_node(label, version):
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


def stop_query_node(label):
    _stop_process(os.path.join(cfg.data_dir, f"query-{label}.pid"),
                  f"query {label}")


def stop_all_query():
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
    "ok":             lambda p, sql, t: expect_ok(p, sql, t["desc"], t["user"], t["pw"]),
    "eq":             lambda p, sql, t: expect_eq(p, sql, str(t.get("value", "")), t["desc"], t["user"], t["pw"]),
    "fail":           lambda p, sql, t: expect_fail(p, sql, t["desc"], t["user"], t["pw"]),
    "known_fail":     lambda p, sql, t: expect_known_fail(p, sql, t["desc"], t["user"], t["pw"]),
    "contains":       lambda p, sql, t: expect_contains(p, sql, t.get("pattern", ""), t["desc"], t["user"], t["pw"]),
    "fail_contains":  lambda p, sql, t: expect_fail_contains(p, sql, t.get("pattern", ""), t["desc"], t["user"], t["pw"]),
}

def run_yaml_test(yaml_file):
    with open(yaml_file) as f:
        doc = yaml.safe_load(f)

    name = doc.get("name", "")
    db = doc.get("database", "")
    cleanup_port = doc.get("cleanup_port", "OLD")
    skip_env = doc.get("skip_env", "")

    if skip_env and os.environ.get(skip_env, "0") == "1":
        log_skip(f"{name} skipped ({skip_env}=1)")
        return
    if cfg.skip_dangerous and skip_env == "SKIP_DANGEROUS_PERM_TESTS":
        log_skip(f"{name} skipped (--skip-dangerous)")
        return

    if name:
        log_section(name)
    if db:
        expect_ok(cfg.port_old, f"CREATE DATABASE IF NOT EXISTS {db}", f"Create {db} db")

    for t in doc.get("tests", []):
        sql = t.get("sql") or _expand_sql_generate(t.get("sql_generate", ""))
        sql = " ".join(sql.splitlines())

        p = cfg.port_new if t.get("port", "OLD") == "NEW" else cfg.port_old
        sleep_before = int(t.get("sleep_before", 0))
        if sleep_before > 0:
            time.sleep(sleep_before)

        ctx = {
            "desc": t.get("desc", ""),
            "user": t.get("user") or "root",
            "pw": t.get("password") or "",
            "value": t.get("value", ""),
            "pattern": t.get("pattern", ""),
        }
        expect = t.get("expect", "ok")
        handler = EXPECT_DISPATCH.get(expect)
        if handler:
            handler(p, sql, ctx)
        else:
            log_error(f"Unknown expect type '{expect}' in {yaml_file} ({ctx['desc']})")

    if db:
        cp = cfg.port_new if cleanup_port == "NEW" else cfg.port_old
        expect_ok(cp, f"DROP DATABASE {db}", f"Cleanup {db}")


# ============================================================================
# Phases
# ============================================================================

def phase_download(skip):
    if skip:
        log_info("Skipping download (--skip-download)")
        return True
    return download_all()


def phase_meta_setup():
    log_section("Phase: Meta Cluster Setup")
    return start_meta_cluster(cfg.initial_meta)


def _verify_query_during_meta_upgrade():
    port = cfg.port_old
    if not wait_for_query(port, timeout=10):
        log_error("Query A not reachable during meta upgrade verification")
        return False
    rc, out = run_sql(port, "SELECT 'meta_upgrade_ok'")
    if rc == 0 and out.strip() == "meta_upgrade_ok":
        log_pass("Query A works during meta rolling upgrade")
        return True
    log_fail(f"Query A broken during meta rolling upgrade: {out}")
    return False


def phase_meta_upgrade():
    log_section("Phase: Meta Rolling Upgrade")
    log_info(f"Upgrade path: v{cfg.initial_meta} → v{cfg.intermediate_meta} → v{cfg.target_meta}")

    log_info(f"Starting query A (v{cfg.old_query}) for upgrade verification...")
    if not start_query_node("a", cfg.old_query):
        return False
    expect_ok(cfg.port_old, "SELECT 'pre_upgrade_ok'", "Query A works before meta upgrade")

    expect_ok(cfg.port_old, "CREATE DATABASE IF NOT EXISTS upgrade_check", "Create upgrade_check db")
    expect_ok(cfg.port_old,
              "CREATE TABLE IF NOT EXISTS upgrade_check.survive (id INT, val VARCHAR)",
              "Create survival table")
    expect_ok(cfg.port_old,
              "INSERT INTO upgrade_check.survive VALUES (1,'before_upgrade')",
              "Insert pre-upgrade data")

    # Rolling upgrade: initial → intermediate
    if not rolling_upgrade_meta(cfg.intermediate_meta, _verify_query_during_meta_upgrade):
        return False
    expect_eq(cfg.port_old, "SELECT val FROM upgrade_check.survive WHERE id=1",
              "before_upgrade", "Data survived intermediate upgrade")

    # Rolling upgrade: intermediate → target
    if not rolling_upgrade_meta(cfg.target_meta, _verify_query_during_meta_upgrade):
        return False
    expect_eq(cfg.port_old, "SELECT val FROM upgrade_check.survive WHERE id=1",
              "before_upgrade", "Data survived final upgrade")

    log_info(f"Meta upgrade complete. Cluster is at v{cfg.target_meta}.")
    return True


def phase_query_setup():
    log_section("Phase: Query Clusters Setup")
    if not start_query_node("a", cfg.old_query):
        return False
    if not start_query_node("b", cfg.new_query):
        return False

    expect_ok(cfg.port_old, "SELECT 'old_ok'", f"OLD (v{cfg.old_query}) connected")
    expect_ok(cfg.port_new, "SELECT 'new_ok'", f"NEW (v{cfg.new_query}) connected")

    expect_eq(cfg.port_old, "SELECT val FROM upgrade_check.survive WHERE id=1",
              "before_upgrade", "OLD sees pre-upgrade data")
    expect_eq(cfg.port_new, "SELECT val FROM upgrade_check.survive WHERE id=1",
              "before_upgrade", "NEW sees pre-upgrade data")

    expect_ok(cfg.port_old, "DROP DATABASE IF EXISTS upgrade_check", "Cleanup upgrade_check")
    return True


def phase_compat_test():
    log_section("Phase: Cross-Version Compatibility Tests")
    log_info(f"OLD = v{cfg.old_query} (port {cfg.port_old})")
    log_info(f"NEW = v{cfg.new_query} (port {cfg.port_new})")

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
        "test_replace_travel_flashback",
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
    parser = argparse.ArgumentParser(
        description="Databend Upgrade Compatibility Test",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("phases", nargs="*", default=["all"],
                        help="Phases to run: download, meta-setup, meta-upgrade, query-setup, compat-test, all")
    parser.add_argument("--initial-meta", dest="initial_meta")
    parser.add_argument("--work-dir", dest="work_dir")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-dangerous", action="store_true")
    args = parser.parse_args()

    _validate_s3_env()

    if args.initial_meta:
        cfg.initial_meta = args.initial_meta
    if args.work_dir:
        cfg.work_dir = args.work_dir
    if args.skip_dangerous:
        cfg.skip_dangerous = True

    phases = set(args.phases)

    def should_run(phase):
        return "all" in phases or phase in phases

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))
    import atexit
    atexit.register(cleanup_processes)

    log_section("Databend Upgrade Compatibility Test")
    log_info(f"Initial meta:       v{cfg.initial_meta}")
    log_info(f"Intermediate meta:  v{cfg.intermediate_meta}")
    log_info(f"Target meta:        v{cfg.target_meta}")
    log_info(f"Old query:          v{cfg.old_query}")
    log_info(f"New query:          v{cfg.new_query}")
    log_info(f"Work dir:           {cfg.work_dir}")
    print()

    if args.cleanup:
        log_warn(f"Cleaning up work directory (preserving binaries): {cfg.work_dir}")
        cleanup_processes()
        import shutil
        for d in [cfg.data_dir, cfg.log_dir, cfg.conf_dir]:
            if os.path.isdir(d):
                shutil.rmtree(d)

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
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
