"""Host abstraction layer for local and remote (SSH) execution.

Provides a unified interface so run.py can operate identically in local mode
(all processes on localhost) and distributed mode (processes on remote EC2s).
"""

import getpass
import os
import signal
import socket
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


class Host:
    """Base class — unified interface for local or remote host operations."""

    @property
    def ip(self) -> str:
        raise NotImplementedError

    def run_cmd(self, cmd, timeout=30):
        """Execute shell command. Returns (returncode, combined stdout+stderr)."""
        raise NotImplementedError

    def start_daemon(self, cmd, log_file, pid_file):
        """Start a background process, write its PID to pid_file."""
        raise NotImplementedError

    def stop_daemon(self, pid_file, label):
        """Stop process by PID file (SIGTERM → wait → SIGKILL)."""
        raise NotImplementedError

    def is_alive(self, pid_file):
        """Check if PID-file process is alive. Returns (bool, pid)."""
        raise NotImplementedError

    def makedirs(self, path):
        """Recursively create directories."""
        raise NotImplementedError

    def write_file(self, path, content):
        """Write string content to a file."""
        raise NotImplementedError

    def upload(self, local_path, remote_path):
        """Upload file/directory. LocalHost: no-op when paths match, else cp."""
        raise NotImplementedError

    def file_exists(self, path) -> bool:
        raise NotImplementedError

    def file_is_executable(self, path) -> bool:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# LocalHost
# ---------------------------------------------------------------------------

class LocalHost(Host):
    """Wraps subprocess calls for localhost execution."""

    @property
    def ip(self):
        return "127.0.0.1"

    def run_cmd(self, cmd, timeout=30):
        try:
            r = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=timeout,
            )
            return r.returncode, (r.stdout + r.stderr).strip()
        except subprocess.TimeoutExpired:
            return 1, f"Command timed out after {timeout}s"

    def start_daemon(self, cmd, log_file, pid_file):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, "w") as lf:
            proc = subprocess.Popen(
                cmd, shell=True, stdout=lf, stderr=lf,
            )
        with open(pid_file, "w") as f:
            f.write(str(proc.pid))
        return proc.pid

    def stop_daemon(self, pid_file, label):
        alive, pid = self.is_alive(pid_file)
        if not alive:
            if os.path.isfile(pid_file):
                os.remove(pid_file)
            return
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

    def is_alive(self, pid_file):
        if not os.path.isfile(pid_file):
            return False, 0
        try:
            pid = int(open(pid_file).read().strip())
        except (ValueError, OSError):
            return False, 0
        try:
            os.kill(pid, 0)
            return True, pid
        except OSError:
            return False, pid

    def makedirs(self, path):
        os.makedirs(path, exist_ok=True)

    def write_file(self, path, content):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(content)

    def upload(self, local_path, remote_path):
        # No-op if same path; otherwise copy
        if os.path.abspath(local_path) == os.path.abspath(remote_path):
            return
        rc = subprocess.call(["cp", "-a", local_path, remote_path])
        if rc != 0:
            raise RuntimeError(f"Local copy failed: {local_path} -> {remote_path}")

    def file_exists(self, path):
        return os.path.isfile(path)

    def file_is_executable(self, path):
        return os.path.isfile(path) and os.access(path, os.X_OK)


# ---------------------------------------------------------------------------
# RemoteHost
# ---------------------------------------------------------------------------

class RemoteHost(Host):
    """Executes commands on a remote machine via SSH."""

    def __init__(self, ip, user=None, ssh_key=None):
        self._host = ip  # original hostname or IP (used for SSH)
        self._ip = self._resolve_ip(ip)
        self._user = user or getpass.getuser()
        self._ssh_key = ssh_key
        self._ssh_base = self._build_ssh_cmd()
        self._scp_base = self._build_scp_cmd()

    @staticmethod
    def _resolve_ip(host):
        """Resolve hostname to IP address. Pass through if already an IP."""
        try:
            socket.inet_aton(host)
            return host  # already an IP
        except OSError:
            pass
        try:
            return socket.gethostbyname(host)
        except socket.gaierror:
            return host  # fallback to original

    def _build_ssh_cmd(self):
        cmd = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            "-o", "BatchMode=yes",
        ]
        if self._ssh_key:
            cmd.extend(["-i", self._ssh_key])
        cmd.append(f"{self._user}@{self._host}")
        return cmd

    def _build_scp_cmd(self):
        cmd = [
            "rsync", "-az",
            "-e", "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes"
            + (f" -i {self._ssh_key}" if self._ssh_key else ""),
        ]
        return cmd

    @property
    def ip(self):
        return self._ip

    def run_cmd(self, cmd, timeout=30):
        full = self._ssh_base + [cmd]
        try:
            r = subprocess.run(full, capture_output=True, text=True, timeout=timeout)
            return r.returncode, (r.stdout + r.stderr).strip()
        except subprocess.TimeoutExpired:
            return 1, f"SSH command timed out after {timeout}s"

    def start_daemon(self, cmd, log_file, pid_file):
        self.run_cmd(f"mkdir -p {os.path.dirname(log_file)}")
        # nohup + redirect, capture PID
        remote_cmd = (
            f"nohup {cmd} > {log_file} 2>&1 & echo $! > {pid_file}"
        )
        rc, out = self.run_cmd(remote_cmd, timeout=15)
        if rc != 0:
            raise RuntimeError(f"Failed to start daemon on {self._ip}: {out}")
        # Read back the PID
        rc2, pid_str = self.run_cmd(f"cat {pid_file}", timeout=5)
        return int(pid_str.strip()) if rc2 == 0 and pid_str.strip().isdigit() else 0

    def stop_daemon(self, pid_file, label):
        rc, pid_str = self.run_cmd(f"cat {pid_file} 2>/dev/null", timeout=5)
        if rc != 0 or not pid_str.strip().isdigit():
            self.run_cmd(f"rm -f {pid_file}", timeout=5)
            return
        pid = pid_str.strip()
        self.run_cmd(f"kill -TERM {pid} 2>/dev/null", timeout=5)
        # Wait up to 10s for exit
        for _ in range(10):
            chk_rc, _ = self.run_cmd(f"kill -0 {pid} 2>/dev/null", timeout=5)
            if chk_rc != 0:
                break
            time.sleep(1)
        else:
            self.run_cmd(f"kill -9 {pid} 2>/dev/null", timeout=5)
        self.run_cmd(f"rm -f {pid_file}", timeout=5)

    def is_alive(self, pid_file):
        rc, pid_str = self.run_cmd(f"cat {pid_file} 2>/dev/null", timeout=5)
        if rc != 0 or not pid_str.strip().isdigit():
            return False, 0
        pid = int(pid_str.strip())
        chk_rc, _ = self.run_cmd(f"kill -0 {pid} 2>/dev/null", timeout=5)
        return chk_rc == 0, pid

    def makedirs(self, path):
        self.run_cmd(f"mkdir -p {path}", timeout=10)

    def write_file(self, path, content):
        self.run_cmd(f"mkdir -p {os.path.dirname(path)}", timeout=10)
        # Use heredoc via stdin to avoid shell quoting issues
        full = self._ssh_base + [f"cat > {path}"]
        subprocess.run(full, input=content, text=True, timeout=15, check=True)

    def upload(self, local_path, remote_path):
        self.run_cmd(f"mkdir -p {remote_path}", timeout=10)
        dest = f"{self._user}@{self._host}:{remote_path}"
        cmd = self._scp_base + [local_path + "/", dest]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if r.returncode != 0:
            raise RuntimeError(
                f"rsync to {self._host}:{remote_path} failed: {r.stderr}"
            )

    def file_exists(self, path):
        rc, _ = self.run_cmd(f"test -f {path}", timeout=5)
        return rc == 0

    def file_is_executable(self, path):
        rc, _ = self.run_cmd(f"test -x {path}", timeout=5)
        return rc == 0

    def __repr__(self):
        return f"RemoteHost({self._ip})"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def check_ssh_connectivity(hosts):
    """Check SSH connectivity to all remote hosts in parallel. Returns True if all OK."""
    remotes = [h for h in hosts if isinstance(h, RemoteHost)]
    if not remotes:
        return True

    def _check(host):
        rc, out = host.run_cmd("echo ok", timeout=10)
        return host, rc == 0 and "ok" in out

    ok = True
    with ThreadPoolExecutor(max_workers=len(remotes)) as pool:
        futs = {pool.submit(_check, h): h for h in remotes}
        for fut in as_completed(futs):
            host, reachable = fut.result()
            if not reachable:
                print(f"[ERROR] SSH connectivity failed for {host.ip}")
                ok = False
            else:
                print(f"[INFO] SSH OK: {host.ip}")
    return ok


def distribute_binaries(local_bin_dir, hosts, remote_bin_dir):
    """rsync the local bin directory to all remote hosts in parallel."""
    remotes = [h for h in hosts if isinstance(h, RemoteHost)]
    if not remotes:
        return True
    # Deduplicate by IP
    seen = set()
    unique = []
    for h in remotes:
        if h.ip not in seen:
            seen.add(h.ip)
            unique.append(h)

    def _upload(host):
        host.upload(local_bin_dir, remote_bin_dir)
        return host

    ok = True
    with ThreadPoolExecutor(max_workers=len(unique)) as pool:
        futs = {pool.submit(_upload, h): h for h in unique}
        for fut in as_completed(futs):
            try:
                host = fut.result()
                print(f"[INFO] Binaries distributed to {host.ip}")
            except Exception as e:
                ok = False
                host = futs[fut]
                print(f"[ERROR] Failed to distribute binaries to {host.ip}: {e}")
    return ok
