"""SSH port-forward tunnel lifecycle for remote Postgres (e.g. Arnold).

The tunnel runs as a child process so MCP clients (Cursor, Claude Desktop) tear it
down when `ppa serve` exits. A lightweight monitor thread restarts the tunnel if
the ssh process dies, with exponential backoff and a cap on consecutive failures.
"""

from __future__ import annotations

import logging
import os
import socket
import subprocess
import threading
import time

_log = logging.getLogger("ppa.tunnel")


class TunnelManager:
    """Manages an SSH tunnel as a child process tied to the MCP serve lifecycle.

    Design rationale: The tunnel must be a child process (not a separate script)
    so that Cursor/MCP clients automatically clean it up when the server stops.
    The previous model (manual ppa-tunnel.sh in a separate terminal) had two
    failure modes: (1) user forgets to start it, (2) it drops and nobody notices.

    Restart logic: exponential backoff (1s, 2s, 4s, max 30s) with a maximum of
    5 consecutive restart attempts before giving up and logging an error.
    """

    def __init__(self, host: str, local_port: int = 5433, remote_port: int = 5432) -> None:
        self.host = host.strip()
        self.local_port = int(local_port)
        self.remote_port = int(remote_port)
        self._process: subprocess.Popen[bytes] | None = None
        self._monitor: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._consecutive_failures = 0
        self._max_consecutive_failures = 5

    def _ssh_cmd(self) -> list[str]:
        return [
            "ssh",
            "-N",
            "-o",
            "BatchMode=yes",
            "-o",
            "ExitOnForwardFailure=yes",
            "-o",
            "ServerAliveInterval=60",
            "-o",
            "ServerAliveCountMax=3",
            "-L",
            f"{self.local_port}:127.0.0.1:{self.remote_port}",
            self.host,
        ]

    def _spawn_ssh(self) -> None:
        with self._lock:
            if self._process and self._process.poll() is None:
                return
            cmd = self._ssh_cmd()
            _log.info(
                "starting ssh tunnel local_port=%s remote_port=%s host=%s",
                self.local_port,
                self.remote_port,
                self.host,
            )
            try:
                # stderr must not be PIPE — ssh writes keepalives; a full pipe would block the tunnel.
                self._process = subprocess.Popen(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except OSError as exc:
                _log.error("failed to spawn ssh: %s", exc)
                raise

    def start(self) -> None:
        """Start the SSH tunnel and a daemon monitor that restarts it if needed."""
        self._spawn_ssh()
        # Wait briefly for the port to accept connections (ssh sets up the forward).
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            if self.is_alive():
                _log.info(
                    "tunnel established on 127.0.0.1:%s -> %s:127.0.0.1:%s",
                    self.local_port,
                    self.host,
                    self.remote_port,
                )
                self._consecutive_failures = 0
                break
            if self._process and self._process.poll() is not None:
                _log.error("ssh tunnel exited immediately rc=%s", self._process.returncode)
                raise RuntimeError("SSH tunnel failed to start; check host, keys, and network.")
            time.sleep(0.3)
        else:
            raise RuntimeError("SSH tunnel did not become ready on port %s within 30s" % self.local_port)

        self._monitor = threading.Thread(target=self._monitor_loop, name="ppa-tunnel-monitor", daemon=True)
        self._monitor.start()

    def stop(self) -> None:
        """Terminate the tunnel and stop the monitor thread."""
        self._stop.set()
        if self._monitor and self._monitor.is_alive():
            self._monitor.join(timeout=2.0)
        proc = self._process
        self._process = None
        if proc is None:
            return
        if proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2.0)
        _log.info("tunnel stopped")

    def is_alive(self) -> bool:
        """True if ssh is running and the local forward port accepts connections."""
        proc = self._process
        if proc is None or proc.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", self.local_port), timeout=1.0):
                pass
        except OSError:
            return False
        return True

    def _restart_with_backoff(self) -> bool:
        """Kill dead process, respawn ssh, wait for port. Returns True if healthy."""
        with self._lock:
            old = self._process
            self._process = None
            if old and old.poll() is None:
                old.terminate()
                try:
                    old.wait(timeout=3.0)
                except subprocess.TimeoutExpired:
                    old.kill()
        delay = 1.0
        max_delay = 30.0
        while delay <= max_delay + 0.01:
            try:
                self._spawn_ssh()
            except OSError as exc:
                _log.warning("ssh restart spawn failed: %s", exc)
                time.sleep(delay)
                delay = min(delay * 2, max_delay)
                continue
            deadline = time.monotonic() + 15.0
            while time.monotonic() < deadline:
                if self.is_alive():
                    _log.info("tunnel restarted successfully on 127.0.0.1:%s", self.local_port)
                    return True
                if self._process and self._process.poll() is not None:
                    break
                time.sleep(0.2)
            _log.warning("tunnel restart did not become ready; backing off %.1fs", delay)
            if self._process and self._process.poll() is None:
                self._process.terminate()
            self._process = None
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
        return False

    def _monitor_loop(self) -> None:
        poll_s = float(os.environ.get("PPA_TUNNEL_MONITOR_INTERVAL", "5") or "5")
        while not self._stop.wait(timeout=poll_s):
            if self.is_alive():
                self._consecutive_failures = 0
                continue
            _log.warning("tunnel not healthy; attempting restart")
            ok = self._restart_with_backoff()
            if not ok:
                self._consecutive_failures += 1
                _log.error(
                    "tunnel restart failed (%s/%s consecutive)",
                    self._consecutive_failures,
                    self._max_consecutive_failures,
                )
                if self._consecutive_failures >= self._max_consecutive_failures:
                    _log.error("giving up on tunnel restarts; fix SSH connectivity and restart ppa serve")
                    break
            else:
                self._consecutive_failures = 0
