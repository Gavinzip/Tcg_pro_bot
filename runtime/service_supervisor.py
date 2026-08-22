from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Mapping, Sequence


@dataclass(frozen=True)
class ServiceProcess:
    name: str
    script: Path
    env: Mapping[str, str]


def _signal_process(process: subprocess.Popen, signum: int) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signum)
        else:
            process.send_signal(signum)
    except ProcessLookupError:
        return


def _stop_processes(processes: Sequence[tuple[ServiceProcess, subprocess.Popen]]) -> None:
    for _, process in processes:
        _signal_process(process, signal.SIGTERM)

    deadline = time.monotonic() + 10.0
    for _, process in processes:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            _signal_process(process, signal.SIGKILL)
            process.wait()


def run_supervised_processes(specs: Sequence[ServiceProcess]) -> int:
    if not specs:
        raise ValueError("At least one service process is required")

    processes: list[tuple[ServiceProcess, subprocess.Popen]] = []
    shutdown_signal: int | None = None

    def request_shutdown(signum: int, _frame: object) -> None:
        nonlocal shutdown_signal
        shutdown_signal = signum

    previous_handlers = {
        signum: signal.getsignal(signum)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    for signum in previous_handlers:
        signal.signal(signum, request_shutdown)

    try:
        for spec in specs:
            if not spec.script.is_file():
                raise FileNotFoundError(f"Service entrypoint is missing: {spec.script}")
            process = subprocess.Popen(
                [sys.executable, str(spec.script)],
                env=dict(spec.env),
                start_new_session=(os.name == "posix"),
            )
            processes.append((spec, process))
            print(f"[service-supervisor] started {spec.name} pid={process.pid}", flush=True)

        while shutdown_signal is None:
            for spec, process in processes:
                return_code = process.poll()
                if return_code is None:
                    continue
                print(
                    f"[service-supervisor] {spec.name} exited code={return_code}; stopping container",
                    flush=True,
                )
                return return_code if return_code != 0 else 1
            time.sleep(0.25)

        print(
            f"[service-supervisor] received signal={shutdown_signal}; stopping services",
            flush=True,
        )
        return 128 + shutdown_signal
    finally:
        _stop_processes(processes)
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
