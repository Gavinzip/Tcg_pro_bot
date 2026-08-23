from __future__ import annotations

import multiprocessing
from multiprocessing.connection import Connection
import signal
import threading
import uuid
from typing import Any


class ProfileLookupWorkerError(RuntimeError):
    def __init__(self, cause_name: str = "profile_lookup_failed") -> None:
        super().__init__("profile_lookup_failed")
        self.cause_name = str(cause_name or "profile_lookup_failed")


class ProfileLookupWorkerTimeout(TimeoutError):
    pass


def _profile_lookup_worker_main(connection: Connection) -> None:
    from runtime.profile_data import ProfileDataError, build_wallet_profile_data

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        while True:
            request = connection.recv()
            if request is None:
                return
            request_id = str(request.get("request_id") or "")
            try:
                payload = build_wallet_profile_data(
                    str(request.get("wallet") or ""),
                    language=str(request.get("language") or "zh-Hant"),
                    include_extremes=bool(request.get("include_extremes")),
                    include_posters=bool(request.get("include_posters")),
                    poster_kind=request.get("poster_kind"),
                )
                response = {"request_id": request_id, "ok": True, "payload": payload}
            except ProfileDataError as exc:
                cause_name = type(exc.__cause__).__name__ if exc.__cause__ is not None else type(exc).__name__
                response = {
                    "request_id": request_id,
                    "ok": False,
                    "error": "profile_lookup_failed",
                    "cause_name": cause_name,
                }
            except BaseException as exc:
                response = {
                    "request_id": request_id,
                    "ok": False,
                    "error": "profile_lookup_failed",
                    "cause_name": type(exc).__name__,
                }
            connection.send(response)
    except (EOFError, BrokenPipeError, ConnectionResetError):
        return
    finally:
        connection.close()


class ProfileLookupWorker:
    """Persistent isolated worker whose active lookup can be terminated on deadline."""

    def __init__(self, *, start_method: str | None = None) -> None:
        # Production always uses spawn so a restarted worker cannot inherit the
        # parent process' currently-held cross-process profile lock.
        method = str(start_method or "spawn").strip()
        self._context = multiprocessing.get_context(method)
        self._lock = threading.Lock()
        self._process: multiprocessing.Process | None = None
        self._connection: Connection | None = None

    def _start_locked(self) -> None:
        if self._process is not None and self._process.is_alive() and self._connection is not None:
            return
        self._stop_locked(graceful=False)
        parent_connection, child_connection = self._context.Pipe(duplex=True)
        process = self._context.Process(
            target=_profile_lookup_worker_main,
            args=(child_connection,),
            name="tcg-profile-lookup-worker",
            daemon=True,
        )
        process.start()
        child_connection.close()
        self._process = process
        self._connection = parent_connection

    def start(self) -> None:
        with self._lock:
            self._start_locked()

    def _stop_locked(self, *, graceful: bool) -> None:
        process = self._process
        connection = self._connection
        self._process = None
        self._connection = None
        if graceful and process is not None and process.is_alive() and connection is not None:
            try:
                connection.send(None)
            except (BrokenPipeError, EOFError, OSError):
                pass
            process.join(timeout=2.0)
        if process is not None and process.is_alive():
            process.terminate()
            process.join(timeout=3.0)
        if process is not None and process.is_alive():
            process.kill()
            process.join(timeout=1.0)
        if connection is not None:
            connection.close()

    def close(self) -> None:
        with self._lock:
            self._stop_locked(graceful=True)

    def run(
        self,
        *,
        wallet: str,
        language: str,
        include_extremes: bool,
        include_posters: bool,
        poster_kind: str | None,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        timeout = max(0.1, float(timeout_seconds))
        with self._lock:
            self._start_locked()
            process = self._process
            connection = self._connection
            if process is None or connection is None:
                raise ProfileLookupWorkerError("worker_start_failed")

            request_id = uuid.uuid4().hex
            try:
                connection.send(
                    {
                        "request_id": request_id,
                        "wallet": wallet,
                        "language": language,
                        "include_extremes": include_extremes,
                        "include_posters": include_posters,
                        "poster_kind": poster_kind,
                    }
                )
                if not connection.poll(timeout):
                    self._stop_locked(graceful=False)
                    raise ProfileLookupWorkerTimeout("profile lookup exceeded its server deadline")
                response = connection.recv()
            except ProfileLookupWorkerTimeout:
                raise
            except (BrokenPipeError, EOFError, OSError) as exc:
                self._stop_locked(graceful=False)
                raise ProfileLookupWorkerError(type(exc).__name__) from exc

            if not isinstance(response, dict) or response.get("request_id") != request_id:
                self._stop_locked(graceful=False)
                raise ProfileLookupWorkerError("worker_protocol_error")
            if not response.get("ok"):
                raise ProfileLookupWorkerError(str(response.get("cause_name") or "profile_lookup_failed"))
            payload = response.get("payload")
            if not isinstance(payload, dict):
                raise ProfileLookupWorkerError("worker_payload_error")
            return payload
