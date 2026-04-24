"""Storage backends for inputs, charts, and rendered artifacts."""

from __future__ import annotations

import io
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Protocol

try:
    import boto3
except ImportError:  # pragma: no cover - optional dependency guard
    boto3 = None  # type: ignore[assignment]

try:
    from botocore.config import Config
except ImportError:  # pragma: no cover - optional dependency guard
    Config = None  # type: ignore[assignment]

from service.config import Settings, get_settings


class StorageBackend(Protocol):
    """Contract for object-storage style persistence."""

    def upload_bytes(self, key: str, payload: bytes, content_type: str | None = None) -> None: ...

    def upload_file(self, local_path: Path, key: str, content_type: str | None = None) -> None: ...

    def download_to_path(self, key: str, destination: Path) -> None: ...

    def open_stream(self, key: str) -> BinaryIO: ...

    def delete_prefix(self, prefix: str) -> None: ...

    def exists(self, key: str) -> bool: ...


@dataclass(frozen=True)
class LocalStorageBackend:
    """Filesystem-backed storage for local development and tests."""

    root: Path

    def _resolve(self, key: str) -> Path:
        return (self.root / key).resolve()

    def upload_bytes(self, key: str, payload: bytes, content_type: str | None = None) -> None:
        del content_type
        destination = self._resolve(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)

    def upload_file(self, local_path: Path, key: str, content_type: str | None = None) -> None:
        del content_type
        destination = self._resolve(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, destination)

    def download_to_path(self, key: str, destination: Path) -> None:
        source = self._resolve(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

    def open_stream(self, key: str) -> BinaryIO:
        return self._resolve(key).open("rb")

    def delete_prefix(self, prefix: str) -> None:
        target = self._resolve(prefix)
        if target.is_dir():
            shutil.rmtree(target, ignore_errors=True)
        elif target.exists():
            target.unlink(missing_ok=True)

    def exists(self, key: str) -> bool:
        return self._resolve(key).exists()


@dataclass(frozen=True)
class S3StorageBackend:
    """S3-compatible storage backed by boto3."""

    bucket: str
    client: object

    def upload_bytes(self, key: str, payload: bytes, content_type: str | None = None) -> None:
        extra = {"ContentType": content_type} if content_type else {}
        self.client.put_object(Bucket=self.bucket, Key=key, Body=payload, **extra)

    def upload_file(self, local_path: Path, key: str, content_type: str | None = None) -> None:
        extra = {"ContentType": content_type} if content_type else {}
        if extra:
            self.client.upload_file(str(local_path), self.bucket, key, ExtraArgs=extra)
        else:
            self.client.upload_file(str(local_path), self.bucket, key)

    def download_to_path(self, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket, key, str(destination))

    def open_stream(self, key: str) -> BinaryIO:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"]

    def delete_prefix(self, prefix: str) -> None:
        continuation_token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            response = self.client.list_objects_v2(**kwargs)
            contents = response.get("Contents", [])
            if contents:
                delete_payload = {"Objects": [{"Key": item["Key"]} for item in contents]}
                self.client.delete_objects(Bucket=self.bucket, Delete=delete_payload)
            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False


def _build_s3_client(settings: Settings) -> object:
    if boto3 is None:
        raise RuntimeError("boto3 is required for STORAGE_BACKEND=s3.")
    session = boto3.session.Session()
    client_config = None
    if Config is not None:
        client_config = Config(s3={"addressing_style": settings.s3_addressing_style})
    return session.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        use_ssl=settings.s3_secure,
        config=client_config,
    )


def get_storage_backend() -> StorageBackend:
    """Return the configured storage backend instance."""
    settings = get_settings()
    if settings.storage_backend == "s3":
        return S3StorageBackend(bucket=settings.s3_bucket, client=_build_s3_client(settings))

    settings.local_storage_root.mkdir(parents=True, exist_ok=True)
    return LocalStorageBackend(root=settings.local_storage_root)


def guess_content_type(path: Path) -> str:
    """Return a coarse content type for stored artifacts."""
    suffix = path.suffix.lower()
    if suffix == ".png":
        return "image/png"
    if suffix == ".docx":
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if suffix == ".pdf":
        return "application/pdf"
    if suffix == ".ipynb":
        return "application/x-ipynb+json"
    if suffix == ".txt":
        return "text/plain; charset=utf-8"
    if suffix == ".zip":
        return "application/zip"
    if suffix == ".csv":
        return "text/csv; charset=utf-8"
    if suffix == ".xlsx":
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if suffix == ".xls":
        return "application/vnd.ms-excel"
    return "application/octet-stream"


def open_bytes_stream(payload: bytes) -> BinaryIO:
    """Wrap bytes in a BinaryIO object."""
    return io.BytesIO(payload)


def check_storage_ready(storage: StorageBackend | None = None) -> tuple[bool, str]:
    """Check whether the configured object storage is reachable."""
    backend = storage or get_storage_backend()
    settings = get_settings()
    try:
        if isinstance(backend, LocalStorageBackend):
            backend.root.mkdir(parents=True, exist_ok=True)
            key = "_health/readyz.txt"
            backend.upload_bytes(key, b"ok", content_type="text/plain")
            backend.delete_prefix(key)
            return True, "ok"

        if isinstance(backend, S3StorageBackend):
            head_bucket = getattr(backend.client, "head_bucket", None)
            if callable(head_bucket):
                head_bucket(Bucket=backend.bucket)
            else:
                key = "_health/readyz.txt"
                backend.upload_bytes(key, b"ok", content_type="text/plain")
                backend.delete_prefix(key)
            return True, "ok"
    except Exception as exc:
        return False, str(exc)

    return False, f"Unsupported storage backend: {settings.storage_backend}"
