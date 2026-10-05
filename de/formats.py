from contextlib import contextmanager
from dataclasses import dataclass, asdict
import os
from pathlib import Path
import re
import sqlite3
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
from typing import ClassVar, Iterator, Optional
import tempfile
from de.core import rewrite_to_parquet_rs


@dataclass(frozen=True)
class CdcParams:
    min_chunk_size: int
    max_chunk_size: int
    norm_level: int


def exact_size(n: int) -> str:
    """Render a count with a binary unit only when it is exact: 65536 -> 64Ki, 300000 -> 300000."""
    for unit, scale in (("Gi", 2**30), ("Mi", 2**20), ("Ki", 2**10)):
        if n >= scale and n % scale == 0:
            return f"{n // scale}{unit}"
    return str(n)


@dataclass(frozen=True)
class FileFormat:
    """Base class for file format handlers."""

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def suffix(self) -> str:
        raise NotImplementedError

    @property
    def paramstem(self) -> str:
        raise NotImplementedError

    def derive_path(self, name: str, directory: Path) -> Path:
        """Construct an output path under directory using name."""
        if self.paramstem:
            slug = re.sub(r"[^a-zA-Z0-9]+", "-", self.paramstem)
            stem = f"{name}-{slug}"
        else:
            stem = name
        return directory / f"{stem}.{self.suffix}"

    def write(
        self,
        name: str,
        src: pa.Table | Path,
        directory: Path,
        sanity_check: bool = True,
    ) -> Path:
        """Write a PyArrow table or rewrite a source parquet file to the derived path."""
        raise NotImplementedError


@dataclass(frozen=True)
class Parquet(FileFormat):
    """Writer-agnostic parquet parameters, subclasses drive one writer implementation.

    With `batch_size=None` the whole source is written with a single write call
    (e.g. `pq.write_table`), otherwise it is streamed in batches of exactly
    `batch_size` rows, one write call per batch.
    """

    suffix = "parquet"
    # whether the implementation can write the source in batches
    streaming: ClassVar[bool] = True
    use_cdc: bool | CdcParams
    compression: Optional[str] = None
    use_dictionary: bool = True
    row_group_size: Optional[int] = None
    data_page_size: Optional[int] = None
    max_rows_per_page: Optional[int] = None
    batch_size: Optional[int] = None

    @property
    def paramstem(self) -> str:
        parts = []
        if self.compression is not None:
            parts.append(self.compression)
        if isinstance(self.use_cdc, CdcParams):
            cdc = self.use_cdc
            parts.append(
                f"cdc={exact_size(cdc.min_chunk_size)}-{exact_size(cdc.max_chunk_size)}"
                f"-n{cdc.norm_level}"
            )
        elif self.use_cdc:
            parts.append("cdc")
        if not self.use_dictionary:
            parts.append("nodict")
        if self.row_group_size is not None:
            parts.append(f"rg={exact_size(self.row_group_size)}")
        if self.data_page_size is not None:
            parts.append(f"dp={exact_size(self.data_page_size)}")
        if self.max_rows_per_page is not None:
            parts.append(f"rpp={exact_size(self.max_rows_per_page)}")
        if self.batch_size is not None:
            parts.append(f"stream={exact_size(self.batch_size)}")
        return " ".join(parts)

    def write(
        self,
        name: str,
        src: pa.Table | Path,
        directory: Path,
        sanity_check: bool = True,
    ) -> Path:
        dest = self.derive_path(name, directory)
        self._write(src, dest)
        if sanity_check:
            if isinstance(src, Path):
                src_meta = pq.read_metadata(src)
                dst_meta = pq.read_metadata(dest)
                assert src_meta.num_rows == dst_meta.num_rows
                assert (
                    src_meta.schema.to_arrow_schema()
                    == dst_meta.schema.to_arrow_schema()
                )
            else:
                assert src.equals(pq.read_table(dest))
        return dest

    def _write(self, src: pa.Table | Path, dest: Path) -> None:
        raise NotImplementedError


def _batches(
    src: pa.Table | Path, batch_size: int
) -> Iterator[pa.RecordBatch | pa.Table]:
    """Batches of exactly batch_size rows (except the last), independent of the source layout."""
    if isinstance(src, Path):
        yield from pq.ParquetFile(src).iter_batches(batch_size=batch_size)
    else:
        for offset in range(0, len(src), batch_size):
            yield src.slice(offset, batch_size).combine_chunks()


@dataclass(frozen=True)
class ParquetCpp(Parquet):
    name = "parquet-cpp"

    def _write(self, src: pa.Table | Path, dest: Path) -> None:
        if isinstance(self.use_cdc, CdcParams):
            cdc: bool | dict = asdict(self.use_cdc)
        else:
            cdc = self.use_cdc
        kwargs = dict(
            compression=self.compression,
            use_dictionary=self.use_dictionary,
            use_content_defined_chunking=cdc,
            data_page_size=self.data_page_size,
            max_rows_per_page=self.max_rows_per_page,
        )
        if self.batch_size is None:
            table = (
                pq.read_table(src).combine_chunks() if isinstance(src, Path) else src
            )
            pq.write_table(table, dest, row_group_size=self.row_group_size, **kwargs)
        else:
            schema = pq.read_schema(src) if isinstance(src, Path) else src.schema
            with pq.ParquetWriter(dest, schema, **kwargs) as writer:
                for batch in _batches(src, self.batch_size):
                    writer.write(batch, row_group_size=self.row_group_size)


@contextmanager
def _source_file(src: pa.Table | Path) -> Iterator[Path]:
    """The source as a parquet file, a table is written to a temporary one."""
    if isinstance(src, Path):
        yield src
    else:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "source.parquet"
            pq.write_table(src, path)
            yield path


@dataclass(frozen=True)
class ParquetRs(Parquet):
    name = "parquet-rs"

    def _write(self, src: pa.Table | Path, dest: Path) -> None:
        with _source_file(src) as path:
            rewrite_to_parquet_rs(str(path), str(dest), self)


@dataclass(frozen=True)
class ParquetJava(Parquet):
    """parquet-java, through the java/ParquetRewrite.java program.

    PARQUET_JAVA_CLASSPATH must hold parquet-hadoop and its dependencies.
    parquet-java writes record by record, there are no write calls to batch,
    so streaming isn't supported.
    """

    name = "parquet-java"
    streaming = False

    def _write(self, src: pa.Table | Path, dest: Path) -> None:
        if self.batch_size is not None:
            raise ValueError("parquet-java has no write calls to stream batches with")
        classpath = os.environ.get("PARQUET_JAVA_CLASSPATH")
        if not classpath:
            raise RuntimeError("set PARQUET_JAVA_CLASSPATH to use parquet-java")
        if isinstance(self.use_cdc, CdcParams):
            cdc = self.use_cdc
            use_cdc = f"{cdc.min_chunk_size},{cdc.max_chunk_size},{cdc.norm_level}"
        else:
            use_cdc = str(self.use_cdc).lower()
        options = dict(
            use_cdc=use_cdc,
            compression=self.compression,
            use_dictionary=str(self.use_dictionary).lower(),
            row_group_size=self.row_group_size,
            data_page_size=self.data_page_size,
            max_rows_per_page=self.max_rows_per_page,
        )
        program = Path(__file__).parent / "java" / "ParquetRewrite.java"
        with _source_file(src) as path:
            subprocess.run(
                ["java", "-cp", classpath, str(program), str(path), str(dest)]
                + [
                    f"{key}={value}"
                    for key, value in options.items()
                    if value is not None
                ],
                check=True,
                capture_output=True,
            )


@dataclass(frozen=True)
class JsonLines(FileFormat):
    name = "jsonl"
    suffix = "jsonlines"
    compression: Optional[str] = None

    @property
    def paramstem(self) -> str:
        return self.compression or ""

    def write(
        self,
        name: str,
        src: pa.Table | Path,
        directory: Path,
        sanity_check: bool = True,
    ) -> Path:
        path = self.derive_path(name, directory)
        table = pq.read_table(src) if isinstance(src, Path) else src
        table.to_pandas().to_json(
            path, orient="records", lines=True, compression=self.compression
        )
        return path


@dataclass(frozen=True)
class Sqlite(FileFormat):
    name = "sqlite"
    suffix = "sqlite"
    compression: Optional[str] = None

    @property
    def paramstem(self) -> str:
        return self.compression or ""

    def write(
        self,
        name: str,
        src: pa.Table | Path,
        directory: Path,
        sanity_check: bool = True,
    ) -> Path:
        path = self.derive_path(name, directory)
        table = pq.read_table(src) if isinstance(src, Path) else src
        con = sqlite3.connect(path)
        table.to_pandas().to_sql("table", con, if_exists="replace", index=False)
        return path
