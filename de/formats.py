from dataclasses import dataclass, asdict
import os
from pathlib import Path
import re
import sqlite3
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
from typing import ClassVar, Iterable, Optional
import tempfile
from de.core import rewrite_to_parquet_rs

# a page limit large enough to never cut
NO_LIMIT = 2**30


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
    # whether the implementation can limit the rows of a page
    page_row_limit: ClassVar[bool] = True
    use_cdc: bool | CdcParams
    compression: Optional[str] = None
    use_dictionary: bool = True
    row_group_size: Optional[int] = None
    data_page_size: Optional[int] = None
    max_rows_per_page: Optional[int] = None
    batch_size: Optional[int] = None

    def __post_init__(self):
        if self.batch_size is not None and not self.streaming:
            raise ValueError(f"{self.name} has no write calls to stream batches with")
        if self.max_rows_per_page not in (None, NO_LIMIT) and not self.page_row_limit:
            raise ValueError(f"{self.name} has no row limit per page")

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
        """Rewrite the source file, a table is written to a temporary file first.

        Implementations that can write tables directly override this instead of
        `_rewrite`.
        """
        if isinstance(src, Path):
            self._rewrite(src, dest)
        else:
            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "source.parquet"
                pq.write_table(src, path)
                self._rewrite(path, dest)

    def _rewrite(self, src: Path, dest: Path) -> None:
        raise NotImplementedError


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
            if isinstance(src, Path):
                src = pq.read_table(src).combine_chunks()
            pq.write_table(src, dest, row_group_size=self.row_group_size, **kwargs)
            return

        # batches of exactly batch_size rows, independent of the source's layout
        size = self.batch_size
        batches: Iterable[pa.RecordBatch | pa.Table]
        if isinstance(src, Path):
            source = pq.ParquetFile(src)
            schema, batches = source.schema_arrow, source.iter_batches(batch_size=size)
        else:
            schema = src.schema
            batches = (
                src.slice(offset, size).combine_chunks()
                for offset in range(0, len(src), size)
            )
        with pq.ParquetWriter(dest, schema, **kwargs) as writer:
            for batch in batches:
                writer.write(batch, row_group_size=self.row_group_size)


@dataclass(frozen=True)
class ParquetRs(Parquet):
    name = "parquet-rs"

    def _rewrite(self, src: Path, dest: Path) -> None:
        rewrite_to_parquet_rs(str(src), str(dest), self)


@dataclass(frozen=True)
class ParquetJava(Parquet):
    """parquet-java, through the java/ParquetRewrite.java program.

    PARQUET_JAVA_CLASSPATH must hold parquet-hadoop and its dependencies.
    parquet-java writes record by record, there are no write calls to batch,
    so streaming isn't supported.
    """

    name = "parquet-java"
    streaming = False

    def _rewrite(self, src: Path, dest: Path) -> None:
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
        subprocess.run(
            ["java", "-cp", classpath, str(program), str(src), str(dest)]
            + [f"{key}={value}" for key, value in options.items() if value is not None],
            check=True,
            capture_output=True,
        )


@dataclass(frozen=True)
class ParquetDuckDB(Parquet):
    """DuckDB, through a COPY statement run by the binary at DUCKDB_BINARY.

    A COPY has no write calls to batch, so streaming isn't supported. DuckDB's
    pages have no row limit, only NO_LIMIT is accepted for max_rows_per_page,
    and NO_LIMIT for data_page_size keeps DuckDB's own 100MiB page limit.

    Row groups are rounded up to whole vectors of 2048 rows, and a source row
    group less than two vectors short of row_group_size is kept as it is, so
    small source row groups show through (the benchmark's sources have 1Mi).
    """

    name = "duckdb"
    streaming = False
    page_row_limit = False

    def _rewrite(self, src: Path, dest: Path) -> None:
        binary = os.environ.get("DUCKDB_BINARY")
        if not binary:
            raise RuntimeError("set DUCKDB_BINARY to use duckdb")
        # left out when disabled, so a duckdb without CDC can write the baseline
        use_cdc: Optional[str] = "true" if self.use_cdc else None
        if isinstance(self.use_cdc, CdcParams):
            cdc = self.use_cdc
            use_cdc = (
                f"{{min_chunk_size: {cdc.min_chunk_size}, "
                f"max_chunk_size: {cdc.max_chunk_size}, norm_level: {cdc.norm_level}}}"
            )
        options = dict(
            format="parquet",
            content_defined_chunking=use_cdc,
            # uncompressed by default like the other writers, duckdb's is snappy
            compression=(
                "uncompressed"
                if self.compression in (None, "none")
                else self.compression
            ),
            dictionary_size_limit=None if self.use_dictionary else 0,
            row_group_size=self.row_group_size,
            data_page_size_limit=(
                None if self.data_page_size == NO_LIMIT else self.data_page_size
            ),
        )
        copy = ", ".join(
            f"{key} {value}" for key, value in options.items() if value is not None
        )
        source, target = (str(path).replace("'", "''") for path in (src, dest))
        result = subprocess.run(
            [
                binary,
                "-c",
                # columns without nulls would be written as required, unlike the source
                "SET disabled_optimizers = 'statistics_propagation';"
                f"COPY (FROM read_parquet('{source}')) TO '{target}' ({copy})",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode:
            raise RuntimeError(f"duckdb failed: {result.stderr}")


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
