from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
import os
from pathlib import Path
import re
import sqlite3
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
from typing import ClassVar, Optional, Sequence
import tempfile
from de.core import rewrite_to_parquet_rs, write_to_parquet_rs

# a page limit large enough to never cut
NO_LIMIT = 2**30

# the encodings of parquet-java's and parquet-rs's writer version 2.0 for values that
# aren't dictionary encoded, by physical type: the others are PLAIN, booleans RLE
V2_ENCODINGS = {
    "INT32": "DELTA_BINARY_PACKED",
    "INT64": "DELTA_BINARY_PACKED",
    "BYTE_ARRAY": "DELTA_BYTE_ARRAY",
    "FIXED_LEN_BYTE_ARRAY": "DELTA_BYTE_ARRAY",
}


def decode_dictionaries(table: pa.Table) -> pa.Table:
    """The table with its dictionary columns cast to their values."""
    fields = [
        field.with_type(field.type.value_type)
        if pa.types.is_dictionary(field.type)
        else field
        for field in table.schema
    ]
    return table.cast(pa.schema(fields))


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

    `data_page_version="2.0"` writes v2 data pages with the encodings of
    parquet-java's and parquet-rs's writer version 2.0: values that aren't
    dictionary encoded are delta encoded if they are integers or byte arrays.
    """

    suffix = "parquet"
    # whether the implementation can write the source in batches
    streaming: ClassVar[bool] = True
    # whether the implementation can limit the rows of a page
    page_row_limit: ClassVar[bool] = True
    # whether the implementation can write v2 data pages
    data_page_v2: ClassVar[bool] = True
    # whether the implementation writes arrow tables, rather than a parquet file's
    # rewrite that never sees the arrow-only types
    arrow_input: ClassVar[bool] = True
    use_cdc: bool | CdcParams
    compression: Optional[str] = None
    use_dictionary: bool = True
    row_group_size: Optional[int] = None
    data_page_size: Optional[int] = None
    max_rows_per_page: Optional[int] = None
    data_page_version: str = "1.0"
    batch_size: Optional[int] = None

    def __post_init__(self):
        if self.data_page_version not in ("1.0", "2.0"):
            raise ValueError(f"unknown data page version: {self.data_page_version}")
        if self.data_page_version == "2.0" and not self.data_page_v2:
            raise ValueError(f"{self.name} writes no v2 data pages")
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
        if self.data_page_version == "2.0":
            parts.append("v2")
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
            self._check(src, dest)
        return dest

    @classmethod
    def write_many(
        cls,
        jobs: Sequence[tuple["Parquet", str, pa.Table, Path]],
        max_workers: Optional[int] = None,
    ) -> list[Path | BaseException]:
        """Write the table of each (format, name, table, directory) job, like `write`.

        Returns the written path or the exception of each job, in the jobs' order. A
        panic of the Rust extension is returned too, though it isn't an Exception.
        """

        def write(job: tuple[Parquet, str, pa.Table, Path]) -> Path | BaseException:
            fmt, name, table, directory = job
            try:
                return fmt.write(name, table, directory)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as e:
                return e

        with ThreadPoolExecutor(max_workers) as executor:
            return list(executor.map(write, jobs))

    def _check(self, src: pa.Table | Path, dest: Path) -> None:
        """Assert that the written file holds the source's data.

        Dictionaries are compared decoded: a reader rebuilds the dictionary of a
        column written without dictionary encoding, in the order its values appear.
        """
        if isinstance(src, Path):
            src_meta = pq.read_metadata(src)
            dst_meta = pq.read_metadata(dest)
            assert src_meta.num_rows == dst_meta.num_rows
            assert (
                src_meta.schema.to_arrow_schema() == dst_meta.schema.to_arrow_schema()
            )
        else:
            written = pq.read_table(dest)
            assert decode_dictionaries(src).equals(decode_dictionaries(written)), (
                f"{dest.name} reads back different data, "
                f"{written.num_rows} of {src.num_rows} rows"
            )

    def _options(self) -> list[str]:
        """The KEY=VALUE options of the ParquetRewrite.java program."""
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
            data_page_version=self.data_page_version,
        )
        return [f"{key}={value}" for key, value in options.items() if value is not None]

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
    """parquet-cpp, through pyarrow.

    pyarrow writes every chunk of a table with a column writer call of its own,
    inside the row groups, so streaming writes a table whose chunks are the batches.
    """

    name = "parquet-cpp"

    def _write(self, src: pa.Table | Path, dest: Path) -> None:
        if isinstance(self.use_cdc, CdcParams):
            cdc: bool | dict = asdict(self.use_cdc)
        else:
            cdc = self.use_cdc
        if isinstance(src, Path):
            src = pq.read_table(src).combine_chunks()
        if self.batch_size is not None:
            # batches of exactly batch_size rows, independent of the source's chunks
            batches = src.combine_chunks().to_batches(max_chunksize=self.batch_size)
            src = pa.Table.from_batches(batches, src.schema)
        pq.write_table(
            src,
            dest,
            row_group_size=self.row_group_size,
            compression=self.compression,
            use_dictionary=self.use_dictionary,
            use_content_defined_chunking=cdc,
            data_page_size=self.data_page_size,
            max_rows_per_page=self.max_rows_per_page,
            data_page_version=self.data_page_version,
            # the page statistics go to the page index instead of every page header,
            # so the page headers carry no statistics, like parquet-rs's and
            # parquet-java's, and the same pages can have the same bytes
            write_page_index=True,
            column_encoding=self._column_encoding(src.schema) or None,
        )

    def _column_encoding(self, schema: pa.Schema) -> dict[str, str]:
        """The writer version 2.0 encodings of the leaf columns, with v2 data pages
        without dictionary encoding: parquet-cpp would keep PLAIN.

        The leaf columns and their physical types are read from an empty file of the
        schema.
        """
        if self.data_page_version != "2.0" or self.use_dictionary:
            return {}
        empty = pa.BufferOutputStream()
        pq.write_table(schema.empty_table(), empty)
        columns = pq.ParquetFile(pa.BufferReader(empty.getvalue())).schema
        return {
            column.path: V2_ENCODINGS[column.physical_type]
            for column in columns
            if column.physical_type in V2_ENCODINGS
        }


@dataclass(frozen=True)
class ParquetRs(Parquet):
    """parquet-rs, through de's Rust extension.

    Tables are handed over in-process through the Arrow C stream interface, so the
    arrays arrive as they are: sliced, with their dictionaries, and with null list
    entries spanning child values. Each record batch is a write call.
    """

    name = "parquet-rs"

    def _write(self, src: pa.Table | Path, dest: Path) -> None:
        if isinstance(src, Path):
            rewrite_to_parquet_rs(str(src), str(dest), self)
            return
        if self.batch_size is None:
            # a single write call, a single chunk keeps its slice
            if any(column.num_chunks > 1 for column in src.columns):
                src = src.combine_chunks()
            batches = src.to_batches()
        else:
            size = self.batch_size
            batches = [
                batch
                for offset in range(0, len(src), size)
                for batch in src.slice(offset, size).combine_chunks().to_batches()
            ]
        reader = pa.RecordBatchReader.from_batches(src.schema, batches)
        write_to_parquet_rs(reader, str(dest), self)


@dataclass(frozen=True)
class ParquetJava(Parquet):
    """parquet-java, through the java/ParquetRewrite.java program.

    PARQUET_JAVA_CLASSPATH must hold parquet-hadoop and its dependencies.
    parquet-java writes record by record, there are no write calls to batch,
    so streaming isn't supported.
    """

    name = "parquet-java"
    streaming = False
    arrow_input = False

    def _rewrite(self, src: Path, dest: Path) -> None:
        [error] = self._run([(src, dest, self._options())])
        if error is not None:
            raise RuntimeError(error)

    @classmethod
    def write_many(
        cls,
        jobs: Sequence[tuple[Parquet, str, pa.Table, Path]],
        max_workers: Optional[int] = None,
    ) -> list[Path | BaseException]:
        """Write the jobs like `Parquet.write_many`, with a JVM per group of jobs.

        Each table is written to a temporary source file once, and the jobs are split
        into max_workers groups (4 by default) written by parallel JVMs.
        """
        with tempfile.TemporaryDirectory() as tmp:
            sources: dict[int, Path] = {}
            for _, _, table, _ in jobs:
                if id(table) not in sources:
                    sources[id(table)] = Path(tmp) / f"{len(sources)}.parquet"
                    pq.write_table(table, sources[id(table)])
            rewrites = [
                (
                    sources[id(table)],
                    fmt.derive_path(name, directory),
                    fmt._options(),
                )
                for fmt, name, table, directory in jobs
            ]
            groups = max_workers or min(4, os.cpu_count() or 1)
            size = max(1, -(-len(rewrites) // groups))
            with ThreadPoolExecutor(groups) as executor:
                chunks = [rewrites[i : i + size] for i in range(0, len(rewrites), size)]
                errors = [e for chunk in executor.map(cls._run, chunks) for e in chunk]

        results: list[Path | BaseException] = []
        for (fmt, _, table, _), (_, dest, _), error in zip(jobs, rewrites, errors):
            if error is not None:
                results.append(RuntimeError(error))
                continue
            try:
                fmt._check(table, dest)
                results.append(dest)
            except Exception as e:
                results.append(e)
        return results

    @staticmethod
    def _run(rewrites: list[tuple[Path, Path, list[str]]]) -> list[Optional[str]]:
        """Run the (src, dest, options) rewrites in one JVM, the error of each or None."""
        classpath = os.environ.get("PARQUET_JAVA_CLASSPATH")
        if not classpath:
            raise RuntimeError("set PARQUET_JAVA_CLASSPATH to use parquet-java")
        program = Path(__file__).parent / "java" / "ParquetRewrite.java"
        jobs = "".join(
            "\t".join([str(src), str(dest), *options]) + "\n"
            for src, dest, options in rewrites
        )
        try:
            result = subprocess.run(
                # only errors are logged, so stderr holds just the exception
                ["java", "-Dorg.slf4j.simpleLogger.defaultLogLevel=error"]
                + ["-cp", classpath, str(program)],
                input=jobs,
                capture_output=True,
                text=True,
            )
        except OSError as e:
            return [f"parquet-java failed: {e}"] * len(rewrites)
        outcomes = {}
        for line in result.stdout.splitlines():
            status, dest, *message = line.split("\t", 2)
            if status in ("ok", "error"):
                outcomes[dest] = (
                    None if status == "ok" else f"parquet-java failed: {message[0]}"
                )
        # the jobs a crashed JVM didn't get to fail with its stderr
        crashed = f"parquet-java failed: {result.stderr}"
        return [outcomes.get(str(dest), crashed) for _, dest, _ in rewrites]


@dataclass(frozen=True)
class ParquetDuckDB(Parquet):
    """DuckDB, through a COPY statement run by the binary at DUCKDB_BINARY.

    A COPY has no write calls to batch, so streaming isn't supported. DuckDB's
    pages have no row limit, only NO_LIMIT is accepted for max_rows_per_page,
    and NO_LIMIT for data_page_size keeps DuckDB's own 100MiB page limit. Its
    pages are always v1, PARQUET_VERSION V2 only changes the encodings.

    Row groups are rounded up to whole vectors of 2048 rows, and a source row
    group less than two vectors short of row_group_size is kept as it is, so
    small source row groups show through (the benchmark's sources have 1Mi).
    """

    name = "duckdb"
    streaming = False
    page_row_limit = False
    data_page_v2 = False
    arrow_input = False

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
