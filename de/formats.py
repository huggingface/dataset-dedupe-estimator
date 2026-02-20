from dataclasses import dataclass, asdict
from pathlib import Path
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
from typing import Optional
import tempfile
from de.core import rewrite_to_parquet_rs


@dataclass(frozen=True)
class CdcParams:
    min_chunk_size: int
    max_chunk_size: int
    norm_level: int


@dataclass(frozen=True)
class FileFormat:
    """Base class for file format handlers."""

    @property
    def name(self) -> str:
        return self.__class__.__name__.replace("Format", "")

    @property
    def suffix(self) -> str:
        raise NotImplementedError

    @property
    def paramstem(self) -> str:
        raise NotImplementedError

    def derive_path(self, name: str, directory: Path) -> Path:
        """Construct an output path under directory using name."""
        stem = f"{name}-{self.paramstem}" if self.paramstem else name
        return directory / f"{stem}.{self.suffix}"

    def write(self, name: str, src: pa.Table | Path, directory: Path, **kwargs) -> Path:
        """Write a PyArrow table or rewrite a source parquet file to the derived path."""
        raise NotImplementedError


@dataclass(frozen=True)
class ParquetCpp(FileFormat):
    suffix = "parquet"
    use_cdc: bool | CdcParams
    compression: Optional[str] = None
    use_dictionary: bool = True
    data_page_size: Optional[int] = None
    row_group_size: Optional[int] = None

    @property
    def paramstem(self) -> str:
        parts = []
        if self.compression is not None:
            parts.append(self.compression)
        if self.use_cdc:
            parts.append("cdc")
        return "-".join(parts)

    def _write_kwargs(self) -> dict:
        kwargs: dict = {
            "use_dictionary": self.use_dictionary,
            "compression": self.compression,
        }
        if isinstance(self.use_cdc, CdcParams):
            kwargs["use_content_defined_chunking"] = asdict(self.use_cdc)
        else:
            kwargs["use_content_defined_chunking"] = self.use_cdc

        if self.data_page_size is not None:
            kwargs["data_page_size"] = self.data_page_size
        return kwargs

    def write(
        self,
        name: str,
        src: pa.Table | Path,
        directory: Path,
        block_size: int = 1024 * 1024,
        sanity_check: bool = True,
    ) -> Path:
        dest = self.derive_path(name, directory)
        kwargs = self._write_kwargs()
        if isinstance(src, Path):
            with pq.ParquetFile(src) as pf:
                schema = pf.schema.to_arrow_schema()
                writer = pq.ParquetWriter(dest, schema, **kwargs)
                for batch in pf.iter_batches(batch_size=block_size):
                    writer.write(batch, row_group_size=self.row_group_size)
                writer.close()
            if sanity_check:
                src_meta = pq.ParquetFile(src).metadata
                dst_meta = pq.ParquetFile(dest).metadata
                assert src_meta.num_rows == dst_meta.num_rows
                assert (
                    src_meta.schema.to_arrow_schema()
                    == dst_meta.schema.to_arrow_schema()
                )
        else:
            if self.row_group_size is not None:
                kwargs["row_group_size"] = self.row_group_size
            pq.write_table(src, dest, **kwargs)
            if sanity_check:
                assert src.equals(pq.read_table(dest))
        return dest


@dataclass(frozen=True)
class ParquetRs(FileFormat):
    suffix = "parquet"
    use_cdc: bool | CdcParams
    compression: Optional[str] = None

    @property
    def paramstem(self) -> str:
        parts = []
        if self.compression is not None:
            parts.append(self.compression)
        if self.use_cdc:
            parts.append("cdc")
        return "-".join(parts)

    def write(self, name: str, src: pa.Table | Path, directory: Path, **kwargs) -> Path:
        if isinstance(self.use_cdc, CdcParams):
            raise ValueError("CDC parameters are not supported by ParquetRs format.")

        use_cdc = bool(self.use_cdc)
        dest = self.derive_path(name, directory)
        if isinstance(src, Path):
            rewrite_to_parquet_rs(
                str(src), str(dest), cdc=use_cdc, compression=self.compression
            )
        else:
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                pq.write_table(src, tmp.name)
                rewrite_to_parquet_rs(
                    tmp.name, str(dest), cdc=use_cdc, compression=self.compression
                )
        return dest


@dataclass(frozen=True)
class JsonLines(FileFormat):
    suffix = "jsonlines"
    compression: Optional[str] = None

    @property
    def paramstem(self) -> str:
        return self.compression or ""

    def write(self, name: str, src: pa.Table | Path, directory: Path, **kwargs) -> Path:
        path = self.derive_path(name, directory)
        table = pq.read_table(src) if isinstance(src, Path) else src
        table.to_pandas().to_json(path, orient="records", lines=True, compression=self.compression)
        return path


@dataclass(frozen=True)
class Sqlite(FileFormat):
    suffix = "sqlite"
    compression: Optional[str] = None

    @property
    def paramstem(self) -> str:
        return self.compression or ""

    def write(self, name: str, src: pa.Table | Path, directory: Path, **kwargs) -> Path:
        path = self.derive_path(name, directory)
        table = pq.read_table(src) if isinstance(src, Path) else src
        con = sqlite3.connect(path)
        table.to_pandas().to_sql("table", con, if_exists="replace", index=False)
        return path
