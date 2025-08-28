# smb_sas7bdat_streamer.py
# -----------------------------------------------------------------------------
# Purpose:
#   Stream large .sas7bdat files directly from an SMB share using pysmb, with a
#   seekable wrapper that allows parsers to perform random access reads.
#
# Notebook-friendly:
#   Import this file in a Jupyter notebook or run functions directly after
#   defining your connection details. See the "EXAMPLE USAGE" section below.
#
# Where to insert YOUR values (as requested):
#   - server_ip   = "10.211.146.11"        # <--- your server IP (from your note)
#   - server_name = "SE152662"             # <--- your server name (from your note)
#   - share_name  = "drop place"           # <--- your SMB share (spaces OK)
#   - file_path   = "abc/efg/file.sas7bdat" # <--- path inside the share
#
#   Also set your SMB credentials:
#     username = "YOUR_USERNAME"
#     password = "YOUR_PASSWORD"
#     domain   = "WORKGROUP"  # or your AD domain, e.g., "CORP"
#
# Prereqs:
#   pip install pysmb sas7bdat pandas
#
# -----------------------------------------------------------------------------

from __future__ import annotations

import io
import logging
from typing import Generator, Iterable, Optional, List

from smb.SMBConnection import SMBConnection
try:
    from smb.smb_constants import FILE_READ_DATA as SMB_FILE_READ_DATA
except Exception:
    SMB_FILE_READ_DATA = 0x00000001  # FILE_READ_DATA fallback
from sas7bdat import SAS7BDAT
import pandas as pd


logger = logging.getLogger(__name__)
if not logger.handlers:
    # Basic logging setup (safe if imported multiple times)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )


def _norm_path(path: str) -> str:
    """
    Normalize an SMB path to always start with a single forward slash,
    use forward slashes only, and avoid trailing slashes (except root).
    """
    p = path.replace("\\", "/")
    p = "/" + p.lstrip("/")
    if len(p) > 1 and p.endswith("/"):
        p = p[:-1]
    return p


def connect_smb(
    server_ip: str,
    server_name: str,
    username: str,
    password: str,
    domain: str = "WORKGROUP",
    port: int = 445,
    my_name: str = "client-node",
    use_ntlm_v2: bool = True,
    is_direct_tcp: bool = True,
) -> SMBConnection:
    """
    Establish and return an SMBConnection.

    Parameters
    ----------
    server_ip : str
        The SMB server IP address. e.g., "10.211.146.11".
    server_name : str
        The server's NetBIOS/DNS name. e.g., "SE152662".
        (Used in SMB negotiation and may affect Kerberos/NTLM behavior.)
    username : str
        SMB username.
    password : str
        SMB password.
    domain : str, default "WORKGROUP"
        Your AD domain or "WORKGROUP".
    port : int, default 445
        SMB2/3 direct TCP port (445). Use 139 only for legacy NetBIOS.
    my_name : str, default "client-node"
        Local client machine name (arbitrary label).
    use_ntlm_v2 : bool, default True
        Use NTLMv2 for authentication (recommended).
    is_direct_tcp : bool, default True
        True => direct TCP (port 445). False => NetBIOS (port 139).

    Returns
    -------
    SMBConnection
        A connected SMBConnection instance.

    Raises
    ------
    AssertionError
        If the connection cannot be established.
    """
    conn = SMBConnection(
        username=username,
        password=password,
        my_name=my_name,
        remote_name=server_name,
        domain=domain,
        use_ntlm_v2=use_ntlm_v2,
        is_direct_tcp=is_direct_tcp,
    )
    ok = conn.connect(server_ip, port)
    if not ok:
        raise ConnectionError(f"Failed to connect to SMB server {server_name}@{server_ip}:{port}")
    logger.info("Connected to SMB server %s (%s)", server_name, server_ip)
    return conn


class SeekableSMBFile(io.RawIOBase):
    """
    A seekable, readable file-like object backed by pysmb random-access I/O.

    This wrapper exposes a standard Python file interface (`read`, `seek`, `tell`),
    mapping each call to SMB offset reads with an open file handle on the server.
    It is suitable for libraries that need a binary, seekable stream (like
    `sas7bdat` or `pandas.read_sas(..., format='sas7bdat')`).

    Example
    -------
    >>> conn = connect_smb("10.211.146.11", "SE152662", "user", "pass")
    >>> fh = SeekableSMBFile(conn, "drop place", "abc/efg/file.sas7bdat")
    >>> data = fh.read(4096)  # bytes
    >>> fh.seek(0)
    >>> fh.close()
    """

    def __init__(
        self,
        conn: SMBConnection,
        share: str,
        path: str,
        file_size: Optional[int] = None
    ) -> None:
        """
        Parameters
        ----------
        conn : SMBConnection
            An established SMBConnection.
        share : str
            The SMB share name. e.g., 'drop place'  (spaces are OK).
        path : str
            Path within the share to the file, using forward slashes.
            e.g., 'abc/efg/file.sas7bdat' or '/abc/efg/file.sas7bdat'
        file_size : Optional[int]
            If known, pass the file size to enable size=-1 reads and SEEK_END.
            If None, size will be discovered via listPath() on first use.
        """
        if not isinstance(conn, SMBConnection):
            raise TypeError("conn must be an SMBConnection")
        self.conn = conn
        self.share = share
        self.path = _norm_path(path)
        self._fid = self.conn.openFile(self.share, self.path, desired_access=SMB_FILE_READ_DATA)
        self._pos = 0
        self._size = file_size
        if self._size is None:
            # Discover file size (directory listing)
            d, b = self._split_dir_base(self.path)
            for f in self.conn.listPath(self.share, d):
                if f.filename == b:
                    self._size = f.file_size
                    break

    # ---- Python IO protocol ----
    def readable(self) -> bool:
        """Return True to mark this as a readable stream."""
        return True

    def seekable(self) -> bool:
        """Return True to mark this as a seekable stream."""
        return True

    def tell(self) -> int:
        """Return current stream position."""
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """
        Move the current stream position.

        whence:
          - io.SEEK_SET (0): from start of file
          - io.SEEK_CUR (1): from current position
          - io.SEEK_END (2): from end of file
        """
        if whence == io.SEEK_SET:
            newpos = offset
        elif whence == io.SEEK_CUR:
            newpos = self._pos + offset
        elif whence == io.SEEK_END:
            if self._size is None:
                raise io.UnsupportedOperation("unknown file size; cannot SEEK_END")
            newpos = self._size + offset
        else:
            raise ValueError("invalid whence")
        if newpos < 0:
            raise ValueError("negative seek position")
        self._pos = newpos
        return self._pos

    def read(self, size: int = -1) -> bytes:
        """
        Read up to 'size' bytes from the current position.
        If size < 0 and the file size is known, read to EOF.
        Returns a 'bytes' object (may be shorter than requested size at EOF).
        """
        if size is None or size < 0:
            if self._size is None:
                raise io.UnsupportedOperation("size=-1 not supported without known file size")
            size = max(0, self._size - self._pos)
        if size == 0:
            return b""
        data: bytes = self.conn.read(self.share, self._fid, self._pos, size)
        self._pos += len(data)
        return data

    def readinto(self, b: bytearray) -> int:
        """
        Read bytes directly into a pre-allocated, writable bytes-like object.
        Returns the number of bytes read. Useful for libraries that call readinto().
        """
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def close(self) -> None:
        """Close the remote file handle and release resources."""
        try:
            if getattr(self, "_fid", None) is not None:
                self.conn.closeFile(self.share, self._fid)
                self._fid = None
        finally:
            super().close()

    # ---- helpers ----
    @staticmethod
    def _split_dir_base(path: str) -> (str, str):
        # path is normalized to start with '/'
        if path == "/":
            return "/", ""
        if "/" not in path[1:]:
            return "/", path.lstrip("/")
        d, b = path.rsplit("/", 1)
        if not d:
            d = "/"
        return d, b


def iter_sas_chunks_from_smb(
    conn: SMBConnection,
    share: str,
    path: str,
    chunksize_rows: int = 50_000,
    encoding: Optional[str] = "latin-1",
) -> Generator[pd.DataFrame, None, None]:
    """
    Stream a remote .sas7bdat file from SMB and yield pandas DataFrames in row-chunks.

    Parameters
    ----------
    conn : SMBConnection
        An active SMB connection (use connect_smb).
    share : str
        The SMB share name (e.g., 'drop place').
    path : str
        File path within the share (e.g., 'abc/efg/file.sas7bdat').
    chunksize_rows : int, default 50_000
        Number of rows per emitted DataFrame batch.
        Tune this based on your memory and the number of columns (e.g., 1,021).
    encoding : Optional[str], default "latin-1"
        Text encoding for character columns if needed.

    Yields
    ------
    pandas.DataFrame
        DataFrame batches of up to chunksize_rows rows.
    """
    smb_f = SeekableSMBFile(conn, share, path)
    try:
        with SAS7BDAT("ignored.sas7bdat", fh=smb_f, encoding=encoding) as rdr:
            # Get column names if provided by the parser
            colnames: Optional[List[str]] = None
            try:
                colnames = [c.name for c in getattr(rdr, "columns", [])] or None
            except Exception:
                colnames = None

            batch = []
            for row in rdr:  # row is a sequence of values
                batch.append(row)
                if len(batch) >= chunksize_rows:
                    df = pd.DataFrame(batch, columns=colnames)
                    yield df
                    batch.clear()

            if batch:
                df = pd.DataFrame(batch, columns=colnames)
                yield df
    finally:
        smb_f.close()


def head_sas_from_smb(
    conn: SMBConnection,
    share: str,
    path: str,
    n_rows: int = 10,
    encoding: Optional[str] = "latin-1",
) -> pd.DataFrame:
    """
    Convenience function to fetch the first n_rows from a remote .sas7bdat via SMB.

    Parameters
    ----------
    conn : SMBConnection
        An active SMB connection.
    share : str
        The SMB share name.
    path : str
        SMB path to the .sas7bdat file.
    n_rows : int, default 10
        How many rows to return.
    encoding : Optional[str], default "latin-1"
        Text encoding for character columns if needed.

    Returns
    -------
    pandas.DataFrame
        The first n_rows of the dataset.
    """
    smb_f = SeekableSMBFile(conn, share, path)
    try:
        with SAS7BDAT("ignored.sas7bdat", fh=smb_f, encoding=encoding) as rdr:
            rows = []
            colnames: Optional[List[str]] = None
            try:
                colnames = [c.name for c in getattr(rdr, "columns", [])] or None
            except Exception:
                colnames = None

            for i, row in enumerate(rdr):
                rows.append(row)
                if i + 1 >= n_rows:
                    break
            return pd.DataFrame(rows, columns=colnames)
    finally:
        smb_f.close()


# -----------------------------------------------------------------------------
# EXAMPLE USAGE (copy/paste into a Jupyter cell and fill in your credentials)
# -----------------------------------------------------------------------------
# Placeholders below show exactly where to add YOUR values:
#
# server_ip   = "10.211.146.11"         # <--- from your message
# server_name = "SE152662"              # <--- from your message
# share_name  = "drop place"            # <--- from your message (spaces OK)
# file_path   = "abc/efg/file.sas7bdat" # <--- from your message
#
# username = "YOUR_USERNAME"            # <--- add your SMB username here
# password = "YOUR_PASSWORD"            # <--- add your SMB password here
# domain   = "WORKGROUP"                # or your AD domain, e.g., "CORP"
#
# from smb_sas7bdat_streamer import connect_smb, iter_sas_chunks_from_smb, head_sas_from_smb
#
# conn = connect_smb(server_ip, server_name, username, password, domain=domain)
# # Peek at the first few rows
# preview_df = head_sas_from_smb(conn, share_name, file_path, n_rows=10)
# display(preview_df)
#
# # Stream in chunks (adjust chunksize_rows for memory/width, e.g., 25_000–50_000)
# for df in iter_sas_chunks_from_smb(conn, share_name, file_path, chunksize_rows=25_000):
#     # TODO: transform/validate each chunk here
#     # Example: print shape then break after first chunk
#     print(df.shape)
#     break
#
# NOTE: If using pandas.read_sas instead of sas7bdat, you can do:
#   import pandas as pd
#   smb_f = SeekableSMBFile(conn, share_name, file_path)
#   it = pd.read_sas(smb_f, format="sas7bdat", iterator=True, chunksize=25_000)
#   for df in it:
#       print(df.shape)
#       break
#
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    # This block is safe to ignore in notebooks; it's for basic CLI testing.
    import os
    print("smb_sas7bdat_streamer module loaded. For notebook usage, import the functions.")
