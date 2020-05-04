import errno
import os
from pathlib import Path


def _raise_path_related_error(exception_cls, error_no: int, path: Path) -> None:
    raise exception_cls(error_no, os.strerror(error_no), str(path))


def raise_file_exists_error(path: Path) -> None:
    _raise_path_related_error(FileExistsError, errno.EEXIST, path)


def raise_file_not_found_error(path: Path) -> None:
    _raise_path_related_error(FileNotFoundError, errno.ENOENT, path)


def raise_is_a_directory_error(path: Path) -> None:
    _raise_path_related_error(IsADirectoryError, errno.EISDIR, path)


def raise_not_a_directory_error(path: Path) -> None:
    _raise_path_related_error(NotADirectoryError, errno.ENOTDIR, path)