from pathlib import Path
from typing import Union, Optional, Mapping, Any, Generator, Collection, TypeVar, Type, Callable

from loguru import logger

import legdb
from lightning_conceptnet.database import Concept, Assertion, get_db_path, LightningConceptNetDatabase, Transaction
from legdb import DbOpenMode


T = TypeVar("T", Concept, Assertion)


class LightningConceptNet:
    def __init__(
            self,
            path_hint: Union[Path, str],
            db_open_mode: legdb.DbOpenMode = legdb.DbOpenMode.READ_WRITE,
            config: Optional[Mapping[str, Any]] = None,
    ):
        if config is None:
            config = {}
        database_size = 2 ** 35
        config.setdefault("map_size", database_size)
        path = get_db_path(path_hint=path_hint, db_open_mode=db_open_mode)
        self._db = LightningConceptNetDatabase(path=path, db_open_mode=db_open_mode, config=config)

    def seek_one(
            self,
            entity: Union[Concept, Assertion],
            txn: Optional[Transaction] = None,
    ) -> Union[Concept, Assertion]:
        return self._db.seek_one(entity=entity, txn=txn)

    def seek(
            self,
            entity: Union[Concept, Assertion],
            txn: Optional[Transaction] = None,
    ) -> Generator[Union[Concept, Assertion], None, None]:
        yield from self._db.seek(entity=entity, txn=txn)

    def range(
            self,
            lower: Optional[Union[Concept, Assertion]] = None,
            upper: Optional[Union[Concept, Assertion]] = None,
            index_name: Optional[str] = None,
            inclusive: bool = True) -> Generator[Union[Concept, Assertion], None, None]:
        yield from self._db.range(lower=lower, upper=upper, index_name=index_name, inclusive=inclusive)

    def find(
            self,
            what: Type[T],
            index_name: Optional[str] = None,
            expression: Optional[Callable[[T], bool]] = None,
            txn: Optional[Transaction] = None,
    ) -> Generator[T, None, None]:
        yield from self._db.find(what=what, index_name=index_name, expression=expression, txn=txn)

    def load(
            self,
            dump_path: Union[Path, str],
            edge_count: Optional[int] = None,
            languages: Optional[Collection[str]] = None,
            compress: bool = True,
    ):
        self._db.load(dump_path=dump_path, edge_count=edge_count, languages=languages, compress=compress)


def build(
        database_path_hint: Union[Path, str],
        dump_path: Union[Path, str],
        edge_count: int = None,
        languages: Optional[Collection[str]] = None,
        compress: bool = True,
):
    import stackprinter
    stackprinter.set_excepthook(style='darkbg2')
    logger.info("Create lightning-conceptnet database")
    config = {"readahead": False, "subdir": False}
    lcn = LightningConceptNet(database_path_hint, db_open_mode=legdb.DbOpenMode.CREATE, config=config)
    lcn.load(dump_path=dump_path, edge_count=edge_count, languages=languages, compress=compress)
