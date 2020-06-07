from pathlib import Path
from typing import Union, Optional, Mapping, Any, Collection, TypeVar

from loguru import logger

import legdb
from legdb.step_builder import StepBuilder
from lightning_conceptnet.database import Concept, Assertion, get_db_path, LightningConceptNetDatabase, Transaction


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
        database_size = 2 ** 28
        config.setdefault("map_size", database_size)
        path = get_db_path(path_hint=path_hint, db_open_mode=db_open_mode)
        self._db = LightningConceptNetDatabase(path=path, db_open_mode=db_open_mode, config=config)

    @property
    def _read_transaction(self) -> Transaction:
        return self._db.read_transaction

    @property
    def concept(self):
        return StepBuilder(
            database=self._db,
            node_cls=Concept,
            edge_cls=Assertion,
            txn=self._read_transaction,
        ).source(Concept)

    @property
    def assertion(self):
        return StepBuilder(
            database=self._db,
            node_cls=Concept,
            edge_cls=Assertion,
            txn=self._read_transaction,
        ).source(Assertion)

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
