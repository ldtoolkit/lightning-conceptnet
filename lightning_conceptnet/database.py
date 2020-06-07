from __future__ import annotations

from multiprocessing import Process
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Union, Optional, Generator, Tuple, Mapping, Any, List, Collection, Dict, Type, NamedTuple
import csv
import re
import sys
import os

import zmq
from loguru import logger
from tqdm import tqdm
import legdb
import legdb.database
import orjson

from lightning_conceptnet.database_creation_worker import DatabaseCreationWorker
from lightning_conceptnet.exceptions import (
    raise_file_exists_error, raise_file_not_found_error, raise_is_a_directory_error)
from lightning_conceptnet.nodes import standardized_concept_uri
from lightning_conceptnet.uri import uri_to_label, split_uri, get_uri_language
from pynndb import write_transaction


@dataclass
class Concept(legdb.Node):
    label: Optional[str] = None
    language: Optional[str] = None
    sense: Optional[List[str]] = None
    external_url: Optional[List[str]] = None

    @property
    def sense_label(self) -> str:
        result = self.sense[0]
        if len(self.sense) > 1 and self.sense[1] in ("wp", "wn"):
            result += ", " + self.sense[-1]
        return result

    @classmethod
    def from_uri(cls: Type[Concept], uri: str, database: Optional[LightningConceptNetDatabase] = None) -> Concept:
        label = uri_to_label(uri)
        language = get_uri_language(uri)
        pieces = split_uri(uri)
        sense = pieces[3:]
        return cls(database=database, label=label, language=language, sense=sense)

    @property
    def uri(self) -> str:
        return standardized_concept_uri(self.language, self.label, *self.sense)

    def __str__(self) -> str:
        return self.uri


@dataclass
class Assertion(legdb.Edge):
    relation: Optional[str] = None
    sources: Optional[List[Dict[str, str]]] = None
    dataset: Optional[str] = None
    license: Optional[str] = None
    weight: Optional[float] = None
    surfaceEnd: Optional[str] = None
    surfaceStart: Optional[str] = None
    surfaceText: Optional[str] = None
    _node_class = Concept


CSVLineTuple = Tuple[str, str, str, str]
CSVLineTupleGenerator = Generator[CSVLineTuple, None, None]
Transaction = legdb.Transaction


class EdgeTuple(NamedTuple):
    assertion: Assertion
    start_concept: Concept
    end_concept: Optional[Concept]
    external_url: str


EdgeTupleGenerator = Generator[EdgeTuple, None, None]


class AssertionIndexBy(Enum):
    relation = "by_relation"


class ConceptIndexBy(Enum):
    label = "by_label"
    language = "by_language"
    language_label = "by_language_label"
    language_label_sense = "by_language_label_sense"
    sense = "by_sense"


def _construct_db_path(parent_dir_path: Path) -> Path:
    db_base_name = "conceptnet.db"
    return parent_dir_path / db_base_name


def _get_db_path_for_db_creation(path_hint: Path) -> Path:
    if not path_hint.exists():
        return path_hint
    elif path_hint.is_dir():
        path_to_check = _construct_db_path(parent_dir_path=path_hint)
        if path_to_check.exists():
            raise_file_exists_error(path_to_check)
        else:
            return path_to_check
    else:
        raise_file_exists_error(path_hint)


def _check_existing_db_path_for_db_read(existing_path: Path) -> bool:
    if existing_path.is_file():
        return True
    else:
        raise_is_a_directory_error(existing_path)


def _get_db_path_for_db_read_write(path_hint: Path) -> Path:
    if path_hint.is_dir():
        path_to_check = _construct_db_path(parent_dir_path=path_hint)
        if path_to_check.exists() and _check_existing_db_path_for_db_read(path_to_check):
            return path_to_check
        else:
            return path_hint
    elif path_hint.is_file():
        _check_existing_db_path_for_db_read(path_hint)
        return path_hint
    else:
        raise_file_not_found_error(path_hint)


def get_db_path(path_hint: Union[Path, str], db_open_mode: legdb.DbOpenMode) -> Path:
    path_to_check = Path(path_hint).expanduser().resolve()
    if db_open_mode == legdb.DbOpenMode.CREATE:
        return _get_db_path_for_db_creation(path_hint=path_to_check)
    elif db_open_mode == legdb.DbOpenMode.READ_WRITE:
        return _get_db_path_for_db_read_write(path_hint=path_to_check)
    else:
        raise ValueError(f"db_open_mode not supported: '{db_open_mode}")


def _to_snake_case(s: str) -> str:
    regex = re.compile('((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))')
    return regex.sub(r'_\1', s).lower()


ZSTD_COMPRESSION_DICT_SIZE = 2**27  # 128 MiB
ZSTD_COMPRESSION_LEVEL = 3
ZSTD_SAMPLES_SIZE = 2**27  # 128 MiB


class LightningConceptNetDatabase(legdb.database.Database):
    def __init__(
            self,
            path: Union[Path, str],
            db_open_mode: legdb.DbOpenMode = legdb.DbOpenMode.READ_WRITE,
            config: Optional[Mapping[str, Any]] = None,
            n_jobs: int = len(os.sched_getaffinity(0)),
    ):
        if config is None:
            config = {}
        config.setdefault("readahead", False)
        config.setdefault("subdir", False)
        super().__init__(path=path, db_open_mode=db_open_mode, config=config, n_jobs=n_jobs)
        indexes = [
            {
                "what": legdb.Node,
                "name": ConceptIndexBy.language_label_sense.value,
                "attrs": ["language", "label", "sense"],
                "func": "!{language}|{label}|{sense}",
                "duplicates": False,
            },
            {
                "what": legdb.Node,
                "name": ConceptIndexBy.language_label.value,
                "attrs": ["language", "label"],
                "func": "!{language}|{label}",
                "duplicates": True,
            },
            {
                "what": legdb.Node,
                "name": ConceptIndexBy.label.value,
                "attrs": ["label"],
                "func": "{label}",
                "duplicates": True,
            },
            {
                "what": legdb.Node,
                "name": ConceptIndexBy.language.value,
                "attrs": ["language"],
                "func": "{language}",
                "duplicates": True,
            },
            {
                "what": legdb.Node,
                "name": ConceptIndexBy.sense.value,
                "attrs": ["sense"],
                "func": "{sense}",
                "duplicates": True,
            },
            {
                "what": legdb.Edge,
                "name": AssertionIndexBy.relation.value,
                "attrs": ["relation"],
                "func": "{relation}",
                "duplicates": True,
            },
        ]
        for index in indexes:
            self.ensure_index(**index)
        self._training_samples = {
            Concept: [],
            Assertion: [],
        }
        self._training_samples_size = {
            Concept: 0,
            Assertion: 0,
        }
        self._training_sample_period = {
            Concept: 1,
            Assertion: 1,
        }
        self._entity_count = {
            Concept: 0,
            Assertion: 0,
        }
        self._worker_processes = []

    def compress(
            self,
            what: Type[Union[legdb.Node, legdb.Edge]],
            training_samples: List[bytes],
            compression_type: legdb.CompressionType = legdb.CompressionType.ZSTD,
            compression_level: int = ZSTD_COMPRESSION_LEVEL,
            compression_dict_size: int = ZSTD_COMPRESSION_DICT_SIZE,
            threads: int = -1,
            vacuum: bool = True,
            txn: Optional[legdb.Transaction] = None,
    ):
        super().compress(
            what=what,
            training_samples=training_samples,
            compression_type=compression_type,
            compression_level=compression_level,
            training_dict_size=compression_dict_size,
            threads=threads,
            txn=txn,
        )

    @staticmethod
    def _edge_parts(dump_path: Path, count: Optional[int] = None) -> CSVLineTupleGenerator:
        with open(str(dump_path), newline="") as f:
            reader = csv.reader(f, delimiter="\t")
            for i, row in enumerate(reader):
                if i == count:
                    break
                yield row[1:5]

    def edge_from_edge_parts(self, edge_parts: CSVLineTuple, languages: Optional[Collection[str]] = None) -> EdgeTuple:
        relation_uri, start_uri, end_uri, edge_data = edge_parts
        relation = relation_uri.replace("/r/", "", 1)
        is_end_uri_external_url = relation == "ExternalURL"
        start_concept = Concept.from_uri(start_uri)
        if is_end_uri_external_url:
            end_concept = None
            external_url = end_uri
        else:
            end_concept = Concept.from_uri(end_uri)
            external_url = None
        language_match = (
                not languages or
                (start_concept.language in languages and
                 (end_concept is None or end_concept.language in languages))
        )
        if language_match:
            edge_data = orjson.loads(edge_data)
            assertion = Assertion(relation=relation, **edge_data)
            return EdgeTuple(
                assertion=assertion,
                start_concept=start_concept,
                end_concept=end_concept,
                external_url=external_url,
            )

    def _initial_save_concept(self, concept: Concept, txn: Transaction) -> str:
        result = self.node_table.seek_one(ConceptIndexBy.language_label_sense.value, concept.to_doc(), txn=txn)
        if result is None:
            oid = self.save(concept, return_oid=True, txn=txn)
        else:
            oid = result.key
        return oid

    def _add_external_url_to_concept(self, concept_oid: str, url: str, txn: Transaction) -> None:
        concept = self.get(Concept, concept_oid.encode(), txn=txn)
        if concept.external_url is None:
            concept.external_url = []
        concept.external_url.append(url)
        self.save(concept, txn=txn)

    def _append_sample(self, entity: Union[legdb.Node, legdb.Edge], edge_count: int):
        cls = type(entity)
        self._entity_count[cls] += 1
        if self._entity_count[cls] % self._training_sample_period[cls] == 0:
            sample = orjson.dumps(entity.to_doc().doc)
            self._training_samples[cls].append(sample)
            self._training_samples_size[cls] += sys.getsizeof(sample)
            period_recalculation_period = 128
            if self._entity_count[cls] % period_recalculation_period == 0:
                mean_sample_size = self._training_samples_size[cls] // len(self._training_samples[cls])
                self._training_sample_period[cls] = max(
                    1,
                    mean_sample_size * (edge_count - self._entity_count[Assertion]) //
                    (ZSTD_SAMPLES_SIZE - self._training_samples_size[cls])
                )

    def train_compression(
            self,
            dump_path: Union[Path, str],
            edge_count: Optional[int] = None,
            languages: Optional[Collection[str]] = None,
    ):
        @write_transaction
        def compress(db, txn=None):
            self.compress(legdb.Node, training_samples=self._training_samples[Concept], txn=txn)
            self.compress(legdb.Edge, training_samples=self._training_samples[Assertion], txn=txn)

        logger.info("Collect samples for training compression")
        edge_parts = enumerate(self._edge_parts(dump_path=dump_path, count=edge_count))
        for _, edge_parts in tqdm(edge_parts, unit=' edges', total=edge_count):
            edge = self.edge_from_edge_parts(edge_parts, languages=languages)
            self._append_sample(edge.start_concept, edge_count=edge_count)
            if edge.end_concept is not None:
                self._append_sample(edge.end_concept, edge_count=edge_count)
            self._append_sample(edge.assertion, edge_count=edge_count)
        logger.info("Train compression")
        compress(self._db)

    def save_edge_concepts(self, edge: EdgeTuple, txn: Transaction):
        start_concept_id = self._initial_save_concept(edge.start_concept, txn=txn)
        if edge.external_url is not None:
            self._add_external_url_to_concept(concept_oid=start_concept_id, url=edge.external_url, txn=txn)
        elif edge.end_concept is not None:
            end_concept_id = self._initial_save_concept(edge.end_concept, txn=txn)
            edge.assertion.start_id = start_concept_id
            edge.assertion.end_id = end_concept_id

    def _start_database_creation_workers(self, languages: Optional[Collection[str]]):
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.PUSH)
        address = "tcp://127.0.0.1"
        port = self._socket.bind_to_random_port(address)
        for worker_i in range(len(os.sched_getaffinity(0)) - 1):
            worker = DatabaseCreationWorker(f"{address}:{port}", database_path=self._path, config=self._config)
            worker_process = Process(target=worker.run, args=(languages, ))
            worker_process.start()
            self._worker_processes.append(worker_process)

    def _stop_database_creation_workers(self):
        for worker_process in self._worker_processes:
            worker_process.terminate()

    @staticmethod
    def line_count(file_path):
        f = open(file_path, "rb")
        result = 0
        buf_size = 1024 * 1024
        read_f = f.raw.read

        buf = read_f(buf_size)
        while buf:
            result += buf.count(b"\n")
            buf = read_f(buf_size)

        return result

    def load(
            self,
            dump_path: Union[Path, str],
            edge_count: Optional[int] = None,
            languages: Optional[Collection[str]] = None,
            compress: bool = True,
    ) -> None:
        dump_path = Path(dump_path).expanduser().resolve()

        if edge_count is None:
            edge_count = self.line_count(dump_path)

        if compress:
            self.train_compression(dump_path=dump_path, edge_count=edge_count, languages=languages)

        self._start_database_creation_workers(languages=languages)

        logger.info("Load lightning-conceptnet database from dump")

        edge_parts = enumerate(self._edge_parts(dump_path=dump_path, count=edge_count))
        for _, edge_parts in tqdm(edge_parts, unit=' edges', total=edge_count):
            self._socket.send_pyobj(edge_parts)

        self._stop_database_creation_workers()

        logger.info("Lightning-conceptnet database building finished")
