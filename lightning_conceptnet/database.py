from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from subprocess import call
from sys import maxsize
from typing import Union, Optional, Generator, Tuple, Mapping, Any, List, Collection, Dict, Type
import csv
import re
import sys

from loguru import logger
from tqdm import tqdm
import legdb
import legdb.database
import ujson

from lightning_conceptnet.exceptions import (
    raise_file_exists_error, raise_file_not_found_error, raise_is_a_directory_error)


CONCEPTNET_EDGE_COUNT = 34074917


CSVLineTuple = Tuple[str, str, str, str]
CSVLineTupleGenerator = Generator[CSVLineTuple, None, None]
Transaction = legdb.Transaction


@dataclass
class Concept(legdb.Node):
    label: Optional[str] = None
    language: Optional[str] = None
    sense: Optional[str] = None
    external_url: Optional[List[str]] = None

    @classmethod
    def from_uri(cls: Type[Concept], uri: str, db: Optional[LegDBDatabase] = None) -> Concept:
        split_uri = uri.split("/", maxsplit=4)
        language, label = split_uri[2:4]
        sense = split_uri[4] if len(split_uri) == 5 else "-"
        return cls(db=db, label=label, language=language, sense=sense)


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


class AssertionIndexBy(Enum):
    relation = "by_relation"


class ConceptIndexBy(Enum):
    label = "by_label"
    language = "by_language"
    language_label = "by_language_label"
    language_label_sense = "by_language_label_sense"
    sense = "by_sense"


class Relation:
    """Names of non-deprecated relations.

    See: https://github.com/commonsense/conceptnet5/wiki/Relations.
    """

    RELATED_TO = 'related_to'
    FORM_OF = 'form_of'
    IS_A = 'is_a'
    PART_OF = 'part_of'
    HAS_A = 'has_a'
    USED_FOR = 'used_for'
    CAPABLE_OF = 'capable_of'
    AT_LOCATION = 'at_location'
    CAUSES = 'causes'
    HAS_SUBEVENT = 'has_subevent'
    HAS_FIRST_SUBEVENT = 'has_first_subevent'
    HAS_LAST_SUBEVENT = 'has_last_subevent'
    HAS_PREREQUISITE = 'has_prerequisite'
    HAS_PROPERTY = 'has_property'
    MOTIVATED_BY_GOAL = 'motivated_by_goal'
    OBSTRUCTED_BY = 'obstructed_by'
    DESIRES = 'desires'
    CREATED_BY = 'created_by'
    SYNONYM = 'synonym'
    ANTONYM = 'antonym'
    DISTINCT_FROM = 'distinct_from'
    DERIVED_FROM = 'derived_from'
    SYMBOL_OF = 'symbol_of'
    DEFINED_AS = 'defined_as'
    MANNER_OF = 'manner_of'
    LOCATED_NEAR = 'located_near'
    HAS_CONTEXT = 'has_context'
    SIMILAR_TO = 'similar_to'
    ETYMOLOGICALLY_RELATED_TO = 'etymologically_related_to'
    ETYMOLOGICALLY_DERIVED_FROM = 'etymologically_derived_from'
    CAUSES_DESIRE = 'causes_desire'
    MADE_OF = 'made_of'
    RECEIVES_ACTION = 'receives_action'
    EXTERNAL_URL = 'external_url'


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


def _get_db_path_for_db_read(path_hint: Path) -> Path:
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
    if db_open_mode == legdb.DbOpenMode.WRITE:
        return _get_db_path_for_db_creation(path_hint=path_to_check)
    elif db_open_mode == legdb.DbOpenMode.READ:
        return _get_db_path_for_db_read(path_hint=path_to_check)
    else:
        raise ValueError(f"db_open_mode not supported: '{db_open_mode}")


def _to_snake_case(s: str) -> str:
    regex = re.compile('((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))')
    return regex.sub(r'_\1', s).lower()


ZSTD_COMPRESSION_DICT_SIZE = 2**27  # 128 MiB
ZSTD_COMPRESSION_LEVEL = 3
ZSTD_SAMPLES_SIZE = 2**27  # 128 MiB


class LegDBDatabase(legdb.database.Database):
    def __init__(
            self,
            path: Union[Path, str],
            db_open_mode: legdb.DbOpenMode = legdb.DbOpenMode.READ,
            config: Optional[Mapping[str, Any]] = None
    ):
        if config is None:
            config = {}
        config.setdefault("readahead", False)
        config.setdefault("subdir", False)
        super().__init__(path=path, db_open_mode=db_open_mode, config=config)
        indexes = [
            {
                "what": legdb.Node,
                "name": ConceptIndexBy.language_label_sense.value,
                "func": "!{language}|{label}|{sense}",
                "duplicates": False,
            },
            {
                "what": legdb.Node,
                "name": ConceptIndexBy.language_label.value,
                "func": "!{language}|{label}",
                "duplicates": True,
            },
            {
                "what": legdb.Node,
                "name": ConceptIndexBy.label.value,
                "func": "{label}",
                "duplicates": True,
            },
            {
                "what": legdb.Node,
                "name": ConceptIndexBy.language.value,
                "func": "{language}",
                "duplicates": True,
            },
            {
                "what": legdb.Node,
                "name": ConceptIndexBy.sense.value,
                "func": "{sense}",
                "duplicates": True,
            },
            {
                "what": legdb.Edge,
                "name": AssertionIndexBy.relation.value,
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

    def get_indexes(self, entity: Union[Concept, Assertion]) -> List[str]:
        result = []
        if isinstance(entity, Concept):
            if entity.language is not None and entity.label is not None and entity.sense is not None:
                result.append(ConceptIndexBy.language_label_sense.value)
            else:
                if entity.language is not None and entity.label is not None:
                    result.append(ConceptIndexBy.language_label.value)
                else:
                    if entity.label is not None:
                        result.append(ConceptIndexBy.label.value)
                    if entity.language is not None:
                        result.append(ConceptIndexBy.language.value)
                if entity.sense is not None:
                    result.append(ConceptIndexBy.sense.value)
        elif isinstance(entity, Assertion):
            if entity.start_id is not None and entity.end_id is not None:
                result.append(legdb.IndexBy.start_id_end_id.value)
            else:
                if entity.start_id is not None:
                    result.append(legdb.IndexBy.start_id.value)
                elif entity.end_id is not None:
                    result.append(legdb.IndexBy.end_id.value)
            if entity.relation is not None:
                result.append(AssertionIndexBy.relation.value)
        return result

    @staticmethod
    def _edge_parts(dump_path: Path, count: Optional[int] = None) -> CSVLineTupleGenerator:
        with open(str(dump_path), newline="") as f:
            reader = csv.reader(f, delimiter="\t")
            for i, row in enumerate(reader):
                if i == count:
                    break
                yield row[1:5]

    @staticmethod
    def _extract_relation_name(uri: str) -> str:
        return _to_snake_case(uri[3:])

    def _initial_save_concept(self, concept: Concept, txn: Transaction) -> bytes:
        result = self.seek_one(concept, index_name=ConceptIndexBy.language_label_sense.value, txn=txn)
        if result is None:
            self._entity_count[Concept] += 1
            oid = self.save(concept, return_oid=True, txn=txn)
        else:
            oid = result.oid
        self._append_sample(concept)
        return oid

    def _add_external_url_to_concept(self, concept_oid: bytes, url: str, txn: Transaction) -> None:
        concept = self.get(Concept, concept_oid, txn=txn)
        if concept.external_url is None:
            concept.external_url = []
        concept.external_url.append(url)
        self.save(concept, txn=txn)

    def _initial_save_assertion(self, assertion: Assertion, txn: Transaction) -> None:
        self._entity_count[Assertion] += 1
        self.save(assertion, txn=txn)
        self._append_sample(assertion)

    def _append_sample(self, entity: Union[legdb.Node, legdb.Edge]):
        cls = type(entity)
        if self._entity_count[cls] % self._training_sample_period[cls] == 0:
            sample = entity.to_doc().out
            self._training_samples[cls].append(sample)
            self._training_samples_size[cls] += sys.getsizeof(sample)
            period_recalculation_period = 128
            if self._entity_count[cls] % period_recalculation_period == 0:
                mean_sample_size = self._training_samples_size[cls] // len(self._training_samples[cls])
                self._training_sample_period[cls] = max(
                    1,
                    mean_sample_size * (CONCEPTNET_EDGE_COUNT - self._entity_count[Assertion]) //
                    (ZSTD_SAMPLES_SIZE - self._training_samples_size[cls])
                )

    def load(
            self,
            dump_path: Union[Path, str],
            edge_count: Optional[int] = CONCEPTNET_EDGE_COUNT,
            languages: Optional[Collection[str]] = None,
            compress: bool = True,
    ) -> None:
        logger.info("Load lightning-conceptnet database from dump")
        dump_path = Path(dump_path).expanduser().resolve()
        edges = enumerate(self._edge_parts(dump_path=dump_path, count=edge_count))
        with self.write_transaction as txn:
            for _, (relation_uri, start_uri, end_uri, edge_data) in tqdm(edges, unit=' edges', total=edge_count):
                relation_name = self._extract_relation_name(relation_uri)
                is_end_uri_external_url = relation_name == Relation.EXTERNAL_URL
                start_concept = Concept.from_uri(start_uri, db=self)
                if is_end_uri_external_url:
                    end_concept = None
                else:
                    end_concept = Concept.from_uri(end_uri, db=self)
                language_match = (
                        not languages or
                        (start_concept.language in languages and
                         (end_concept is None or end_concept.language in languages))
                )
                if language_match:
                    start_concept_id = self._initial_save_concept(start_concept, txn=txn)
                    if is_end_uri_external_url:
                        self._add_external_url_to_concept(concept_oid=start_concept_id, url=end_uri, txn=txn)
                    elif end_concept is not None:
                        end_concept_id = self._initial_save_concept(end_concept, txn=txn)
                        edge_data = ujson.loads(edge_data)
                        assertion = Assertion(
                            start_id=start_concept_id,
                            end_id=end_concept_id,
                            relation=relation_name,
                            db=self,
                            **edge_data,
                        )
                        self._initial_save_assertion(assertion, txn=txn)
        call(["du", "-h", self._path])
        logger.info("Compress lightning-conceptnet database")
        if compress:
            self.compress(legdb.Node, training_samples=self._training_samples[Concept])
            self.compress(legdb.Edge, training_samples=self._training_samples[Assertion])
            logger.info("Vacuum lightning-conceptnet database")
            self.vacuum()
        logger.info("Lightning-conceptnet database building finished")


class LightningConceptNet:
    def __init__(
            self,
            path_hint: Union[Path, str],
            db_open_mode: legdb.DbOpenMode = legdb.DbOpenMode.READ,
            config: Optional[Mapping[str, Any]] = None,
    ):
        if config is None:
            config = {}
        config.setdefault("writemap", db_open_mode == legdb.DbOpenMode.WRITE)
        path = get_db_path(path_hint=path_hint, db_open_mode=db_open_mode)
        self._db = LegDBDatabase(path=path, db_open_mode=db_open_mode, config=config)

    def seek_one(
            self,
            entity: Union[Concept, Assertion],
            txn: Optional[Transaction] = None,
    ) -> Union[Concept, Assertion]:
        return self._db.seek_one(entity=entity, txn=txn)

    def seek(
            self,
            entity: Union[Concept, Assertion],
            limit: int = maxsize,
            txn: Optional[Transaction] = None,
    ) -> Generator[Union[Concept, Assertion], None, None]:
        yield from self._db.seek(entity=entity, limit=limit, txn=txn)

    def range(
            self,
            lower: Optional[Union[Concept, Assertion]] = None,
            upper: Optional[Union[Concept, Assertion]] = None,
            index_name: Optional[str] = None,
            inclusive: bool = True) -> Generator[Union[Concept, Assertion], None, None]:
        yield from self._db.range(lower=lower, upper=upper, index_name=index_name, inclusive=inclusive)

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
        edge_count: int = CONCEPTNET_EDGE_COUNT,
        languages: Optional[Collection[str]] = None,
        compress: bool = True,
):
    import stackprinter
    stackprinter.set_excepthook(style='darkbg2')
    logger.info("Create lightning-conceptnet database")
    database_size = 2**36  # 64 GiB
    config = {"map_size": database_size, "readahead": False, "subdir": False, "lock": False}
    lcn = LightningConceptNet(database_path_hint, db_open_mode=legdb.DbOpenMode.WRITE, config=config)
    lcn.load(dump_path=dump_path, edge_count=edge_count, languages=languages, compress=compress)
