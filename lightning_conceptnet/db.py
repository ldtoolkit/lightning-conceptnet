from enum import Enum
from sys import maxsize
from lightning_conceptnet.exceptions import (raise_file_exists_error, raise_not_a_directory_error,
                                             raise_file_not_found_error, raise_is_a_directory_error)
from pathlib import Path
from tqdm import tqdm
from typing import Union, Optional, Generator, Tuple, Mapping, Any, MutableMapping, List, Callable, Collection

import csv
import legdb
import re
import ujson


CONCEPTNET_EDGE_COUNT = 34074917


CSVLineTuple = Tuple[str, str, str, str]
CSVLineTupleGenerator = Generator[CSVLineTuple, None, None]
EdgeData = legdb.Doc
Transaction = legdb.Transaction


class Concept(legdb.Node):
    pass


class Assertion(legdb.Edge):
    def __init__(
            self,
            u: Optional[Concept] = None,
            v: Optional[Concept] = None,
            relation: Optional[str] = None,
            data: Optional[MutableMapping[str, Any]] = None,
    ) -> None:
        if data is None:
            data = {}
        data["relation"] = relation
        super().__init__(u=u, v=v, doc=data)


class AssertionIndexBy(Enum):
    relation = "by_relation"


class ConceptIndexBy(Enum):
    label = "by_label"
    language = "by_language"
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


class DbOpenMode(Enum):
    CREATE = 'create'
    READ = 'read'


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


def get_db_path(path_hint: Union[Path, str], db_open_mode: DbOpenMode) -> Path:
    path_to_check = Path(path_hint).expanduser().resolve()
    if db_open_mode == DbOpenMode.CREATE:
        return _get_db_path_for_db_creation(path_hint=path_to_check)
    elif db_open_mode == DbOpenMode.READ:
        return _get_db_path_for_db_read(path_hint=path_to_check)
    else:
        raise ValueError(f"db_open_mode not supported: '{db_open_mode}")


def _to_snake_case(s: str) -> str:
    regex = re.compile('((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))')
    return regex.sub(r'_\1', s).lower()


class LegDBDatabase(legdb.Database):
    def __init__(self, path: Union[Path, str], config: Optional[Mapping[str, Any]] = None):
        super().__init__(path=path, config=config)
        indexes = [
            {
                "what": legdb.What.Edge,
                "name": AssertionIndexBy.relation.value,
                "func": "{relation}",
                "duplicates": True,
            },
            {
                "what": legdb.What.Node,
                "name": ConceptIndexBy.language_label_sense.value,
                "func": "!{language}|{label}|{sense}",
                "duplicates": False,
            },
            {
                "what": legdb.What.Node,
                "name": ConceptIndexBy.label.value,
                "func": "{label}",
                "duplicates": True,
            },
            {
                "what": legdb.What.Node,
                "name": ConceptIndexBy.language.value,
                "func": "{language}",
                "duplicates": True,
            },
            {
                "what": legdb.What.Node,
                "name": ConceptIndexBy.sense.value,
                "func": "{sense}",
                "duplicates": True,
            },
        ]
        for index in indexes:
            self.ensure_index(**index)

    def get_indexes(self, what: legdb.What, doc: legdb.Doc) -> List[str]:
        result = []
        if what == legdb.What.Node:
            if doc._language is not None and doc._label is not None and doc._sense is not None:
                result.append(ConceptIndexBy.language_label_sense.value)
            else:
                if doc._label is not None:
                    result.append(ConceptIndexBy.label.value)
                if doc._language is not None:
                    result.append(ConceptIndexBy.language.value)
                if doc._sense is not None:
                    result.append(ConceptIndexBy.sense.value)
        elif what == legdb.What.Edge:
            if doc._u is not None and doc._v is not None:
                result.append(legdb.IndexBy.u_v.value)
            else:
                if doc._u is not None:
                    result.append(legdb.IndexBy.u.value)
                elif doc._v is not None:
                    result.append(legdb.IndexBy.v.value)
            if doc._relation is not None:
                result.append(AssertionIndexBy.relation.value)
        return result


class LightningConceptNet:
    def __init__(
            self,
            path_hint: Union[Path, str],
            db_open_mode: DbOpenMode = DbOpenMode.READ,
            config: Optional[Mapping[str, Any]] = None
    ):
        path = get_db_path(path_hint=path_hint, db_open_mode=db_open_mode)
        self._db = LegDBDatabase(path=path, config=config)

    def seek(
            self,
            doc: Union[Concept, Assertion],
            limit: int = maxsize
    ) -> Generator[Union[Concept, Assertion], None, None]:
        if isinstance(doc, Concept):
            what = legdb.What.Node
        elif isinstance(doc, Assertion):
            what = legdb.What.Edge
        else:
            raise TypeError("doc should be either Concept or Relation")
        yield from self._db.seek(what=what, doc=doc, limit=limit)

    def range(
            self,
            lower: Optional[Union[Concept, Assertion]] = None,
            upper: Optional[Union[Concept, Assertion]] = None,
            index_name: Optional[str] = None,
            inclusive: bool = True) -> Generator[Union[Concept, Assertion], None, None]:
        if isinstance(lower, Concept):
            what = legdb.What.Node
        elif isinstance(lower, Assertion):
            what = legdb.What.Edge
        else:
            raise TypeError("doc should be either Concept or Relation")
        yield from self._db.range(what=what, lower=lower, upper=upper, index_name=index_name, inclusive=inclusive)

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

    def _make_concept(self, concept_uri: str) -> Concept:
        split_uri = concept_uri.split("/", maxsplit=4)
        language, label = split_uri[2:4]
        sense = split_uri[4] if len(split_uri) == 5 else "-"
        return Concept({"label": label, "language": language, "sense": sense})

    def _save_concept(self, concept: Concept) -> Concept:
        index_name = ConceptIndexBy.language_label_sense.value
        result = self._db.seek_one(legdb.What.Node, doc=concept, index_name=index_name)
        if result is None:
            self._db.add_node(concept)
            result = self._db.seek_one(legdb.What.Node, doc=concept, index_name=index_name)
        return result

    def _add_external_url_to_concept(self, concept: Concept, url: str) -> None:
        if concept._external_url is None:
            concept._external_url = []
        concept._external_url.append(url)
        self._db.update_node(concept)

    def load(
            self,
            dump_path: Union[Path, str],
            edge_count: Optional[int] = None,
            languages: Optional[Collection[str]] = None
    ) -> None:
        dump_path = Path(dump_path)
        edges = enumerate(self._edge_parts(dump_path=dump_path, count=edge_count))
        for i, (relation_uri, start_uri, end_uri, edge_data) in tqdm(edges, unit=' edges', total=edge_count):
            relation_name = self._extract_relation_name(relation_uri)
            is_end_uri_external_url = relation_name == Relation.EXTERNAL_URL
            u_concept = self._make_concept(concept_uri=start_uri)
            if is_end_uri_external_url:
                v_concept = None
            else:
                v_concept = self._make_concept(concept_uri=end_uri)
            language_match = (
                    languages is None or
                    (u_concept._language in languages and (v_concept is None or v_concept._language in languages))
            )
            if language_match:
                u_concept = self._save_concept(u_concept)
                if is_end_uri_external_url:
                    self._add_external_url_to_concept(concept=u_concept, url=end_uri)
                elif v_concept is not None:
                    v_concept = self._save_concept(v_concept)
                if v_concept is not None:
                    edge_data = ujson.loads(edge_data)
                    relation = Assertion(u=u_concept, v=v_concept, relation=relation_name, data=edge_data)
                    self._db.add_edge(relation)


def create():
    import stackprinter
    stackprinter.set_excepthook(style='darkbg2')
    lcn = LightningConceptNet(
        Path("~/dev/conceptnet/conceptnet.db").expanduser(),
        db_open_mode=DbOpenMode.CREATE,
        config={"map_size": 1024 * 1024 * 1024 * 64, "lock": False, "readahead": False, "subdir": False},
    )
    lcn.load(
        dump_path=Path("~/dev/conceptnet/conceptnet-assertions-5.7.0.csv").expanduser(),
        edge_count=CONCEPTNET_EDGE_COUNT,
        # languages=['lt'],
    )


def test():
    lcn = LightningConceptNet(Path("/home/rominf/dev/conceptnet/conceptnet.db").expanduser())
    saggio = list(lcn.seek(Concept({'language': 'it', 'label': 'saggio', 'sense': 'a'})))
    for x in saggio:
        print(dict(saggio))
    taip = list(lcn.seek(Concept({'language': 'lt', 'label': 'taip'})))
    ne = list(lcn.seek(Concept({'language': 'lt', 'label': 'ne'})))
    l = list(lcn.seek(Assertion(u=taip[0], relation="antonym")))
    for x in l:
        print(dict(x))


if __name__ == "__main__":
    create()
