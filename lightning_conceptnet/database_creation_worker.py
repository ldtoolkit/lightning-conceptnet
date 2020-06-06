from pathlib import Path
from signal import signal, SIGTERM
from typing import Optional, Mapping, Any, Collection

import zmq

import legdb
from legdb import Transaction
from lightning_conceptnet import database
from pynndb import write_transaction


class DatabaseCreationWorker:
    def __init__(
            self,
            server_address: str,
            database_path: Path,
            config: Optional[Mapping[str, Any]] = None,
    ):
        self._server_address = server_address
        self._database_path = database_path
        self._config = dict(config, sync=False, metasync=False)
        self._db = None
        self._context = None
        self._socket = None
        self._edges = []

    def _save_edges(self, txn: Transaction):
        for edge in self._edges:
            self._db.save_edge_concepts(edge=edge, txn=txn)
        for edge in self._edges:
            self._db.save(edge.assertion, txn=txn)
        self._db.sync()
        self._edges.clear()

    @staticmethod
    def handle_sigterm(*args):
        _ = args
        raise KeyboardInterrupt

    def run(self, languages: Optional[Collection[str]]):
        @write_transaction
        def save_edges(db, txn=None):
            self._save_edges(txn=txn)

        signal(SIGTERM, self.handle_sigterm)
        self._db = database.LightningConceptNetDatabase(
            path=self._database_path,
            db_open_mode=legdb.DbOpenMode.READ_WRITE,
            config=self._config,
        )
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.PULL)
        self._socket.connect(self._server_address)
        try:
            edge_batch_count = 100
            while True:
                edge_parts = self._socket.recv_pyobj()
                edge = self._db.edge_from_edge_parts(edge_parts, languages=languages)
                self._edges.append(edge)
                if len(self._edges) == edge_batch_count:
                    save_edges(self._db._db)
        except KeyboardInterrupt:
            save_edges(self._db._db)
