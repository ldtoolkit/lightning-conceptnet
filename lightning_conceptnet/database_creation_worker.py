from pathlib import Path
from signal import signal, SIGTERM
from typing import Optional, Mapping, Any, Collection

import zmq

import legdb
from lightning_conceptnet import database


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

    def _save_edges(self):
        with self._db.write_transaction as txn:
            for edge in self._edges:
                self._db.save_edge_concepts(edge=edge, txn=txn)
        with self._db.write_transaction as txn:
            for edge in self._edges:
                self._db.save(edge.assertion, txn=txn)
        self._db.sync()
        self._edges.clear()

    @staticmethod
    def handle_sigterm(*args):
        _ = args
        raise KeyboardInterrupt

    def run(self, languages: Optional[Collection[str]]):
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
                    self._save_edges()
        except KeyboardInterrupt:
            self._save_edges()
