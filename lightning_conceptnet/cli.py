from pathlib import Path
from typing import Optional, List

import typer

import lightning_conceptnet

app = typer.Typer()


@app.command()
def build(
        database_path_hint: Path,
        dump_path: Path,
        edge_count: Optional[int] = None,
        languages: Optional[List[str]] = None,
        compress: bool = True,
):
    lightning_conceptnet.build(
        database_path_hint=database_path_hint,
        dump_path=dump_path,
        edge_count=edge_count,
        languages=languages,
        compress=compress,
    )
