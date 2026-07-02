"""Écriture vers la couche Bronze (HDFS), avec backends interchangeables.

- "pyarrow" : pyarrow.fs.HadoopFileSystem  (recommandé sur le cluster ; utilise
              la configuration Hadoop/HADOOP_CONF_DIR de l'environnement)
- "cli"     : appelle `hdfs dfs -put/-test`  (si le client CLI est disponible)
- "local"   : écrit dans un dossier local  (tests hors cluster)

Toutes les destinations sont des chemins RELATIFS sous la racine Bronze, ex.
"nbb/csv/0203430576/2024_ABC.csv". put_bytes() renvoie le chemin absolu final.
"""
from __future__ import annotations

import os
import subprocess
from urllib.parse import urlparse

from . import config


class HdfsIO:
    def __init__(self, backend: str | None = None, base: str | None = None):
        self.backend = backend or config.HDFS_BACKEND
        self.base = (base or config.HDFS_BRONZE).rstrip("/")
        self._fs = None
        if self.backend == "pyarrow":
            self._init_pyarrow()
        elif self.backend == "local":
            # en local, on retire le schéma hdfs:// éventuel
            p = urlparse(self.base)
            if p.scheme in ("hdfs", "file"):
                self.base = p.path or self.base

    # ---- initialisation pyarrow -------------------------------------------
    def _init_pyarrow(self):
        from pyarrow import fs
        p = urlparse(self.base)
        host = p.hostname or "default"      # "default" => namenode de la conf Hadoop
        port = p.port or 8020
        self._fs = fs.HadoopFileSystem(host=host, port=port)
        self._base_path = p.path or "/"     # chemin sans le schéma pour pyarrow

    # ---- résolution de chemin ---------------------------------------------
    def full_path(self, rel: str) -> str:
        return f"{self.base}/{rel.lstrip('/')}"

    def _pyarrow_path(self, rel: str) -> str:
        return f"{self._base_path.rstrip('/')}/{rel.lstrip('/')}"

    # ---- API --------------------------------------------------------------
    def exists(self, rel: str) -> bool:
        if self.backend == "pyarrow":
            from pyarrow.fs import FileType
            info = self._fs.get_file_info(self._pyarrow_path(rel))
            return info.type != FileType.NotFound
        if self.backend == "cli":
            return subprocess.run(
                ["hdfs", "dfs", "-test", "-e", self.full_path(rel)]
            ).returncode == 0
        # local
        return os.path.exists(self.full_path(rel))

    def put_bytes(self, data: bytes, rel: str) -> str:
        """Écrit `data` à l'emplacement Bronze `rel`. Renvoie le chemin absolu."""
        if self.backend == "pyarrow":
            path = self._pyarrow_path(rel)
            parent = path.rsplit("/", 1)[0]
            self._fs.create_dir(parent, recursive=True)
            with self._fs.open_output_stream(path) as f:
                f.write(data)
            return self.full_path(rel)
        if self.backend == "cli":
            dest = self.full_path(rel)
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", dest.rsplit("/", 1)[0]], check=True)
            # écrit via stdin -> HDFS
            subprocess.run(["hdfs", "dfs", "-put", "-f", "-", dest], input=data, check=True)
            return dest
        # local
        dest = self.full_path(rel)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "wb") as f:
            f.write(data)
        return dest

    def put_file(self, local_path: str, rel: str) -> str:
        with open(local_path, "rb") as f:
            return self.put_bytes(f.read(), rel)

    # ---- lecture (nécessaire au recalcul Gold depuis le Bronze) -----------
    def read_bytes(self, rel: str) -> bytes:
        if self.backend == "pyarrow":
            with self._fs.open_input_stream(self._pyarrow_path(rel)) as f:
                return f.read()
        if self.backend == "cli":
            return subprocess.run(
                ["hdfs", "dfs", "-cat", self.full_path(rel)],
                capture_output=True, check=True,
            ).stdout
        with open(self.full_path(rel), "rb") as f:
            return f.read()

    def read_text(self, rel: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(rel).decode(encoding, "replace")

    def list_files(self, rel_dir: str = "", suffix: str | None = None) -> list:
        """Liste (récursive) les chemins RELATIFS des fichiers sous `rel_dir`.

        Les chemins renvoyés sont relatifs à la racine Bronze — donc directement
        réutilisables avec :meth:`read_text` / :meth:`exists`.
        """
        rel_dir = rel_dir.strip("/")
        if self.backend == "pyarrow":
            from pyarrow.fs import FileSelector, FileType
            base = self._pyarrow_path(rel_dir).rstrip("/")
            root = self._base_path.rstrip("/")
            try:
                infos = self._fs.get_file_info(FileSelector(base, recursive=True))
            except Exception:
                return []
            out = []
            for i in infos:
                if i.type == FileType.File:
                    rel = i.path[len(root):].lstrip("/") if i.path.startswith(root) else i.path
                    if suffix is None or rel.endswith(suffix):
                        out.append(rel)
            return sorted(out)
        if self.backend == "cli":
            full = self.full_path(rel_dir)
            res = subprocess.run(["hdfs", "dfs", "-ls", "-R", full],
                                 capture_output=True, text=True)
            base = self.base.rstrip("/")
            out = []
            for line in res.stdout.splitlines():
                if line.startswith("d") or not line.strip():
                    continue  # répertoire
                path = line.split()[-1]
                rel = path[len(base):].lstrip("/") if path.startswith(base) else path
                if suffix is None or rel.endswith(suffix):
                    out.append(rel)
            return sorted(out)
        # local
        root = self.base.rstrip("/")
        start = os.path.join(root, rel_dir)
        out = []
        for dirpath, _dirs, files in os.walk(start):
            for name in files:
                full = os.path.join(dirpath, name)
                rel = os.path.relpath(full, root)
                if suffix is None or rel.endswith(suffix):
                    out.append(rel)
        return sorted(out)


def get_hdfs(backend: str | None = None, base: str | None = None) -> HdfsIO:
    return HdfsIO(backend=backend, base=base)
