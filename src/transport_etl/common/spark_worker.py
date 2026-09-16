"""Windows PySpark worker entry point with the SPARK-53759 flush backport."""

from __future__ import annotations

import os

from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import write_int
from pyspark.worker import main as run_worker


def main() -> None:
    """Run a simple PySpark worker and flush results before socket shutdown."""
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    sock_file, _ = local_connect_and_auth(java_port, auth_secret)
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    try:
        run_worker(sock_file, sock_file)
    finally:
        # PySpark 3.5.2 omits this flush on its Windows simple-worker path.
        sock_file.flush()


if __name__ == "__main__":
    main()
