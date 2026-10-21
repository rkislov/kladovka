#!/usr/bin/env python3
"""Optional macOS vmmap diagnostics AFTER each write_pilot run and verification."""
import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import subprocess
import sys

import write_pilot


def main():
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument('--maps-output', required=True)
    args, remaining = parser.parse_known_args()
    if not shutil.which('vmmap'):
        parser.error('vmmap is required; use write_pilot.py for portable measurements')
    original = write_pilot.servers
    observations = []

    @contextmanager
    def observed(*args, **kwargs):
        with original(*args, **kwargs) as cluster:
            yield cluster
            nodes = []
            for i, pid in enumerate(cluster[1]):
                result = subprocess.run(['vmmap', '-summary', str(pid)], text=True,
                                        capture_output=True, timeout=30)
                path = args[2] / f'data-{i}' / '.consensus' / 'raft.db'
                nodes.append(dict(node=i, status=result.returncode, stdout=result.stdout,
                                  stderr=result.stderr, raft_db_bytes=path.stat().st_size))
            observations.append(nodes)

    sys.argv = [sys.argv[0], *remaining]
    with Path(args.maps_output).open('x') as out:
        write_pilot.servers = observed
        try:
            write_pilot.main()
        finally:
            write_pilot.servers = original
            json.dump(dict(method='vmmap -summary after PUT, drain, idle and GET verification; '
                                  'not sampled during timed PUT', runs=observations), out, indent=2)


if __name__ == '__main__':
    main()
