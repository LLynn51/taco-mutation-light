"""Trusted local-only isolation probe; run inside the container."""
import json
import os
from pathlib import Path
import sys


def main():
    status = dict(line.split(":", 1) for line in Path("/proc/self/status").read_text().splitlines() if ":" in line)
    def denied(path):
        try:
            Path(path).write_text("probe")
            return False
        except OSError:
            return True
    Path("/tmp/private-probe").write_text("independent tmpfs")
    result = {"python_version": ".".join(map(str, sys.version_info[:3])), "uid": os.getuid(),
              "cap_eff": status["CapEff"].strip(), "no_new_privs": status["NoNewPrivs"].strip(),
              "interfaces": sorted(p.name for p in Path("/sys/class/net").iterdir()),
              "source_read_only": denied("/unit/source.py"), "root_read_only": denied("/usr/local/lib/probe-file"),
              "tmp_writable": Path("/tmp/private-probe").read_text() == "independent tmpfs",
              "memory_max": Path("/sys/fs/cgroup/memory.max").read_text().strip(),
              "pids_max": Path("/sys/fs/cgroup/pids.max").read_text().strip()}
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
