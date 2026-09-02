import os
import re
import subprocess


def get_taosd_pid():
    """Get the PID of the running taosd process."""
    result = subprocess.run(
        ["pidof", "taosd"], capture_output=True, text=True
    )
    pids = result.stdout.strip().split()
    if not pids:
        return None
    # Return the first (or only) PID
    return int(pids[0])


def get_system_cpu_count():
    """Get the number of CPUs available to this process (cgroup-aware)."""
    return os.sched_getaffinity(0).__len__()


def get_all_thread_affinities(pid):
    """Read CPU affinity masks for all threads of a given PID.

    Returns a list of dicts:
      [{"tid": int, "name": str, "cpus_allowed": set_of_ints}, ...]
    """
    task_dir = f"/proc/{pid}/task"
    if not os.path.isdir(task_dir):
        return []

    threads = []
    for tid_name in os.listdir(task_dir):
        tid = int(tid_name)
        info = {"tid": tid, "name": "", "cpus_allowed": set()}

        # Read thread name
        comm_path = os.path.join(task_dir, tid_name, "comm")
        try:
            with open(comm_path, "r") as f:
                info["name"] = f.read().strip()
        except (IOError, OSError):
            pass

        # Read Cpus_allowed from status
        status_path = os.path.join(task_dir, tid_name, "status")
        try:
            with open(status_path, "r") as f:
                for line in f:
                    if line.startswith("Cpus_allowed:"):
                        hex_mask = line.split(":")[1].strip()
                        info["cpus_allowed"] = parse_cpu_hex_mask(hex_mask)
                        break
        except (IOError, OSError):
            pass

        threads.append(info)

    return threads


def get_thread_affinity(pid, thread_name_pattern):
    """Get CPU affinity sets for threads matching a name pattern.

    Args:
        pid: Process ID
        thread_name_pattern: regex pattern to match thread comm name

    Returns:
        List of (tid, name, cpu_set) tuples for matching threads
    """
    pattern = re.compile(thread_name_pattern)
    all_threads = get_all_thread_affinities(pid)
    return [
        (t["tid"], t["name"], t["cpus_allowed"])
        for t in all_threads
        if pattern.search(t["name"])
    ]


def parse_cpu_hex_mask(hex_mask):
    """Parse a hex CPU mask (e.g., 'ff' or 'ff,ffffffff') into a set of core IDs.

    The mask may contain commas separating groups of 8 hex digits (32 bits each).
    """
    # Remove commas — Linux uses them to group every 32 bits
    clean = hex_mask.replace(",", "")
    value = int(clean, 16)
    cores = set()
    bit = 0
    while value > 0:
        if value & 1:
            cores.add(bit)
        value >>= 1
        bit += 1
    return cores


def get_full_cpu_set():
    """Get the full set of CPU core IDs available to this process."""
    return os.sched_getaffinity(0)


def has_restricted_affinity(cpu_set, total_cpus=None):
    """Check if a CPU set is restricted (not using all available cores).

    Args:
        cpu_set: set of CPU core IDs
        total_cpus: total number of CPUs (defaults to system available)

    Returns:
        True if the thread is restricted to a subset of available cores
    """
    if total_cpus is None:
        total_cpus = get_system_cpu_count()
    return len(cpu_set) < total_cpus


def parse_core_ids_string(core_ids_str):
    """Parse a core_ids string from SHOW CPU_ALLOCATION (e.g., '0,1,2') into a set of ints.

    Returns empty set for '-' (disabled).
    """
    if core_ids_str.strip() == "-":
        return set()
    return set(int(x.strip()) for x in core_ids_str.split(","))
