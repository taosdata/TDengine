import sys
import heapq

if len(sys.argv) != 3:
    print("Usage: python gen_balanced_tasks.py test-result-3.0.txt <total_workers>")
    sys.exit(1)

task_file = sys.argv[1]
total_workers = int(sys.argv[2])

tasks = []
with open(task_file) as f:
    for line in f:
        parts = line.strip().split('|')
        if len(parts) < 3:
            continue
        try:
            time = int(parts[-1])
        except ValueError:
            time = 1
        # Remove |success|time, keep the first part and add two commas at the front
        case = ',,' + parts[0]
        tasks.append((time, case))

tasks.sort(reverse=True)  # longest first

workers = [[] for _ in range(total_workers)]
heap = [(0, i) for i in range(total_workers)]  # (total_time, worker_id)

for time, case in tasks:
    total, idx = heapq.heappop(heap)
    workers[idx].append(case)
    heapq.heappush(heap, (total + time, idx))

for i, w in enumerate(workers):
    with open(f"balanced_task_{i}.txt", "w") as f:
        for case in w:
            f.write(case + "\n")