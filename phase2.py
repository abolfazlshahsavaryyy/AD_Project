from collections import deque
import phase1


def build_job_graph(jobs, precedence):
    job_map = {job["id"]: job for job in jobs}
    successors = {jid: [] for jid in job_map}
    predecessors = {jid: [] for jid in job_map}
    indegree = {jid: 0 for jid in job_map}

    for dep in precedence:
        pre, post = dep.get("before"), dep.get("after")
        if pre in job_map and post in job_map:
            successors[pre].append(post)
            predecessors[post].append(pre)
            indegree[post] += 1

    return job_map, successors, predecessors, indegree


def topological_sort(job_map, successors, indegree):
    queue = deque([jid for jid, deg in indegree.items() if deg == 0])
    topo_order = []
    while queue:
        current = queue.popleft()
        topo_order.append(current)
        for nxt in successors[current]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)

    if len(topo_order) != len(job_map):
        return None 
    return topo_order


def prepare_machine_grid(machines, machine_cap_time, time_slots):
    sorted_slots = sorted(time_slots)
    slot_index = {t: i for i, t in enumerate(sorted_slots)}
    n_slots = len(sorted_slots)
    machine_grid = {}

    for m in machines:
        mid = m["id"]
        default_cap = m.get("cpu_capacity", 1)
        grid = [default_cap] * n_slots
        for t, cap in machine_cap_time.get(mid, {}).items():
            if int(t) in slot_index:
                grid[slot_index[int(t)]] = cap
        machine_grid[mid] = grid

    return machine_grid, sorted_slots, n_slots


def schedule_jobs(
    topo_order,
    assignments,
    job_map,
    predecessors,
    machine_grid,
    sorted_slots,
    n_slots,
    job_duration,
):
    schedule = {}
    finish_times = {}
    valid_schedule = True

    for jid in topo_order:
        if jid not in assignments:
            continue
        mid = assignments[jid]
        dur = job_duration.get(jid, 1)
        deadline = job_map[jid].get("deadline", float("inf"))

        earliest_start = 0
        for pred in predecessors[jid]:
            if pred in finish_times:
                earliest_start = max(earliest_start, finish_times[pred])

        scheduled = False
        for start_idx in range(n_slots):
            start_time = sorted_slots[start_idx]
            if start_time < earliest_start:
                continue
            finish_time = start_time + dur
            if finish_time > deadline:
                break

            can_schedule = True
            if start_idx + dur > n_slots:
                can_schedule = False
            else:
                for i in range(dur):
                    if machine_grid[mid][start_idx + i] < 1:
                        can_schedule = False
                        break

            if can_schedule:
                for i in range(dur):
                    machine_grid[mid][start_idx + i] -= 1
                schedule[jid] = {"machine": mid, "start_time": start_time}
                finish_times[jid] = finish_time
                scheduled = True
                break

        if not scheduled:
            valid_schedule = False
            break

    return schedule, valid_schedule


def schedule_jobs_phase2(input_data):
    jobs = input_data.get("jobs", [])
    machines = input_data.get("machines", [])
    cost_table = input_data.get("cost_table", {})
    time_slots = input_data.get("time_frames", [])
    machine_cap_time = input_data.get("machine_capacity_time", {})
    precedence = input_data.get("precedence", [])
    job_duration = input_data.get("job_duration", {})

    if not time_slots:
        return {"schedule": {}, "valid": False, "total_cost": 0}


    phase1_input = {"tasks": jobs, "nodes": machines, "exec_cost": cost_table}
    phase1_result = phase1.assign_tasks_to_nodes(phase1_input)
    assignments = phase1_result.get("assignments", {})
    base_cost = phase1_result.get("total_cost", 0)

    if not assignments:
        return {"schedule": {}, "valid": True, "total_cost": 0}

    job_map, successors, predecessors, indegree = build_job_graph(jobs, precedence)
    topo_order = topological_sort(job_map, successors, indegree)
    if topo_order is None:
        return {"schedule": {}, "valid": False, "total_cost": base_cost}


    machine_grid, sorted_slots, n_slots = prepare_machine_grid(
        machines, machine_cap_time, time_slots
    )

    schedule, valid_schedule = schedule_jobs(
        topo_order,
        assignments,
        job_map,
        predecessors,
        machine_grid,
        sorted_slots,
        n_slots,
        job_duration,
    )

    return {"schedule": schedule, "valid": valid_schedule, "total_cost": base_cost}



if __name__ == "__main__":
    input_data = {
        "jobs": [
            {"id": "T1", "cpu": 2, "ram": 4, "deadline": 3},
            {"id": "T2", "cpu": 1, "ram": 2, "deadline": 3},
            {"id": "T3", "cpu": 3, "ram": 3, "deadline": 4},
        ],
        "machines": [
            {"id": "N1", "cpu_capacity": 5, "ram_capacity": 6},
            {"id": "N2", "cpu_capacity": 6, "ram_capacity": 5},
        ],
        "cost_table": {
            "T1": {"N1": 4, "N2": 2},
            "T2": {"N1": 3, "N2": 4},
            "T3": {"N1": 2, "N2": 3},
        },
        "time_frames": [0, 1, 2, 3],
        "machine_capacity_time": {
            "N1": {"0": 2, "1": 2, "2": 2, "3": 2},
            "N2": {"0": 3, "1": 3, "2": 2, "3": 2},
        },
        "precedence": [
            {"before": "T1", "after": "T3"},
            {"before": "T2", "after": "T3"},
        ],
        "job_duration": {"T1": 1, "T2": 1, "T3": 2},
    }

    result = schedule_jobs_phase2(input_data)
    print(result)


output_data = {
    "schedule": {
        "T1": {"machine": "N2", "start_time": 0},
        "T2": {"machine": "N1", "start_time": 0},
        "T3": {"machine": "N1", "start_time": 1},
    },
    "valid": True,
    "total_cost": 7,
}
