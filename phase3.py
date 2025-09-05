import json
from collections import deque
import phase1


def update_node_capacities(nodes, node_map, node_time_caps, cap_updates):
    for nid, updates in cap_updates.items():
        if nid not in node_map:
            new_node = {"id": nid, "cpu_capacity": 1, "ram_capacity": 1}
            nodes.append(new_node)
            node_map[nid] = new_node

        if nid not in node_time_caps:
            node_time_caps[nid] = {}
        node_time_caps[nid].update({str(k): v for k, v in updates.items()})


def process_events(events, prev_schedule, task_map, tasks, exec_costs, to_reschedule, earliest_times, failed_nodes):
    for evt in events:
        evt_type = evt.get("type")
        evt_time = evt.get("time", 0)

        if evt_type == "node_failure":
            nid = evt.get("node")
            if not nid:
                continue
            failed_nodes.add(nid)
            for tid, sched in prev_schedule.items():
                if sched.get("node") == nid and sched.get("start_time", 0) >= evt_time:
                    to_reschedule.add(tid)
                    earliest_times[tid] = evt_time

        elif evt_type == "new_task":
            new_task = evt.get("task")
            if not new_task or "id" not in new_task:
                continue
            tid = new_task["id"]
            if tid not in task_map:
                tasks.append(new_task)
                task_map[tid] = new_task
                exec_costs[tid] = new_task.get("exec_cost", {})
                to_reschedule.add(tid)
                earliest_times[tid] = evt_time


def filter_failed_nodes(nodes, node_map, failed_nodes):
    nodes[:] = [n for n in nodes if n["id"] not in failed_nodes]
    for nid in failed_nodes:
        node_map.pop(nid, None)


def handle_capacity_violations(cap_updates, prev_schedule, task_map, task_durs, to_reschedule, earliest_times):
    for nid, updates in cap_updates.items():
        for time_str, new_cap in updates.items():
            t_val = int(time_str)
            cpu_used = 0
            for tid, sched in prev_schedule.items():
                if sched.get("node") == nid:
                    start = sched.get("start_time", 0)
                    dur = task_durs.get(tid, 1)
                    if start <= t_val < start + dur:
                        cpu_used += task_map.get(tid, {}).get("cpu", 1)
            if cpu_used > new_cap:
                for tid, sched in prev_schedule.items():
                    if sched.get("node") == nid and sched.get("start_time", 0) >= t_val:
                        to_reschedule.add(tid)
                        earliest_times[tid] = max(
                            earliest_times.get(tid, 0), t_val)


def mark_failed_node_tasks(prev_schedule, failed_nodes, to_reschedule):
    for tid, sched in prev_schedule.items():
        if sched.get("node") in failed_nodes:
            to_reschedule.add(tid)


def setup_rescheduling(prev_schedule, to_reschedule, task_map):
    fixed = {tid: sched for tid, sched in prev_schedule.items()
             if tid not in to_reschedule}
    resched_tasks = [task_map[tid] for tid in to_reschedule if tid in task_map]
    return fixed, resched_tasks


def fetch_new_assignments(resched_tasks, nodes, exec_costs):
    new_assigns = {}
    if resched_tasks:
        phase1_in = {"tasks": resched_tasks,
                     "nodes": nodes, "exec_cost": exec_costs}
        new_assigns = phase1.assign_tasks_to_nodes(
            phase1_in).get("assignments", {})
    return new_assigns


def init_schedules(nodes, slots, node_time_caps):
    sorted_slots = sorted(slots)
    slot_map = {t: i for i, t in enumerate(sorted_slots)}
    num_slots = len(sorted_slots)
    schedules = {
        n["id"]: [n.get("cpu_capacity", 1)] * num_slots for n in nodes
    }
    for nid, caps in node_time_caps.items():
        for t_str, cap in caps.items():
            if int(t_str) in slot_map:
                schedules[nid][slot_map[int(t_str)]] = cap
    return schedules, sorted_slots, slot_map, num_slots


def apply_fixed_tasks(fixed, schedules, slot_map, task_map, task_durs, num_slots):
    for tid, sched in fixed.items():
        nid = sched["node"]
        start = sched["start_time"]
        dur = task_durs.get(tid, 1)
        if start in slot_map and nid in schedules:
            start_idx = slot_map[start]
            cpu_req = task_map.get(tid, {}).get("cpu", 1)
            for i in range(dur):
                if start_idx + i < num_slots:
                    schedules[nid][start_idx + i] -= cpu_req


def get_topological_order(resched_tasks, deps):
    succs = {t["id"]: [] for t in resched_tasks}
    degrees = {t["id"]: 0 for t in resched_tasks}
    for dep in deps:
        before, after = dep.get("before"), dep.get("after")
        if before in succs and after in succs:
            succs[before].append(after)
            degrees[after] += 1
    queue = deque([tid for tid, deg in degrees.items() if deg == 0])
    order = []
    while queue:
        tid = queue.popleft()
        order.append(tid)
        for succ in succs[tid]:
            degrees[succ] -= 1
            if degrees[succ] == 0:
                queue.append(succ)
    return order


def schedule_tasks(order, new_assigns, task_map, task_durs, earliest_times, deps, fixed, schedules, sorted_slots, slot_map, num_slots):
    new_schedule = dict(fixed)
    failed = [tid for tid in to_reschedule_global if tid not in new_assigns]
    finish_times = {tid: sched["start_time"] +
                    task_durs.get(tid, 1) for tid, sched in fixed.items()}

    for tid in order:
        if tid not in new_assigns:
            continue
        task = task_map[tid]
        nid = new_assigns[tid]
        dur = task_durs.get(tid, 1)
        deadline = task.get("deadline", float('inf'))
        cpu_req = task.get("cpu", 1)
        earliest = earliest_times.get(tid, 0)

        for dep in deps:
            if dep.get("after") == tid and dep.get("before") in finish_times:
                earliest = max(earliest, finish_times[dep.get("before")])

        scheduled = False
        for idx in range(num_slots):
            t = sorted_slots[idx]
            if t < earliest or t + dur > deadline:
                continue
            can_schedule = idx + \
                dur <= num_slots and all(
                    schedules[nid][idx + i] >= cpu_req for i in range(dur))
            if can_schedule:
                for i in range(dur):
                    schedules[nid][idx + i] -= cpu_req
                new_schedule[tid] = {"node": nid, "start_time": t}
                finish_times[tid] = t + dur
                scheduled = True
                break
        if not scheduled:
            failed.append(tid)

    return new_schedule, failed, finish_times


def finalize_results(to_reschedule, failed, new_schedule, exec_costs):
    reassigned = [tid for tid in to_reschedule if tid not in failed]
    total_cost = sum(exec_costs.get(tid, {}).get(
        sched["node"], 0) for tid, sched in new_schedule.items())
    penalty = len(reassigned)
    return reassigned, list(set(failed)), total_cost, penalty


def handle_dynamic_events(data):
    tasks = data.get("tasks", [])
    nodes = data.get("nodes", [])
    exec_costs = data.get("exec_cost", {})
    slots = data.get("time_slots", [])
    node_time_caps = data.get("node_capacity_per_time", {})
    deps = data.get("dependencies", [])
    task_durs = data.get("task_duration", {})
    prev_schedule = data.get("previous_schedule", {})
    events = data.get("events", [])
    cap_updates = data.get("node_capacity_update", {})

    task_map = {t["id"]: t for t in tasks}
    node_map = {n["id"]: n for n in nodes}

    global to_reschedule_global
    to_reschedule = set()
    earliest_times = {}
    failed_nodes = set()

    update_node_capacities(nodes, node_map, node_time_caps, cap_updates)
    process_events(events, prev_schedule, task_map, tasks,
                   exec_costs, to_reschedule, earliest_times, failed_nodes)
    filter_failed_nodes(nodes, node_map, failed_nodes)
    handle_capacity_violations(
        cap_updates, prev_schedule, task_map, task_durs, to_reschedule, earliest_times)
    mark_failed_node_tasks(prev_schedule, failed_nodes, to_reschedule)

    to_reschedule_global = to_reschedule
    fixed, resched_tasks = setup_rescheduling(
        prev_schedule, to_reschedule, task_map)
    new_assigns = fetch_new_assignments(resched_tasks, nodes, exec_costs)
    schedules, sorted_slots, slot_map, num_slots = init_schedules(
        nodes, slots, node_time_caps)
    apply_fixed_tasks(fixed, schedules, slot_map,
                      task_map, task_durs, num_slots)
    order = get_topological_order(resched_tasks, deps)
    new_schedule, failed, finish_times = schedule_tasks(
        order, new_assigns, task_map, task_durs, earliest_times, deps, fixed, schedules, sorted_slots, slot_map, num_slots)

    unmapped = [tid for tid in to_reschedule if tid not in task_map]
    failed.extend(unmapped)
    reassigned, failed, total_cost, penalty = finalize_results(
        to_reschedule, failed, new_schedule, exec_costs)

    return {
        "updated_schedule": new_schedule,
        "reassigned_tasks": reassigned,
        "failed_tasks": failed,
        "total_cost": total_cost,
        "change_penalty": penalty
    }


to_reschedule_global = None

input_data = {
    "tasks": [
        {"id": "T1", "cpu": 2, "ram": 4, "deadline": 3, "duration": 1},
        {"id": "T2", "cpu": 1, "ram": 2, "deadline": 3, "duration": 1},
        {"id": "T3", "cpu": 3, "ram": 3, "deadline": 4, "duration": 2}
    ],
    "nodes": [
        {"id": "N1", "cpu_capacity": 5, "ram_capacity": 6},
        {"id": "N2", "cpu_capacity": 6, "ram_capacity": 5},
        {"id": "N3", "cpu_capacity": 2, "ram_capacity": 4}
    ],
    "exec_cost": {
        "T1": {"N1": 4, "N2": 2},
        "T2": {"N1": 3, "N2": 4},
        "T3": {"N1": 2, "N2": 3},
        "T4": {"N1": 3, "N3": 2}
    },
    "task_duration": {"T1": 1, "T2": 1, "T3": 2, "T4": 1},
    "time_slots": [0, 1, 2, 3, 4],
    "previous_schedule": {
        "T1": {"node": "N1", "start_time": 0},
        "T2": {"node": "N2", "start_time": 1},
        "T3": {"node": "N1", "start_time": 2}
    },
    "events": [
        {"type": "node_failure", "node": "N2", "time": 1},
        {"type": "new_task", "task": {
            "id": "T4", "cpu": 2, "ram": 2, "deadline": 4, "duration": 1,
            "exec_cost": {"N1": 3, "N3": 2}
        }}
    ],
    "node_capacity_update": {
        "N1": {"2": 1},
        "N3": {"1": 2}
    }
}

result = handle_dynamic_events(input_data)
print(json.dumps(result, indent=2))
