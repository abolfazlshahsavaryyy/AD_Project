import json

def get_example_instance():
    """
    Returns a hardcoded scheduling instance instead of reading from a file.
    """
    return {
        "node": "N1",
        "assigned_tasks": [
            {"id": "T1", "cpu": 2, "ram": 2, "duration": 1, "deadline": 2},
            {"id": "T3", "cpu": 2, "ram": 1, "duration": 2, "deadline": 4},
            {"id": "T4", "cpu": 1, "ram": 1, "duration": 1, "deadline": 3}
        ],
        "resource_per_time": {
            "0": {"cpu": 2},
            "1": {"cpu": 2},
            "2": {"cpu": 2},
            "3": {"cpu": 2}
        },
        "time_slots": [0, 1, 2, 3]
    }

#########################################################################################
# ---------------------- Helper Functions ----------------------

def build_available_resources(resources, timeline):
    """
    Create a dictionary of available CPU resources for each time slot.
    """
    cpu_capacity_map = {}
    for t in sorted(timeline):
        cpu_capacity_map[t] = resources.get(str(t), {}).get("cpu", 0)
    return cpu_capacity_map


def sort_tasks_by_deadline(tasks, timeline):
    """
    Sort tasks by deadline first (earliest deadline first), 
    then by ID as a tie-breaker.
    """
    last_time = timeline[-1] if timeline else 0

    def sort_key(task):
        deadline = task.get("deadline", last_time)
        return (deadline, task.get("id", ""))

    return sorted(tasks, key=sort_key)


def can_schedule(task, start, resources, timeline):
    """
    Check if a task can run starting at 'start' time without exceeding resources.
    """
    cpu_required = task.get("cpu", 1)
    duration = task.get("duration", 1)

    for i in range(duration):
        current_time = start + i
        if current_time not in timeline:
            return False
        if resources.get(current_time, 0) < cpu_required:
            return False
    return True


def allocate_resources(task, start, resources):
    """
    Deduct CPU usage from resources when a task is scheduled.
    """
    cpu_required = task.get("cpu", 1)
    duration = task.get("duration", 1)

    for i in range(duration):
        resources[start + i] -= cpu_required

####################################################################################
def run_local_scheduler(instance):
    """
    Main scheduler that places tasks on a single node using EDF (earliest deadline first).
    """
    tasks = instance.get("assigned_tasks", [])
    resources = instance.get("resource_per_time", {})
    timeline = instance.get("time_slots", [])

    if not timeline:
        print("Warning: No timeline provided.")
        return {"execution_schedule": {}, "total_idle_time": 0, "penalty_cost": 0}

    available_resources = build_available_resources(resources, timeline)
    sorted_tasks = sort_tasks_by_deadline(tasks, timeline)

    schedule = {}
    penalty_count = 0

    for task in sorted_tasks:
        task_id = task.get("id")
        duration = task.get("duration", 1)
        deadline = task.get("deadline", timeline[-1])

        scheduled = False

        for start in timeline:
            finish = start + duration
            if finish > deadline:
                break

            if can_schedule(task, start, available_resources, timeline):
                allocate_resources(task, start, available_resources)
                schedule[task_id] = {"start_time": start, "meets_deadline": True}
                scheduled = True
                break

        if not scheduled:
            penalty_count += 1
            schedule[task_id] = {"start_time": None, "meets_deadline": False}

    idle_total = sum(available_resources.values())

    return {
        "execution_schedule": schedule,
        "total_idle_time": idle_total,
        "penalty_cost": penalty_count,
    }


# ---------------------- Run Example ----------------------

if __name__ == "__main__":
    input_instance = get_example_instance()
    result = run_local_scheduler(input_instance)
    print(json.dumps(result, indent=2))
