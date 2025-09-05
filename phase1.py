import heapq
import json

class Edge:

    def __init__(self, v, rev, capacity, cost):
        self.v = v            
        self.rev = rev  
        self.capacity = capacity
        self.cost = cost


class MCMF:

    def __init__(self, num_vertices):
        self.num_vertices = num_vertices
        self.graph = [[] for _ in range(num_vertices)]

    def add_edge(self, start_node, end_node, capacity, cost):
        forward_rev_idx = len(self.graph[end_node])
        backward_rev_idx = len(self.graph[start_node])

        # forward edge
        self.graph[start_node].append(Edge(end_node, forward_rev_idx, capacity, cost))
        # reverse edge
        self.graph[end_node].append(Edge(start_node, backward_rev_idx, 0, -cost))


    def _shortest_path(self, source_vertex, potentials):
        INF = float('inf')

        # Initialization
        n = self.num_vertices
        dist = [INF] * n
        parent = [(-1, -1)] * n  # (prev_vertex, edge_index)
        dist[source_vertex] = 0

        # Priority queue: (distance, vertex)
        priority_q = [(0, source_vertex)]

        while priority_q:
            current_dist, u = heapq.heappop(priority_q)
            if current_dist > dist[u]:
                continue

            for edge_index, edge in enumerate(self.graph[u]):
                if edge.capacity <= 0:
                    continue

                # Calculate reduced cost
                reduced = edge.cost + potentials[u] - potentials[edge.v]
                new_dist = dist[u] + reduced

                # Relaxation step
                if new_dist < dist[edge.v]:
                    dist[edge.v] = new_dist
                    parent[edge.v] = (u, edge_index)
                    heapq.heappush(priority_q, (new_dist, edge.v))

        # Split parent into two arrays to keep compatibility
        parent_vertex = [p[0] for p in parent]
        parent_edge_idx = [p[1] for p in parent]

        return dist, parent_vertex, parent_edge_idx


    def _update_prices(self, prices, distances):
        for i in range(self.num_vertices):
            if distances[i] < float('inf'):
                prices[i] += distances[i]

    def _find_bottleneck(self, source_vertex, sink_vertex, parent_vertex, parent_edge_idx, flow_limit, total_flow):
        push_flow = flow_limit - total_flow
        curr = sink_vertex
        while curr != source_vertex:
            prev = parent_vertex[curr]
            edge_idx = parent_edge_idx[curr]
            push_flow = min(push_flow, self.graph[prev][edge_idx].capacity)
            curr = prev
        return push_flow

    def _augment_flow(self, source_vertex, sink_vertex, parent_vertex, parent_edge_idx, push_flow):
        curr = sink_vertex
        while curr != source_vertex:
            prev = parent_vertex[curr]
            edge_idx = parent_edge_idx[curr]

            # forward edge
            self.graph[prev][edge_idx].capacity -= push_flow
            # reverse edge
            rev_edge_idx = self.graph[prev][edge_idx].rev
            self.graph[curr][rev_edge_idx].capacity += push_flow

            curr = prev


    def solve_flow(self, source_vertex, sink_vertex, max_flow=None):
        INF = float('inf')
        total_flow, total_cost = 0, 0
        prices = [0] * self.num_vertices
        flow_limit = max_flow if max_flow is not None else INF

        while total_flow < flow_limit:
            distances, parent_vertex, parent_edge_idx = self._shortest_path(source_vertex, prices)

            if distances[sink_vertex] == INF:
                break  # no path found

            self._update_prices(prices, distances)
            push_flow = self._find_bottleneck(source_vertex, sink_vertex, parent_vertex, parent_edge_idx, flow_limit, total_flow)

            if push_flow == 0:
                break

            total_flow += push_flow
            total_cost += push_flow * prices[sink_vertex]
            self._augment_flow(source_vertex, sink_vertex, parent_vertex, parent_edge_idx, push_flow)

        return total_flow, total_cost


def _build_solver(task_list, compute_nodes, execution_costs):
    num_tasks = len(task_list)
    num_nodes = len(compute_nodes)

    # Graph layout:
    # source_vertex = 0
    # tasks = 1..num_tasks
    # nodes = num_tasks+1 .. num_tasks+num_nodes
    # sink_vertex = last
    source_vertex = 0
    task_offset = 1
    node_offset = task_offset + num_tasks
    sink_vertex = node_offset + num_nodes
    total_vertices = sink_vertex + 1

    flow_solver = MCMF(total_vertices)

    _add_source_to_tasks(flow_solver, source_vertex, task_offset, num_tasks)
    _add_tasks_to_nodes(flow_solver, task_list, compute_nodes, execution_costs, task_offset, node_offset)
    _add_nodes_to_sink(flow_solver, node_offset, num_nodes, sink_vertex, num_tasks)

    return flow_solver, source_vertex, sink_vertex, task_offset, node_offset


def _add_source_to_tasks(flow_solver, source_vertex, task_offset, num_tasks):
    
    for i in range(num_tasks):
        flow_solver.add_edge(source_vertex, task_offset + i, 1, 0)


def _add_tasks_to_nodes(flow_solver, task_list, compute_nodes, execution_costs, task_offset, node_offset):
    for i, task in enumerate(task_list):
        task_id = task.get("id")
        for j, node in enumerate(compute_nodes):
            node_id = node.get("id")
            cost = execution_costs.get(task_id, {}).get(node_id, None)
            if cost is not None:
                flow_solver.add_edge(task_offset + i, node_offset + j, 1, int(cost))


def _add_nodes_to_sink(flow_solver, node_offset, num_nodes, sink_vertex, num_tasks):
    for j in range(num_nodes):
        node_capacity = num_tasks
        flow_solver.add_edge(node_offset + j, sink_vertex, node_capacity, 0)


def _extract_assignments(flow_solver, task_list, compute_nodes, task_offset, node_offset):
    assignments = {}
    for i, task in enumerate(task_list):
        task_vertex = task_offset + i
        for edge in flow_solver.graph[task_vertex]:
            is_node_edge = node_offset <= edge.v < node_offset + len(compute_nodes)
            if is_node_edge and edge.capacity == 0:  # means flow was used
                task_id = task.get("id")
                node_idx = edge.v - node_offset
                node_id = compute_nodes[node_idx].get("id")
                assignments[task_id] = node_id
                break
    return assignments


def assign_tasks_to_nodes(data):
    task_list = data.get("tasks", [])
    compute_nodes = data.get("nodes", [])
    execution_costs = data.get("exec_cost", {})

    if not task_list or not compute_nodes:
        return {"assignments": {}, "total_cost": 0}

    # 1) Build solver with graph
    flow_solver, source_vertex, sink_vertex, task_offset, node_offset = _build_solver(task_list, compute_nodes, execution_costs)

    # 2) Run solver
    num_tasks = len(task_list)
    _, total_cost = flow_solver.solve_flow(source_vertex, sink_vertex, max_flow=num_tasks)

    # 3) Extract assignments
    assignments = _extract_assignments(flow_solver, task_list, compute_nodes, task_offset, node_offset)

    return {"assignments": assignments, "total_cost": total_cost}


def sample_task_data():
    return {
        "tasks": [
            {"id": "T1", "cpu": 2, "ram": 4, "deadline": 2},
            {"id": "T2", "cpu": 1, "ram": 2, "deadline": 3}
        ],
        "nodes": [
            {"id": "N1", "cpu_capacity": 5, "ram_capacity": 6},
            {"id": "N2", "cpu_capacity": 3, "ram_capacity": 3}
        ],
        "exec_cost": {
            "T1": {"N1": 4, "N2": 6},
            "T2": {"N1": 3, "N2": 2}
        }
    }


def main():
    input_data = sample_task_data() 
    result = assign_tasks_to_nodes(input_data) 
    print(json.dumps(result, indent=2))  


if __name__ == "__main__":
    main()
