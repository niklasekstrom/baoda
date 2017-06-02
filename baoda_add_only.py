from typing import Any, List, Dict, NamedTuple, Sequence, Set, FrozenSet

class BranchId(NamedTuple): lt: int; pid: int
class NodeId(NamedTuple): bid: BranchId; length: int
class Node(NamedTuple):
    nid: NodeId; array_state: Sequence[int]
    def __hash__(self) -> int:
        return self.nid.__hash__()
class BlockReq(NamedTuple): bid: BranchId
class BlockRes(NamedTuple): bid: BranchId; cb: BranchId; msn: Node
class StoreReq(NamedTuple): bid: BranchId; node: Node
class StoreRes(NamedTuple): bid: BranchId; cb: BranchId; nid: NodeId
class KnownCommittedReq(NamedTuple): bid: BranchId; nid: NodeId

root_branch_id: BranchId = BranchId(0, 0)
root_node_id: NodeId = NodeId(root_branch_id, 0)
root_node: Node = Node(root_node_id, [])

my_pid: int = 1
members: FrozenSet[int] = frozenset([1, 2, 3])
block_quorum_size: int = 2
store_quorum_size: int = 2
neighbors: FrozenSet[int] = members - frozenset([my_pid])

block_branches: Set[BranchId] = {root_branch_id}
stored_nodes: Set[Node] = {root_node}
known_committed: Set[NodeId] = {root_node_id}

max_stored_up_to: Dict[BranchId, Dict[int, Node]] = {}
node_stored_by: Dict[NodeId, Set[int]] = {}


def current_branch() -> BranchId:
    return max(block_branches)


def max_stored_node() -> Node:
    return max(stored_nodes)


def max_known_committed() -> NodeId:
    return max(known_committed)


def notify(s: str, o: Any) -> None:
    print((s, o))


# noinspection PyUnusedLocal
def send(msg: Any, to: int) -> None:
    pass


def broadcast(msg: Any) -> None:
    for to in neighbors:
        send(msg, to)


def branch() -> None:
    branch_id = BranchId(current_branch().lt + 1, my_pid)
    block_branches.add(branch_id)
    max_stored_up_to[branch_id] = {my_pid: max_stored_node()}
    broadcast(BlockReq(branch_id))


def receive_block_req(req: BlockReq, src: int) -> None:
    if current_branch() < req.bid and current_branch().pid == my_pid:
        notify('Abandoning branch', current_branch())
    block_branches.add(req.bid)
    send(BlockRes(req.bid, current_branch(), max_stored_node()), src)


def receive_block_res(res: BlockRes, src: int) -> None:
    cb = current_branch()
    if res.bid != cb:
        return

    if cb < res.cb:
        notify('Abandoning branch', cb)
        block_branches.add(res.cb)
        return

    msu: Dict[int, Node] = max_stored_up_to[cb]
    msu[src] = res.msn

    if max_stored_node().nid.bid < cb and len(msu) >= block_quorum_size:
        branching_point: Node = max(msu.values())
        array_state: Sequence[int] = branching_point.array_state

        initial_node = Node(NodeId(cb, len(array_state)), array_state)
        stored_nodes.add(initial_node)

        notify('Branch started', cb)

        node_stored_by[initial_node.nid] = {my_pid}
        broadcast(StoreReq(cb, initial_node))


def append(e: int) -> None:
    cb = current_branch()
    msn = max_stored_node()
    if cb.pid != my_pid or msn.nid.bid != cb:
        return

    array_state: List[int] = list(msn.array_state)
    array_state.append(e)

    append_node = Node(NodeId(cb, len(array_state)), array_state)
    stored_nodes.add(append_node)

    node_stored_by[append_node.nid] = {my_pid}
    broadcast(StoreReq(cb, append_node))


def receive_store_req(req: StoreReq, src: int) -> None:
    if req.bid == current_branch():
        stored_nodes.add(req.node)
    send(StoreRes(req.bid, current_branch(), max_stored_node().nid), src)


def receive_store_res(res: StoreRes, src: int) -> None:
    cb = current_branch()
    if res.bid != cb:
        return

    if cb < res.cb:
        notify('Abandoning branch', cb)
        block_branches.add(res.cb)
        return

    nsb: Set[int] = node_stored_by[res.nid]
    nsb.add(src)

    if max_known_committed() < res.nid and len(nsb) >= store_quorum_size:
        mkc = res.nid
        known_committed.add(mkc)
        notify('Max known committed updated', mkc)
        broadcast(KnownCommittedReq(cb, mkc))


# noinspection PyUnusedLocal
def receive_known_committed_req(req: KnownCommittedReq, src: int) -> None:
    cb = current_branch()
    if cb == req.bid:
        known_committed.add(req.nid)
        mkc = req.nid
        notify('Max known committed updated', mkc)


def size_tentative() -> int:
    cb = current_branch()
    msn = max_stored_node()
    assert cb.pid == my_pid and msn.nid.bid == cb
    return msn.nid.length


def get_tentative(index: int) -> int:
    cb = current_branch()
    msn = max_stored_node()
    assert cb.pid == my_pid and msn.nid.bid == cb
    assert index in range(msn.nid.length)
    return msn.array_state[index]


def size_committed() -> int:
    mkc = max_known_committed()
    return mkc.length


def get_committed(index: int) -> int:
    mkc = max_known_committed()
    assert index in range(mkc.length)
    msn = max_stored_node()
    return msn.array_state[index]
