package rx.distributed.raft.events.rpc.request

import rx.distributed.raft.events.client.ClientEvent

trait ClientRpcRequest extends RpcRequest[ClientEvent]
