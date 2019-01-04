import rpyc
import sys
import threading

"""
The BlockStore service is an in-memory data store that stores blocks of data,
indexed by the hash value.  Thus it is a key-value store. It supports basic
get() and put() operations. It does not need to support deleting blocks of
data–we just let unused blocks remain in the store. The BlockStore service only
knows about blocks–it doesn’t know anything about how blocks relate to files.
"""


class BlockStore(rpyc.Service):
    """
    Initialize any datastructures you may need.
    """

    def __init__(self):
        self.blockMap = {}

    """
        store_block(h, b) : Stores block b in the key-value store, indexed by
        hash value h

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    """

    def exposed_store_block(self, h, block):
        self.log('Storing', h, block)
        self.blockMap[h] = block

    """
    b = get_block(h) : Retrieves a block indexed by hash value h

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    """

    def exposed_get_block(self, h):
        self.log('Getting', self.blockMap[h])
        return self.blockMap[h]

    """
        True/False = has_block(h) : Signals whether block indexed by h exists
        in the BlockStore service

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    """

    def exposed_has_block(self, h):
        self.log('Querying', h, h in self.blockMap)
        return h in self.blockMap.keys()

    def exposed_ping(self):
        pass

    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)

    def log(self, *args):
        print(*args)


if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer

    port = int(sys.argv[1])
    #port = 5000
    server = ThreadedServer(BlockStore(), port=port)
    server.start()
