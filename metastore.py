import rpyc
import sys

'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues - 
1. The file being modified has missing blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''


class ErrorResponse(Exception):
    def __init__(self, message):
        super(ErrorResponse, self).__init__(message)
        self.error = message

    def missing_blocks(self, hashlist):
        self.error_type = 1
        self.missing_blocks = hashlist

    def wrong_version_error(self, version):
        self.error_type = 2
        self.current_version = version

    def file_not_found(self):
        self.error_type = 3


'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''


class MetadataStore(rpyc.Service):
    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """

    def __init__(self, config):
        self.fileMap, self.fileVersion, self.hashLocation = {}, {}, {}
        self.blockStores = {}
        self.get_config_data(config)

    '''
        ModifyFile(f,v,hl): Modifies file f so that it now contains the
        contents refered to by the hashlist hl.  The version provided, v, must
        be exactly one larger than the current version that the MetadataStore
        maintains.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''

    def exposed_modify_file(self, filename, version, hashLoc):
        hashLoc = list(hashLoc)
        self.log('client modifying', filename)
        if not self.is_version_valid(filename, version):
            self.log(filename, 'version error')
            response = ErrorResponse('wrong version number')
            response.wrong_version_error(self.fileVersion.get(filename, 0))
            raise response

        missingBlocks = self.compute_missing_blocks(hashLoc)
        self.log('missing blocks:', missingBlocks)
        if len(missingBlocks) > 0:
            self.log('some blocks are missing, report to client')
            response = ErrorResponse('missing blocks')
            response.missing_blocks(missingBlocks)
            raise response
        else:
            self.fileVersion[filename] = version
            self.fileMap[filename] = [pair[0] for pair in hashLoc]
            for pair in hashLoc:
                hashkey, location = pair[0], pair[1]
                self.hashLocation[hashkey] = location
            self.log('no missing blocks.', filename, 'modification accepted')

    '''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.
    
        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''

    def exposed_delete_file(self, filename, version):
        self.log('client deleting', filename)
        if filename not in self.fileVersion:
            self.log('delete failed, not found')
            response = ErrorResponse('Not Found')
            response.file_not_found()
            raise response

        if not self.is_version_valid(filename, version):
            self.log('delete failed, version error')
            response = ErrorResponse('wrong version number')
            curVersion = self.fileVersion.get(filename, 0)
            response.wrong_version_error(curVersion)
            raise response

        if filename in self.fileMap:
            self.fileMap.pop(filename)
        self.fileVersion[filename] = version
        self.log('delete', filename, 'success')

    '''
        (v,hl) = ReadFile(f): Reads the file with filename f, returning the
        most up-to-date version number v, and the corresponding hashlist hl. If
        the file does not exist, v will be 0.
    
        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''

    def exposed_read_file(self, filename):
        version = self.fileVersion.get(filename, 0)
        hashlist = self.fileMap.get(filename, [])
        hashLoc = [(hashkey, self.hashLocation[hashkey]) for hashkey in hashlist]
        self.log('client reading', filename, (version, hashLoc))
        return (version, hashLoc)

    def is_version_valid(self, filename, version):
        return version == self.fileVersion.get(filename, 0) + 1

    def compute_missing_blocks(self, hashLoc):
        missingBlocks = []
        for pair in hashLoc:
            hashkey, targetBlockStore = pair[0], self.blockStores[pair[1]]
            with rpyc.connect(targetBlockStore[0], targetBlockStore[1]) as conn:
                if not conn.root.has_block(hashkey):
                    missingBlocks.append(pair)
        return missingBlocks

    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)

    def get_config_data(self, config):
        with open(config, 'r') as cfg:
            cont = cfg.read()
        for line in cont.split('\n'):
            if len(line) == 0:
                continue
            pair = line.split(': ')
            if pair[0] == 'B':
                self.numBlockStores = int(pair[1])
            elif pair[0] == 'metadata':
                continue
            else:
                blockStoreNum = int(pair[0][5:])
                addr = pair[1].split(':')
                self.blockStores[blockStoreNum] = (addr[0], int(addr[1]))

    def log(self, *args):
        print(*args)


if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer

    server = ThreadedServer(MetadataStore(sys.argv[1]), port=6000)
    # server = ThreadedServer(MetadataStore('config.txt'), port=6000)
    server.start()
