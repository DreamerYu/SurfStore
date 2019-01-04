import rpyc
import hashlib
import os
import sys
import time

"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""


class SurfStoreClient():
    """
    Initialize the client and set up connections to the block stores and
    metadata store using the config file
    """

    def __init__(self, config, loc_method):
        self.get_config_data(config)
        self.loc_method = loc_method
        if self.loc_method == 'hash':
            self.findServer = lambda h: self.findServer_hash(h)
        elif self.loc_method == 'dist':
            self.findServer = lambda h: self.findServer_dist(h)
        else:
            self.log('invalid blockstore locating method!')
            sys.exit(1)

    """
    upload(filepath) : Reads the local file, creates a set of 
    hashed blocks and uploads them onto the MetadataStore 
    (and potentially the BlockStore if they were not already present there).
    """

    def upload(self, filepath):
        if not os.path.isfile(filepath):
            sys.stdout.write('Not Found')
            return
        filename = self.getFileName(filepath)
        with rpyc.connect(self.metadataStore['host'], self.metadataStore['port']) as conn:
            (curVersion, _) = conn.root.read_file(filename)
            with open(filepath, 'rb') as file:
                content = file.read()
            (hashlist, hashBlockDict) = self.getHashBlock(content)
            if self.loc_method == 'dist':
                self.nearestBlockStore = self.findNearest()
            hashLoc = self.generateHashInfo(hashlist)
            self.modifyFile(conn, filename, curVersion + 1, hashLoc, hashBlockDict)

    """
    delete(filename) : Signals the MetadataStore to delete a file.
    """

    def delete(self, filename):
        with rpyc.connect(self.metadataStore['host'], self.metadataStore['port']) as conn:
            (curVersion, hashlist) = conn.root.read_file(filename)
            if curVersion == 0 and len(hashlist) == 0:
                sys.stdout.write('Not Found')
                return
            self.deleteFile(conn, filename, curVersion + 1)

    """
        download(filename, dst) : Downloads a file (f) from SurfStore and saves
        it to (dst) folder. Ensures not to download unnecessary blocks.
    """

    def download(self, filename, fileroot):
        with rpyc.connect(self.metadataStore['host'], self.metadataStore['port']) as conn:
            (_, hashLoc) = conn.root.read_file(filename)
            hashLoc = list(hashLoc)
        if len(hashLoc) == 0:
            sys.stdout.write('Not Found')
            return
        absfilepath = fileroot + '/' + filename
        (hashval, hashBlockDict) = self.getFileBlock(absfilepath)
        finalcontent = []
        for pair in hashLoc:
            if pair[0] in hashval:
                block = hashBlockDict[pair[0]]
            else:
                block = self.getBlockFromServer(pair[0], pair[1])
            finalcontent.append(block)
        self.writeToFile(finalcontent, absfilepath)
        sys.stdout.write('OK')

    """
     Use eprint to print debug messages to stderr
     E.g - 
     self.eprint("This is a debug message")
    """

    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)

    def modifyFile(self, conn, filename, version, hashLoc, hashBlockDict):
        while True:
            try:
                conn.root.modify_file(filename, version, hashLoc)
            except Exception as reply:
                if 'error_type' not in dir(reply):
                    pass
                if reply.error_type == 1:
                    missingBlocks = eval(reply.missing_blocks)
                    self.storeMissingBlocks(missingBlocks, hashBlockDict)
                elif reply.error_type == 2:
                    version = reply.current_version + 1
            else:
                sys.stdout.write('OK')
                break

    def deleteFile(self, conn, filename, version):
        while True:
            try:
                conn.root.delete_file(filename, version)
            except Exception as reply:
                if 'error_type' not in dir(reply):
                    pass
                elif reply.error_type == 2:
                    version = reply.current_version + 1
                else:
                    sys.stdout.write('Not Found')
                    break
            else:
                sys.stdout.write('OK')
                break

    def getFileName(self, filepath):
        return filepath[filepath.rfind('/') + 1:]

    def writeToFile(self, finalcontent, absfilepath):
        with open(absfilepath, 'wb') as file:
            for block in finalcontent:
                file.write(block)

    def generateHashInfo(self, hashlist):
        return [(hashval, self.findServer(hashval)) for hashval in hashlist]

    def storeMissingBlocks(self, hashLoc, hashBLockDict):
        for pair in hashLoc:
            hashkey, targetBlockStore = pair[0], self.blockStores[pair[1]]
            block = hashBLockDict[hashkey]
            with rpyc.connect(targetBlockStore[0], targetBlockStore[1]) as conn:
                conn.root.store_block(hashkey, block)

    def get_config_data(self, config):
        self.metadataStore, self.blockStores = {}, {}
        with open(config, 'r') as cfg:
            cont = cfg.read()
        for line in cont.split('\n'):
            if len(line) == 0:
                continue
            pair = line.split(': ')
            if pair[0] == 'B':
                self.numBlockStores = int(pair[1])
            elif pair[0] == 'metadata':
                addr = pair[1].split(':')
                self.metadataStore['host'] = addr[0]
                self.metadataStore['port'] = int(addr[1])
            else:
                blockStoreNum = int(pair[0][5:])
                addr = pair[1].split(':')
                self.blockStores[blockStoreNum] = (addr[0], int(addr[1]))

    def getHashBlock(self, content):
        i, hashlist, hashBlockDict = 0, [], {}
        while i < len(content):
            block = content[i: i + 4096]
            hashval = hashlib.sha256(block).hexdigest()
            hashlist.append(hashval)
            hashBlockDict[hashval] = block
            i += 4096
        return (hashlist, hashBlockDict)

    def getFileBlock(self, absfilepath):
        if not os.path.isfile(absfilepath):
            return ([], {})
        with open(absfilepath, 'rb') as file:
            content = file.read()
        return self.getHashBlock(content)

    def getBlockFromServer(self, hashkey, location):
        targetBlockStore = self.blockStores[location]
        with rpyc.connect(targetBlockStore[0], targetBlockStore[1]) as conn:
            return conn.root.get_block(hashkey)

    def findServer_hash(self, h):
        self.log('finding server using hash:', int(h, 16) % self.numBlockStores)
        return int(h, 16) % self.numBlockStores

    def findServer_dist(self, *args):
        self.log('finding nearest server:', self.nearestBlockStore)
        return self.nearestBlockStore

    def findNearest(self):
        RTTs = {}
        for blockStoreNum, targetBlockStore in self.blockStores.items():
            with rpyc.connect(targetBlockStore[0], targetBlockStore[1]) as conn:
                start = time.time() # on Windows, should be start = time.clock()
                conn.root.ping()
                end = time.time() # on Windows, should be start = time.clock()
            RTTs[end - start] = blockStoreNum
        return RTTs[min(RTTs.keys())]

    def log(self, *args):
        print(*args)


if __name__ == '__main__':
    client = SurfStoreClient(sys.argv[1], sys.argv[2])
    operation = sys.argv[3]
    if operation == 'upload':
        client.upload(sys.argv[4])
    elif operation == 'download':
        client.download(sys.argv[4], sys.argv[5])
    elif operation == 'delete':
        client.delete(sys.argv[4])
    else:
        print("Invalid operation")
