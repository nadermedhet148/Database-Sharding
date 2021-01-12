from hashlib import md5

class RangeShardManger : 
    def __init__(self, connections):
        self.connections = connections

    @staticmethod
    def hash(val):
        m = md5(val.encode())
        return int(m.hexdigest(), 16)

    def getNode(self,name):
        hash_value = self.hash(name)
        selectNodeIndex = hash_value % len(self.connections)
        return self.connections[selectNodeIndex] , hash_value