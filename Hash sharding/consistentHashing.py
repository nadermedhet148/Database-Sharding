from hashlib import md5
from bisect import bisect

class RingObject():
    def __init__(self , name , object):
        self.name = name
        self.object = object

class Ring(object):

    def __init__(self, server_list):
        nodes = self.generate_nodes(server_list, len(server_list))

        hnodes = [self.hash(node) for node in nodes]
        
        hnodes.sort()

        self.num_replicas =  len(server_list)
        self.nodes = nodes
        self.hnodes = hnodes
        self.nodes_map = {self.hash(node): nodes.get(node) for node in nodes}

    @staticmethod
    def hash(val):
        m = md5(val.encode())
        return int(m.hexdigest(), 16)

    @staticmethod
    def generate_nodes(server_list, num_replicas):
        nodes = {}
        for i in range(0,num_replicas):
            for server in server_list:
                nodes[str(i) + '' +server.name] = server
        return nodes

    def get_node(self, val):
        pos = bisect(self.hnodes, self.hash(val))
        if pos == len(self.hnodes):
            return { "connection" : self.nodes_map[self.hnodes[0]].object , "hashCode" : self.hnodes[0]  }
        else:
            return { "connection" : self.nodes_map[self.hnodes[pos]].object , "hashCode" : self.hnodes[pos]  }

