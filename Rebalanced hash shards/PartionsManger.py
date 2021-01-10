from hashlib import md5
import math


class PartionsManger(object):

    def __init__(self, nodes, partions_number):

        self.partions_number = partions_number
        self.nodes = nodes

    @staticmethod
    def hash(val):
        m = md5(val.encode())
        return int(m.hexdigest(), 16)

    def get_node(self, val):
        hashed_val = self.hash(val) % self.partions_number
        selected_node_pos = math.ceil(hashed_val % len(self.nodes))
        return self.nodes[selected_node_pos], hashed_val

    def add_node(self, node):
        moved_partions = {}
        old_nodes_len = len(self.nodes)
        for i in range(0,len(self.nodes)):
            moved_partions[i] = {
                'connection' : self.nodes[i],
                'partions' : []
            }
        self.nodes.append(node)

        for hashed_value in range(self.partions_number):
            old_partion_node = math.ceil(hashed_value % old_nodes_len)
            new_partion_node = math.ceil(hashed_value % (old_nodes_len + 1))
            if(old_partion_node != new_partion_node):
                moved_partions.get(old_partion_node).get('partions').append(
                    {
                        'new_node': self.nodes[new_partion_node],
                        'hashed_value': hashed_value
                    })
        return moved_partions


            
