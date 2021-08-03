import sys

def make_client_erf(num_server_nodes, num_nodes, clients_per_node):
    print('cpu_index_using: logical')
    num_ranks = num_nodes*clients_per_node
    cpu_per_rank = 168 // clients_per_node
    for rank in range(0, num_ranks):
        host = rank // clients_per_node
        cpu_min = (rank - host*clients_per_node) * cpu_per_rank
        cpu_max = cpu_min + cpu_per_rank - 1
        print('rank: %d: { host: %d; cpu: { %d-%d } } : app 0' % (rank, (host+num_server_nodes)+1, cpu_min, cpu_max))

if __name__ == '__main__':
    make_client_erf(int(sys.argv[1]),
                    int(sys.argv[2]),
                    int(sys.argv[3]))
