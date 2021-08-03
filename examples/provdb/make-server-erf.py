import sys

def make_server_erf(num_nodes, servers_per_node):
    print('cpu_index_using: logical')
    num_ranks = num_nodes*servers_per_node
    cpu_per_rank = 168/servers_per_node
    for rank in range(0, num_ranks):
        host = rank / servers_per_node
        cpu_min = (rank - host*servers_per_node) * cpu_per_rank
        cpu_max = cpu_min + cpu_per_rank - 1
        print('rank: %d: { host: %d; cpu: { %d-%d } } : app 0' % (rank, host+1, cpu_min, cpu_max))

if __name__ == '__main__':
    make_server_erf(int(sys.argv[1]),
                    int(sys.argv[2]))
