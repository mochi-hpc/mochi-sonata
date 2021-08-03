import sys
import json
import copy

def make_server_config(infile, num_dbs):
    with open(infile) as f:
        base_config = json.loads(f.read())
    pool_config = base_config["argobots"]["pools"][1]
    es_config = base_config["argobots"]["xstreams"][1]
    del base_config["argobots"]["pools"][1]
    del base_config["argobots"]["xstreams"][1]
    for i in range(0, num_dbs):
        pool_name = "pool_s"+str(i)
        es_name = "stream_s"+str(i)
        base_config["argobots"]["pools"].append(
                copy.deepcopy(pool_config))
        base_config["argobots"]["pools"][-1]["name"] = pool_name
        base_config["argobots"]["xstreams"].append(
                copy.deepcopy(es_config))
        base_config["argobots"]["xstreams"][-1]["name"] = es_name
        base_config["argobots"]["xstreams"][-1]["scheduler"]["pools"] = [ pool_name ]
    print(json.dumps(base_config, indent=2))



if __name__ == '__main__':
    make_server_config(sys.argv[1], int(sys.argv[2]))
