import os
import sys
from pyspark import SparkContext
import time
from itertools import combinations
# import networkx as nx
# os.environ["PYSPARK_SUBMIT_ARGS"]=("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell")
from graphframes import *
from pyspark.sql import SQLContext


start = time.time()
# os.environ["PYSPARK_SUBMIT_ARGS"]=("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell")


sc = SparkContext()
rdd = sc.textFile(sys.argv[2])
header = rdd.first()
data = rdd.filter(lambda row:row != header).map(lambda x:x.split(",")).groupByKey().map(lambda x:(x[0], list(x[1]))).sortBy(lambda x:x[0])
# print(data.take(5))
datas = data.collect()

filter_threshold = int(sys.argv[1])

edges = []
nodes = []
for i,j in datas:
    for k,l in datas:
        if i!=k:
            if len(set(j).intersection(set(l)))>=filter_threshold:
                edges.append((i,k))
                nodes.append((i,))
                nodes.append((k,))
nodes = list(set(nodes))
# print(nodes)
# print(len(nodes))


# G = nx.Graph()
# G.add_edges_from(nodes)
# print(G.number_of_edges())
# print(G.number_of_nodes())
# print(len(nodes))
# print(G.nodes())
# print(G.edges())

#
# dicNode = {}
# for i, e in enumerate(G.nodes()):
#     dicNode[str(i)] = e
# id_Node = list(dicNode.items())
# # print(id_Node)
#
# #reverse dicNode
# new_dict = {v : k for k, v in dicNode.items()}
# # print(new_dict)
#
# id_Edge = []
# for i in G.edges():
#     id_Edge.append((new_dict[i[0]], new_dict[i[1]]))
# # print(id_Edge)
#
#
sqlContext = SQLContext(sc)
vertices = sqlContext.createDataFrame(nodes, ["id"])
edges = sqlContext.createDataFrame(edges, ["src", "dst"])
g = GraphFrame(vertices, edges)
print(g)

result = g.labelPropagation(maxIter=5)
# rdd = result.select("id", "label").rdd.map(lambda x:(x[1],x[0]))
rdd = result.select("id", "label").rdd.map(lambda x:(x[1],x[0])).groupByKey().map(lambda x:sorted(list(x[1]))).sortBy(lambda x:x).sortBy(lambda x:len(x),ascending=True)
# rdd = result.select("id", "label").rdd.map(lambda x:(x[1],x[0])).groupByKey().map(lambda x:sorted(list(x[1])))
# print(rdd.take(5))
# result.groupBy("label").count().show()

re = rdd.collect()
f = open(sys.argv[3], "w")
for i in re:
    if len(i)>1:
        for j in range(len(i)-1):
            f.write("'" + i[j] + "'" + ",")
        f.write("'" + i[-1] + "'" + "\n")
    else:
        f.write("'" + str(i[0]) + "'" + "\n")
f.close()

end = time.time()
print("Running time:",end-start)