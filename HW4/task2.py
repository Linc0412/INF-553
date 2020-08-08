import os
import sys
from pyspark import SparkContext
import time
from itertools import combinations
import networkx as nx


start = time.time()

filter_threshold = int(sys.argv[1])
sc = SparkContext()
rdd = sc.textFile(sys.argv[2])
header = rdd.first()
data = rdd.filter(lambda row:row != header).map(lambda x:x.split(",")).groupByKey().map(lambda x:(x[0], list(x[1]))).sortBy(lambda x:x[0])
edges = data.cartesian(data).map(lambda x:((x[0][0],x[1][0]), len(set(x[0][1]).intersection(set(x[1][1]))))).filter(lambda x:x[0][0]!=x[0][1] and x[1]>=filter_threshold).map(lambda x:x[0])
# print(edges.take(5))
# print(edges.count())


# (user1_id, [other_user_id])
edgeUsers = edges.groupByKey().map(lambda x:(x[0],list(x[1])))
# print(edgeUsers.count())
# print(edgeUsers.take(5))
# nodes = edgeUsers.map(lambda x:x[0]).collect()
# print(nodes)

edgeUsers_list = edgeUsers.collect()

dicTree = {}
for i in edgeUsers_list:
    dicTree[i[0]] = i[1]
# print(dicTree)
#

def BETW(root):
    # find bfs tree, root = x[0]
    head = root
    bfs_order = []
    bfs_order.append(head) # the list of bfs traversal order ['123']
    # print(bfs_order)
    treeLevel = {head: 0}  # {node: level}
    dicParent = {}  # {childNode: [parent nodes in bfs graph]}

    for head in bfs_order:
        children = dicTree[head]
        # print(children)
        for child in children:
            if child not in bfs_order:
                bfs_order.append(child)
                treeLevel[child] = treeLevel[head] + 1
                dicParent[child] = [head]
            else:
                if treeLevel[child] == treeLevel[head] + 1:
                    dicParent[child] += [head]

    # print(len(bfs_order))

    # calculate betweenness of every bfs
    dicNodeValue = {}
    for i in range(len(bfs_order) - 1, -1, -1):
        node = bfs_order[i]
        if node not in dicNodeValue:
            dicNodeValue[node] = 1
        if node in dicParent:
            parents = dicParent[node]
            parentsSize = len(parents)
            for parent in parents:
                if parent not in dicNodeValue:
                    dicNodeValue[parent] = 1
                credits = float(dicNodeValue[node] / parentsSize)
                dicNodeValue[parent] += credits
                edgeBetw = (min(node, parent), max(node, parent))
                yield (edgeBetw, credits / 2)


# ((user1_id, user2_id), betweenness)
betweenness = edgeUsers.flatMap(lambda x: BETW(x[0])) \
    .reduceByKey(lambda x, y: x + y).map(lambda x:(sorted(x[0]), x[1])) \
    .sortBy(lambda x: x[0][0]).sortBy(lambda x: x[1], ascending=False)
# betweenness = edgeUsers.flatMap(lambda x: GN(x[0]))
# print(betweenness.take(5))


f1 = open(sys.argv[3], "w")
if betweenness:
    for e in betweenness.collect():
        f1.write(str(tuple(e[0])))
        f1.write(", ")
        f1.write(str(e[1]))
        f1.write("\n")
f1.close()



# Community Detection
graph = nx.Graph()
graph.add_edges_from(edges.collect())

edgeNumber = edges.count()
nodeList = edgeUsers.map(lambda x:x[0]).collect()
# print(len(nodeList))
# print(edgeNumber)


dicK = {}
for node in nodeList:
    dicK[node] = len(dicTree[node])

modularity = 0
dicModu = {}
for i in nodeList:
    for j in nodeList:
        if j in dicTree[i]:
            A = 1
        else:
            A = 0
        # ki = len(dicTree[i])
        # kj = len(dicTree[j])
        tmp = (A-0.5*dicK[i]*dicK[j]/edgeNumber)/(2*edgeNumber)
        dicModu[(i, j)] = tmp
        modularity += tmp  # null graph modularity
# print(modularity)

communities = [x for x in nx.connected_components(graph)]
# print(communities)# [{'asd','qew'}]
# print(len(communities)) # 13
# print(dicModu)
# print(nx.connected_components(graph))

maxModu = (modularity, communities)
# commSize = nx.number_connected_components(graph)
# commSize = 1
# print(maxModu)
#
def calcModularity(community):
    res = 0
    for i in community:
        for j in community:
            res += dicModu[(i, j)]
    return res

def dfs(i, j, visited):
    # if i == j:
    #     return True
    visited.add(i)
    children = dicTree[i]
    for child in children:
        if child == j:
            return True
        if child not in visited:
            if dfs(child, j, visited):
                return True
    return False


betweennessList = betweenness.collect()

for e in betweennessList:
    # print(e)
    i = e[0][0]
    j = e[0][1]
    graph.remove_edge(i, j)
    newModu = 0
    # tmpSize = nx.number_connected_components(graph)
    dicTree[i].remove(j)
    dicTree[j].remove(i)
    # community1 = bfs(i, set([]))
    if dfs(i, j, set([])):
        continue

    communities = [x for x in nx.connected_components(graph)]
    # sorted(communities)
    for community in communities:
        newModu += calcModularity(community)
    modularity = newModu
    # print("modu")
    # print(modularity)
    if maxModu[0] < modularity:
        maxModu = (modularity, communities)
# print("maxModu")
# print(maxModu[1])
commList = []
for i in maxModu[1]:
    commList.append(sorted(list(i)))
# print(len(commList))
# print(type(commList[0]))
# print(commList[0])
commList = sorted(commList)
commList = sorted(commList, key=lambda x:len(x))
# print(commList)
f2 = open(sys.argv[4], "w")
for i in commList:
    if len(i)>1:
        for j in range(len(i)-1):
            f2.write("'" + i[j] + "'" + ",")
        f2.write("'" + i[-1] + "'" + "\n")
    else:
        f2.write("'" + str(i[0]) + "'" + "\n")
f2.close()



end = time.time()
print("Running time:",end-start)