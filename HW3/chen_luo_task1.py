from __future__ import division
from pyspark import SparkContext
import time
import sys

start = time.time()

sc = SparkContext()
rdd = sc.textFile(sys.argv[1])
header = rdd.first()
data = rdd.filter(lambda row:row != header).map(lambda x:x.split(","))

# unique business_ids 24732
business_ids = data.map(lambda x:x[1]).distinct().collect()
# print(business_ids.take(5))
business = list(business_ids)
business.sort()

#unique user_ids 11270
user_ids = data.map(lambda x:x[0]).distinct().collect()
user = list(user_ids)
user.sort()
# print(user_ids.take(5))
# print(user_ids.count())
dicB = {}
for i, e in enumerate(business):
    dicB[e] = i

dicU = {}
for i, e in enumerate(user):
    dicU[e] = i

vu = sc.broadcast(dicU)

mat_rdd = data.map(lambda x: (x[1], [vu.value[x[0]]])).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0])
matrix = mat_rdd.collect()

# print(matrix)


m = len(user)  # m: the number of the bins


# hash function:
def minhash(x, has):
    a = has[0]
    b = has[1]
    p = has[2]
    # return ((a * x + b) % p) % m
    return min([((a * e + b) % p) % m for e in x[1]])


hashes = [[29, 43, 709], [1069, 457, 163], [37, 101, 97], [171, 12, 2063], \
          [542, 2780, 10289], [862, 123, 9817], [212, 3, 5393], [142, 7173, 6997], \
          [982, 19, 10861], [333, 709, 191], [183, 523, 5503], [972, 102, 659], \
          [199,6607,491], [241,619,1033], [983,991,587], [233,41,991], \
          [1303,59,5843], [419,53,9001]]


signatures = mat_rdd.map(lambda x: (x[0], [minhash(x, has) for has in hashes]))
# print(signatures.take(5))

n = len(hashes)  # the size of the signature column
b = 9
r = int(n / b)


def sig(x):
    res = []
    for i in range(b):
        res.append(((i, tuple(x[1][i * r:(i + 1) * r])), [x[0]]))
    return res


def pairs(x):
    res = []
    length = len(x[1])
    whole = list(x[1])
    whole.sort()
    for i in range(length):
        for j in range(i + 1, length):
            res.append(((whole[i], whole[j]), 1))
    return res


cand = signatures.flatMap(sig).reduceByKey(lambda x, y: x + y).filter(lambda x: len(x[1]) > 1).flatMap(pairs) \
    .reduceByKey(lambda x, y: x).map(lambda x: x[0])
# print(cand.count())
# print(cand.take(5))



def jaccard(x):
    a = set(matrix[dicB[x[0]]][1])
    b = set(matrix[dicB[x[1]]][1])
    inter = a & b
    union = a | b
    jacc = len(inter) / len(union)
    return (x[0], x[1], jacc)


result = cand.map(jaccard).filter(lambda x: x[2] >= 0.5).sortBy(lambda x: x[1]).sortBy(lambda x: x[0])


re = result.collect()


f = open(sys.argv[2], "w")
f.write("business_id_1" + ", " + "business_id_2" + ", " + "similarity" + "\n")
for i in re:
    f.write(str(i[0]) + ", " + str(i[1]) + ", " + str(i[2]) + "\n")
f.close()

end = time.time()
print("Running time: ")
print(str(end - start))
