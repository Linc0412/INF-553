from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import time
import binascii
import sys

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, 10)
lines = ssc.socketTextStream("localhost", int(sys.argv[1]))
path = sys.argv[2]
f = open(path,'w')
f.write("Time,FPR\n")
f.close()

data = lines.map(lambda x:json.loads(x)).map(lambda x:(x["city"],int(binascii.hexlify(x["city"].encode('utf8')),16)))

bit_matrix = [False]*200
city_matrix = []
fp = 0
tn = 0
# print(bit_matrix)
# hash function:
def f_hash(y):
    global fp
    global tn
    global city_matrix
    global bit_matrix
    global FPR
    y = y.collect()
    file = open(path, 'a')
    for x in y:
        r = True
        for f in hashes:
            a = f[0]
            b = f[1]
            p = f[2]
        # return ((a * x + b) % p) % m
            i = ((a * x[1] + b) % p) % 200
            r = r&bit_matrix[i]
        if r is False:
            for f in hashes:
                a = f[0]
                b = f[1]
                p = f[2]
            # return ((a * x + b) % p) % m
                i = ((a * x[1] + b) % p) % 200
                bit_matrix[i] = True
            if x[0] not in city_matrix:
                tn+=1
        else:
             if x[0] not in city_matrix:
                    fp+=1
        city_matrix.append(x[0])
    if (fp+tn)==0:
        FPR = 0
    if (fp+tn)!=0:
        FPR = float(fp)/(fp+tn)
    # print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+','+str(FPR))
    file.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+','+str(FPR) + '\n')
    file.close()
# def check_tn(x):
#     global tn
#     for f in hashes:
#         a = f[0]
#         b = f[1]
#         p = f[2]
#         i = ((a * x[1] + b) % p) % 200
#         if bit_matrix[i] is False:
#             tn += 1
#             break
#     # print(tn)
#     return tn
#
# def check_fp(x):
#     global fp
#     ret = True
#     for f in hashes:
#         a = f[0]
#         b = f[1]
#         p = f[2]
#         i = ((a * x[1] + b) % p) % 200
#         ret = ret & bit_matrix[i]
#     if ret is True:
#         if x[1] not in city_matrix:
#             fp += 1
#     # print(fp)
#     return fp

hashes = [[29, 43, 709],[1069, 457, 163], [37, 101, 97], [171, 12, 2063]]


# signatures = data.map(lambda x: (x[0], [f_hash(x, has) for has in hashes]))

result = data.foreachRDD(lambda x: f_hash(x))



ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate