from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import binascii
import time
import sys
# Create a local StreamingContext with two working thread and batch interval of 1 second
# sc = SparkContext("local[2]", "NetworkWordCount")
sc = SparkContext.getOrCreate()

ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream("localhost", int(sys.argv[1]))
path = sys.argv[2]
f = open(path,'w')
f.write("Time,Gound Truth,Estimation\n")
f.close()

data = lines.map(lambda x:json.loads(x)).map(lambda x:(x["city"],int(binascii.hexlify(x["city"].encode('utf8')),16)))
# # Reduce last 30 seconds of data, every 10 seconds
# windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)
def trailZeros(binNum):
    count = 0
    binNum = binNum[2:]
    for i in range(len(binNum) - 1, -1, -1):
        if binNum[i] == "0":
            count += 1
        else:
            break
    if count == len(binNum):
        return 1
    else:
        # print(count)
        return count

def estimatedCity(y):
    y = y.collect()
    file = open(path,'a')
    avg_list = []
    for group in hashes:
        max_list = []
        for f in group:
            Max = -1
            a = f[0]
            b = f[1]
            p = f[2]
            for x in y:
                hashedNum = ((a * x[1] + b) % p) % 200
                r = trailZeros(bin(hashedNum))
                if Max==-1 or Max<r:
                    Max = r
            est = 2 ** Max
            max_list.append(est)
        avg = float(sum(max_list)/len(max_list))
        print(avg)
        avg_list.append(avg)
    avg_list=sorted(avg_list)
    median = avg_list[5]   # 9 groups, 5 is the midian
    y = set(y)
    # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+','+str(len(y))+','+str(median))
    file.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+','+str(len(y))+','+str(median)+'\n')
    file.close()
result = data.window(30,10).foreachRDD(lambda x:estimatedCity(x))


hashes = [[[29, 43, 709], [1069, 457, 163], [37, 101, 97],[73,79,83],[557,5,317]],\
          [[171, 12, 2063],[1303, 59, 5843],[419, 53, 9001],[293,101,743],[1201,563,331]],\
          [[542, 2780, 10289], [862, 123, 9817], [212, 3, 5393],[1747,967,19],[12,982,2063]],\
          [[142, 7173, 6997],[199, 6607, 491],[241, 619, 1033],[2963,222,5323],[192,67,7039]],\
        [[142, 7173, 6997],[199, 6607, 491],[241, 619, 1033],[2963,222,5323],[192,67,7039]],\
        [[342, 713, 9323],[1991, 607, 491],[171, 491, 163],[2963,222,5323],[192,67,7039]],\
        [[1212, 773, 8753],[19, 671, 491],[241, 862, 97],[296,212,19],[1921,671,97]],\
        [[1422, 17, 6069],[129, 6607, 709],[241, 222, 83],[23,322,331],[12,6727,9001]],\
          [[982, 19, 10861], [333, 709, 191], [183, 523, 5503],[888,120,8929],[872,111,9973]]]

ssc.start()
ssc.awaitTermination()

