from pyspark import SparkContext
import json
import time
import sys

# start=time.time()

sc = SparkContext()

line1 = sc.textFile(sys.argv[1]).coalesce(5).map(lambda x: json.loads(x))
line2 = sc.textFile(sys.argv[2]).coalesce(5).map(lambda x: json.loads(x))

bid_star = line1.map(lambda x:(x['business_id'], x['stars']))
bid_city = line2.map(lambda x:(x['business_id'], x['city']))
city_star = bid_city.join(bid_star).map(lambda x:x[1]).aggregateByKey((0,0), lambda U,v:(U[0]+v, U[1]+1), lambda U1,U2:(U1[0]+U2[0], U1[1]+U2[1])).map(lambda x:(x[0],float(x[1][0])/x[1][1]))


sorted_rdd = city_star.sortBy(lambda x:x[0], True).sortBy(lambda x:x[1], False)
start1=time.time()

result1 = sorted_rdd.collect()[:10]
# print(result1)
end1=time.time()
# print("first method time:", end1-start1)

start2=time.time()
result2 = sorted_rdd.take(10)
# print(result2)
end2=time.time()
# print("second method time:", end2-start2)

# end=time.time()
# print("running time is :", end-start)

f1 = open(sys.argv[3],'w')
f1.write('city' + ',' + 'stars' + '\r\n')
for i in range(len(result1)):
    f1.write(result1[i][0] + ',' + str(result1[i][1]) + '\r\n')


output = [{"m1":end1-start1, "m2":end2-start2, "explanation":"The second method is faster than the first one because sortBy() is O(nlgn) while top() is O(n)"}]
with open(sys.argv[4], 'w') as f:
    f.write(json.dumps(output))
