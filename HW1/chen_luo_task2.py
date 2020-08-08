from pyspark import SparkContext
import json
import time
import sys

start=time.time()

sc = SparkContext()

start1=time.time()
lines = sc.textFile(sys.argv[1])
Task_f = lines.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'], 1)).\
    reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1], False)
end1=time.time()
exe_time1=end1-start1
# print("exe_time1:", exe_time1)

pre_Part = Task_f.getNumPartitions()
num_items = lines.mapPartitions(lambda x: [sum(1 for i in x)])
# print(num_items.collect())
# print(pre_Part)



start2=time.time()
re_lines = lines.coalesce(int(sys.argv[3]))
Task_f2 = re_lines.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'], 1)).\
    reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1], False)
end2=time.time()
exe_time2 = end2-start2
# print("exe_time2: ", exe_time2)

cur_Part = Task_f2.getNumPartitions()
num_items2 = re_lines.mapPartitions(lambda x: [sum(1 for i in x)])
# print(num_items2.collect())
# print(cur_Part)

output = [{"default":{"n_partition":pre_Part, "n_items":num_items.collect(), "exe_time":exe_time1},
           "customized":{"n_partition":cur_Part, "n_items":num_items2.collect(), "exe_time":exe_time2},
           "explanation":"Default shows the original number of partitions, the number of items for each partition and its execution time. The customized one shows a different result after I change the function of the partition to make fully use of SSD."}]

with open(sys.argv[2], 'w') as f:
    f.write(json.dumps(output))

end=time.time()
print("running time: ", end-start)