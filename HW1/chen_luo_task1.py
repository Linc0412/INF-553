from pyspark import SparkContext
import json
import time
import sys

start=time.time()

sc = SparkContext()

lines = sc.textFile(sys.argv[1])
# print(lines.count())

lines_dict = lines.map(lambda x: json.loads(x))
lines_2018 = lines_dict.filter(lambda x: '2018' in x['date'])
# print(lines_2018.count())

distinct_user = lines_dict.map(lambda x: (x['user_id'], 1)).reduceByKey(lambda x,y:x+y)
# print(distinct_user.count())

# top_10_user = distinct_user.sortBy(lambda x: len(x[1]), False).map(lambda x:(x[0], len(x[1]))).take(10)
top_10_user = distinct_user.top(10, lambda x: x[1])
# print(top_10_user)

distinct_business = lines_dict.map(lambda x: (x['business_id'], 1)).reduceByKey(lambda x,y:x+y)
# print(distinct_business.count())

# top_10_business = distinct_business.sortBy(lambda x: len(x[1]), False).map(lambda x:(x[0], len(x[1]))).take(10)
top_10_business = distinct_business.top(10, lambda x: x[1])
# print(top_10_business)


result = [{"n_review":lines.count(), "n_review_2018":lines_2018.count(), "n_user":distinct_user.count(), "top10_user":top_10_user, "n_business":distinct_business.count(), "top10_business":top_10_business}]

with open(sys.argv[2], 'w') as f:
    f.write(json.dumps(result))

end=time.time()
print("running time: ", end-start)