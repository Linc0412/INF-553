from pyspark import SparkContext
from itertools import combinations, chain
from collections import defaultdict
import time
import sys

start = time.time()


def pass1(all_baskets):
    # all_baskets = list(all_baskets)
    # counts = {}
    # candidates = []
    # single_freq = []
    # for each_basket in all_baskets:
    #     for i in each_basket:
    #         if i not in counts:
    #             counts[i] = 1
    #         else:
    #             counts[i]+=1
    #
    #     for k, v in counts.items():
    #         if v>= sub_support:
    #             candidates.append(k)
    #             single_freq.append(k)
    # single_freq = list(set(single_freq))

    all_baskets = list(all_baskets)
    counts = defaultdict(int)
    ttemp = list()
    candidates = []
    single_freq = []
    print(sub_support)
    for each_basket in all_baskets:
        ttemp.append(set(each_basket))
        for i in each_basket:
            counts[i] += 1
    for k, v in counts.items():
        if v >= sub_support:
            candidates.append(k)
            single_freq.append(frozenset([k]))
    single_freq = list(set(single_freq))
#Single part is over

    items_num = 2

    L = set()
    L = single_freq
    c = 0
    while (True):
        c = c+1
        # print("pass%s"%c)
        # pair_count = {}
        # item_pairs = combinations(single_freq, items_num)
        #
        # for pair in item_pairs:
        #     for each_basket in all_baskets:
        #         if set(pair).issubset(set(each_basket)):
        #             if pair not in pair_count:
        #                 pair_count[pair] = 1
        #             else:
        #                 pair_count[pair] += 1
        pair_count = defaultdict(int)
        
        # item_pairs = combinations(L, items_num)
        # item_pairs = [set(sorted(x)) for x in chain(*[combinations(L, items_num)])]
        item_pairs = set([i.union(j) for i in L for j in L if len(i.union(j)) == items_num])
        print("item_num:%s"%items_num)
        print(len(item_pairs))

        temp = set()
        for pair in item_pairs:
            for each_basket in ttemp:
                if pair.issubset(each_basket):
                    pair_count[frozenset(pair)] += 1
                    # temp.add(pair)
        # if len(pair_count) == 0: break
        Q = set()
        # for i in temp:
        #     for j in i:
        #         Q.add(j)
        # L = Q
        ll = 0
        print("done count")
        for k, v in pair_count.items():
            if v >= sub_support:
                candidates.append(k)
                ll= ll+1
                Q.add(k)
        # Q = list(set(Q))
        print("new candidate:%s"%ll)
        # print(len(Q))
        L = Q
        if len(L) == 0:break
        items_num+=1
    print("finish pass1")
    return candidates

def pass2(all_baskets):
    all_baskets = list(all_baskets)
    counts = defaultdict(int)

    for candidate_item in total_candidates:
        # print(candidate_item)
        for each_basket in all_baskets:
            if isinstance(candidate_item, str):
                if candidate_item in each_basket:
                    counts[candidate_item] += 1
            else:
                if set(candidate_item).issubset(each_basket):
                    counts[candidate_item] += 1

    return counts.items()

support = int(sys.argv[2])
num_par = 4
sub_support = float(support) / num_par

sc = SparkContext()

lines = sc.textFile(sys.argv[3], num_par)
header = lines.first()
kkkk = int(sys.argv[1])
rdd3 = lines.filter(lambda row: row != header).map(lambda x: x.split(',')).groupByKey().sortByKey(ascending = True).map(lambda x:list(set(x[1]))).filter(lambda x:len(x) >kkkk )


rdd1 = rdd3.mapPartitions(pass1).distinct()
total_candidates = rdd1.collect()

# print(total_candidates)
rdd2 = rdd3.mapPartitions(pass2).reduceByKey(lambda x,y: x + y).filter(lambda x: x[1] >= support).map(lambda x: x[0])

total_frequents = rdd2.collect()

# print(rdd2.collect())

# first_line_cand = rdd1.filter(lambda x: type(x) == str).sortBy(lambda x:x).collect()
# length = 2
#
f1 = open(sys.argv[4], 'w')

def final_output(list_result):
    finalresult = defaultdict(list)
    for item in list_result:
        if type(item) == str:
            k = 1
            finalresult[k].append(item)
        else:
            k = len(item)
            finalresult[k].append(sorted(item))

    # print(finalresult)
    for i in finalresult.keys():
        finallist = sorted(finalresult[i])
        out = ""
        for j in finallist:
            if i == 1:
                out += "('" + str(j) + "'), "
            else:
                out += str(tuple(j)) + ", "
        f1.write(out[:-2] + "\n")


f1.write("Candidates:" + "\n")
final_output(total_candidates)
f1.write("Frequent Itemsets:" + "\n")
final_output(total_frequents)

end = time.time()
print("Duration:", end - start)