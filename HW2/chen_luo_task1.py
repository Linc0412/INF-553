from pyspark import SparkContext
from itertools import combinations,chain
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
    candidates = []
    single_freq = []
    for each_basket in all_baskets:
        for i in each_basket:
            counts[i] += 1
    for k, v in counts.items():
        if v >= sub_support:
            candidates.append(k)
            single_freq.append(frozenset([k]))
    single_freq = list(set(single_freq))
#Single part is over

    items_num = 2

    L = []
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
            for each_basket in all_baskets:
                if set(pair).issubset(set(each_basket)):
                    pair_count[frozenset(pair)] += 1
                    # temp.add(pair)
        # if len(pair_count) == 0: break
        Q = []
        # for i in temp:
        #     for j in i:
        #         Q.add(j)
        # L = Q
        ll = 0
        for k, v in pair_count.items():
            if v >= sub_support:
                candidates.append(k)
                ll= ll+1
                Q.append(k)
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

    # all_baskets = list(all_baskets)
    # counts = {}
    # for candidate in total_candidates:
    #     for each_basket in all_baskets:
    #         if isinstance(candidate, str):
    #             if candidate in each_basket:
    #                 if candidate not in counts:
    #                     counts[candidate] = 1
    #                 else:
    #                     counts[candidate] += 1
    #         else:
    #             if set(candidate).issubset(set(each_basket)):
    #                 if candidate not in counts:
    #                     counts[candidate] = 1
    #                 else:
    #                     counts[candidate] += 1
    # return counts.items()


support = int(sys.argv[2])
num_par = 2
sub_support = float(support / num_par)

sc = SparkContext()

lines = sc.textFile(sys.argv[3], num_par)
header = lines.first()
case = int(sys.argv[1])
if case == 1:
    rdd1 = lines.filter(lambda row: row != header).map(lambda x: x.split(',')).groupByKey().map(lambda x:list(x[1])).mapPartitions(pass1).distinct()
    total_candidates = rdd1.collect()

    # print(total_candidates)
    rdd2 = lines.filter(lambda row: row != header).map(lambda x: x.split(',')).groupByKey().map(lambda x:list(x[1])).mapPartitions(pass2).reduceByKey(lambda x,y: x + y).filter(lambda x: x[1] >= support).map(lambda x: x[0])
    total_frequents = rdd2.collect()
if case == 2:
    rdd1 = lines.filter(lambda row: row != header).map(lambda x: x.split(',')).map(
        lambda x: (x[1], x[0])).groupByKey().map(lambda x: list(x[1])).mapPartitions(pass1).distinct()
    total_candidates = rdd1.collect()
    # print(total_candidates)
    rdd2 = lines.filter(lambda row: row != header).map(lambda x: x.split(',')).map(
        lambda x: (x[1], x[0])).groupByKey().map(lambda x: list(x[1])).mapPartitions(pass2).reduceByKey(
        lambda x, y: x + y).filter(lambda x: x[1] >= support).map(lambda x: x[0])
    total_frequents = rdd2.collect()
# print(rdd2.collect())

# first_line_cand = rdd1.filter(lambda x: type(x) == str).sortBy(lambda x:x).collect()
# length = 2
#
f1 = open(sys.argv[4], 'w')
#
# #Write Candidates
# def cand():
#     length = 2
#     f1.write("Candidates:" + "\n")
#     for i in first_line_cand:
#         if first_line_cand.index(i) != len(first_line_cand) - 1:
#             f1.write("('" + str(i) + "')" + ",")
#         else:
#             f1.write("('" + str(i) + "')" + "\n")
#
#     while(True):
#         lines = []
#         lines = sorted(rdd1.filter(lambda x: type(x) == tuple and len(x) == length).collect())
#         for i in lines:
#             if lines.index(i) != len(lines) - 1:
#                 f1.write(str(i) + ",")
#             else:
#                 f1.write(str(i) + "\n")
#         length += 1
#         if len(rdd1.filter(lambda x: type(x) == tuple and len(x) == length).collect()) == 0:break
#
#     return True
#
# #Write frequent itemsets
# first_line_freq = rdd1.filter(lambda x: type(x) == str).sortBy(lambda x:x).collect()
# def freq():
#     length = 2
#     f1.write("Frequent Itemsets:" + "\n")
#     for i in first_line_freq:
#         if first_line_freq.index(i) != len(first_line_freq) - 1:
#             f1.write("('" + str(i) + "')" + ",")
#         else:
#             f1.write("('" + str(i) + "')" + "\n")
#
#     while (True):
#         lines = []
#         lines = sorted(rdd2.filter(lambda x: type(x) == tuple and len(x) == length).collect())
#         for i in lines:
#             if lines.index(i) != len(lines) - 1:
#                 f1.write(str(i) + ",")
#             else:
#                 f1.write(str(i) + "\n")
#         length += 1
#         if len(rdd2.filter(lambda x: type(x) == tuple and len(x) == length).collect()) == 0: break
#
#     return True
#
# cand()
# freq()
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
print("Duration:", end-start)