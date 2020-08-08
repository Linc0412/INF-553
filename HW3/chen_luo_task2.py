import sys
case = int(sys.argv[3])
if case == 1:
    from pyspark.mllib.recommendation import ALS, Rating
    from pyspark import SparkContext
    import time
    start = time.time()

    # Load and parse the training data
    sc = SparkContext()
    rdd = sc.textFile(sys.argv[1])
    header = rdd.first()
    data = rdd.filter(lambda row:row != header).map(lambda l: l.split(','))

    #Test data
    testRdd = sc.textFile(sys.argv[2])
    testHeader = testRdd.first()
    testdata = testRdd.filter(lambda row: row != testHeader).map(lambda x: x.split(","))
    test_bid = testdata.map(lambda x:x[1]).distinct().collect()
    testBid = list(test_bid)
    testBid.sort()
    test_uid = testdata.map(lambda x:x[0]).distinct().collect()
    testUid = list(test_uid)
    testUid.sort()
    print("test count: ")
    print(testdata.count())

    business_ids = data.map(lambda x:x[1]).distinct().collect()
    # print(business_ids.take(5))
    business = list(business_ids)
    business.sort()
    business = business+testBid
    #unique user_ids 11270
    user_ids = data.map(lambda x:x[0]).distinct().collect()
    user = list(user_ids)
    user.sort()
    user = user+testUid
    # print(user_ids.take(5))
    # print(user_ids.count())
    dicB = {}
    for i, e in enumerate(business):
        dicB[e] = i

    dicU = {}
    for i, e in enumerate(user):
        dicU[e] = i

    vu = sc.broadcast(dicU)
    vb = sc.broadcast(dicB)

    traindata = data.map(lambda x:(vu.value[x[0]], vb.value[x[1]], x[2]))
    # print(traindata.take(5))
    # print(traindata.count())



    ratings = traindata.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))
    # print(ratings.take(2))

    def f(x):
        if x[2] > 5:
            rate = 5
        elif x[2] < 1:
            rate = 1
        else:
            rate = x[2]
        return ((x[0], x[1]), rate)

    # Build the recommendation model using Alternating Least Squares
    rank = 2
    numIterations = 10
    model = ALS.train(ratings, rank, numIterations)

    #Evaluate the model on training data
    test_pred = testdata.map(lambda x:(vu.value[x[0]], vb.value[x[1]]))
    testData = test_pred.map(lambda x: (x, None))
    preds = model.predictAll(test_pred).map(f)
    noPred = testData.subtractByKey(preds).map(lambda x: (x[0], 3))
    predictions = sc.union([preds, noPred])
    # print(predictions.count())
    ratesAndPreds = testdata.map(lambda x:((vu.value[x[0]], vb.value[x[1]]), x[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (float(r[1][0]) - float(r[1][1]))**2).mean()
    RMSE = pow(MSE, 0.5)
    print("Root Mean Squared Error = " + str(RMSE))

    predictions = predictions.map(lambda x:(user[int(x[0][0])], business[int(x[0][1])], str(x[1])))
    re = predictions.collect()

    f = open(sys.argv[4], "w")
    f.write("user_id" + ", " + "business_id" + ", " + "prediction" + "\n")
    for i in re:
        f.write(str(i[0]) + ", " + str(i[1]) + ", " + str(i[2]) + "\n")
    f.close()

    end = time.time()
    print("Running time:", end-start)
    # # Save and load model
    # model.save(sc, "myCollaborativeFilter")
    # sameModel = MatrixFactorizationModel.load(sc, "myCollaborativeFilter")
if case == 2:
    from itertools import combinations
    from pyspark import SparkContext
    import time

    start = time.time()

    # Load and parse the training data
    sc = SparkContext()
    rdd = sc.textFile(sys.argv[1])
    header = rdd.first()
    data = rdd.filter(lambda row: row != header).map(lambda l: l.split(','))

    # Test data
    testRdd = sc.textFile(sys.argv[2])
    testHeader = testRdd.first()
    testdata = testRdd.filter(lambda row: row != testHeader).map(lambda x: x.split(","))
    test_bid = testdata.map(lambda x: x[1]).distinct().collect()
    testBid = list(test_bid)
    testBid.sort()
    test_uid = testdata.map(lambda x: x[0]).distinct().collect()
    testUid = list(test_uid)
    testUid.sort()
    # print("test count: ")
    # print(testdata.count())

    business_ids = data.map(lambda x: x[1]).distinct().collect()
    # print(business_ids.take(5))
    business = list(business_ids)
    business.sort()
    business = business + testBid
    # unique user_ids 11270
    user_ids = data.map(lambda x: x[0]).distinct().collect()
    user = list(user_ids)
    user.sort()
    user = user + testUid
    # print(user_ids.take(5))
    # print(user_ids.count())
    dicB = {}
    for i, e in enumerate(business):
        dicB[e] = i

    dicU = {}
    for i, e in enumerate(user):
        dicU[e] = i

    vu = sc.broadcast(dicU)
    vb = sc.broadcast(dicB)

    traindata = data.map(lambda x: (int(vu.value[x[0]]), int(vb.value[x[1]]), float(x[2])))

    dicTrain = {}
    for i in traindata.collect():
        dicTrain[(i[0], i[1])] = i[2]

    userAvgRating = traindata.map(lambda x: (x[0], x[2])).aggregateByKey((0, 0), lambda U, v: (U[0] + v, U[1] + 1),
                                                                         lambda U1, U2: (
                                                                         U1[0] + U2[0], U1[1] + U2[1])).map(
        lambda x: (x[0], float(x[1][0]) / x[1][1]))
    # print(userAvgRating.take(5))

    dic_User_Avg = {}
    for i in userAvgRating.collect():
        dic_User_Avg[i[0]] = i[1]

    # print(dicTrain)
    # print(dicAvg)
    # (business_id, [(user_id, rating)])
    businessToUser = traindata.map(lambda x: (x[1], [(x[0], x[2])])).partitionBy(16).reduceByKey(lambda x, y: x + y)


    # print(businessToUser.take(5))

    #
    def userRatingComb(x):
        for y in combinations(x[1], 2):
            a, b = min(y[0], y[1]), max(y[0], y[1])
            yield ((a[0], b[0]), [(a[1], b[1])])


    # ((user1_id, user2_id), [(rating1, rating2)])    user1_id < user2_id
    userToRating = businessToUser.flatMap(userRatingComb).partitionBy(16).reduceByKey(lambda x, y: x + y)


    # print(userToRating.take(5))

    def pearson_Similarity(x):
        ratings = x
        rating1 = [x[0] for x in ratings]
        rating2 = [x[1] for x in ratings]
        avg1 = sum(rating1) / len(rating1)
        avg2 = sum(rating2) / len(rating2)
        diff1 = [x - avg1 for x in rating1]
        diff2 = [x - avg2 for x in rating2]
        root1 = pow(sum([x ** 2 for x in diff1]), 0.5)
        root2 = pow(sum([x ** 2 for x in diff2]), 0.5)
        up = sum([diff1[i] * diff2[i] for i in range(len(rating1))])
        down = root1 * root2
        if up == 0:
            return 0
        return up / down


    # ((user1_id, user2_id), pearson_similarity) user1_id < user2_id
    user_similarity = userToRating.map(lambda x: (x[0], pearson_Similarity(x[1])))
    # print(user_similarity.take(5))

    # (activeUser_id, [(user1_id, similarity)])
    userSimiMatrix = user_similarity.flatMap(lambda x: [(x[0][0], [(x[0][1], x[1])]), (x[0][1], [(x[0][0], x[1])])]) \
        .reduceByKey(lambda x, y: x + y)
    # print(userSimiMatrix.take(5))

    dicSimi = {}

    for e in userSimiMatrix.collect():
        com = e[1]
        com.sort(key=lambda y: y[1], reverse=True)
        length = len(com)
        if length > 5:
            dicSimi[e[0]] = com[:5]
        else:
            dicSimi[e[0]] = com


    def predict(x):
        # if x[0] not in dicSimi:
        #     if x[0] not in dic_User_Avg:
        #         return (x, 3)
        #     return (x, dic_User_Avg[x[0]])
        allCom = dicSimi[x[0]]
        # allCom.sort(key=lambda y: y[1], reverse=True)
        # length = len(allCom)
        companion = allCom
        # if length > 5:
        #     companion = allCom[:5]
        # else:
        #     companion = allCom
        avg = dic_User_Avg[x[0]]
        sumi = 0
        down = 0
        if len(companion) == 0:
            print("no companion")
        for e in companion:
            tmp = (e[0], x[1])
            if tmp in dicTrain:
                rate = dicTrain[tmp] - dic_User_Avg[e[0]]
                sumi += rate * e[1]
                down += abs(e[1])
        if down == 0:
            return (x, avg)
        return (x, sumi / down + avg)


    def f(x):
        if x[1] > 5:
            rate = 5
        elif x[1] < 0:
            rate = 0
        else:
            rate = x[1]
        return (x[0], rate)


    result = testdata.map(lambda x: (int(vu.value[x[0]]), int(vb.value[x[1]]))).map(predict).map(f)

    ratesAndPreds = testdata.map(lambda x: ((vu.value[x[0]], vb.value[x[1]]), x[2])).join(result)
    MSE = ratesAndPreds.map(lambda r: (float(r[1][0]) - float(r[1][1])) ** 2).mean()
    RMSE = pow(MSE, 0.5)
    print("Root Mean Squared Error = " + str(RMSE))

    # predictions = result.map(lambda x:(user[int(x[0][0])], business[int(x[0][1])], str(x[1])))
    re = result.collect()

    f = open(sys.argv[4], "w")
    f.write("user_id" + ", " + "business_id" + ", " + "prediction" + "\n")
    for i in re:
        f.write(user[i[0][0]] + ", " + business[i[0][1]] + ", " + str(i[1]) + "\n")
    f.close()

    end = time.time()
    print("Running time:", end - start)

if case == 3:
    from itertools import combinations
    from pyspark import SparkContext
    import time

    start = time.time()

    # Load and parse the training data
    sc = SparkContext()
    rdd = sc.textFile(sys.argv[1])
    header = rdd.first()
    data = rdd.filter(lambda row: row != header).map(lambda l: l.split(','))

    # Test data
    testRdd = sc.textFile(sys.argv[2])
    testHeader = testRdd.first()
    testdata = testRdd.filter(lambda row: row != testHeader).map(lambda x: x.split(","))
    test_bid = testdata.map(lambda x: x[1]).distinct().collect()
    testBid = list(test_bid)
    testBid.sort()
    test_uid = testdata.map(lambda x: x[0]).distinct().collect()
    testUid = list(test_uid)
    testUid.sort()
    # print("test count: ")
    # print(testdata.count())

    business_ids = data.map(lambda x: x[1]).distinct().collect()
    # print(business_ids.take(5))
    business = list(business_ids)
    business.sort()
    business = business + testBid
    # unique user_ids 11270
    user_ids = data.map(lambda x: x[0]).distinct().collect()
    user = list(user_ids)
    user.sort()
    user = user + testUid
    # print(user_ids.take(5))
    # print(user_ids.count())
    dicB = {}
    for i, e in enumerate(business):
        dicB[e] = i

    dicU = {}
    for i, e in enumerate(user):
        dicU[e] = i

    vu = sc.broadcast(dicU)
    vb = sc.broadcast(dicB)

    traindata = data.map(lambda x: (int(vu.value[x[0]]), int(vb.value[x[1]]), float(x[2])))

    dicTrain = {}
    for e in traindata.collect():
        dicTrain[(e[0], e[1])] = e[2]

    businessAvg = traindata.map(lambda x: (x[1], [x[2]])).reduceByKey(lambda x, y: x + y).map(
        lambda x: (x[0], sum(x[1]) / len(x[1])))
    # print(userAvg.take(5))

    dicAvg = {}
    for e in businessAvg.collect():
        dicAvg[e[0]] = e[1]

    # (user_id, [(business_id, rating)])
    userToBusiness = traindata.map(lambda x: (x[0], [(x[1], x[2])])).reduceByKey(lambda x, y: x + y)


    # print(businessToUser.take(5))

    #
    def businessRatingComb(x):
        for y in combinations(x[1], 2):
            a, b = min(y[0], y[1]), max(y[0], y[1])
            yield ((a[0], b[0]), [(a[1], b[1])])


    # ((business1_id, business2_id), [(rating1, rating2)])    user1_id < user2_id
    businessToRating = userToBusiness.flatMap(businessRatingComb).partitionBy(16).reduceByKey(lambda x, y: x + y)


    # print(userToRating.take(5))

    def calSimi(x):
        ratings = x
        ratingA = [x[0] for x in ratings]
        ratingB = [x[1] for x in ratings]
        avg1 = sum(ratingA) / len(ratingA)
        avg2 = sum(ratingB) / len(ratingB)
        diff1 = [x - avg1 for x in ratingA]
        diff2 = [x - avg2 for x in ratingB]
        root1 = pow(sum([x ** 2 for x in diff1]), 0.5)
        root2 = pow(sum([x ** 2 for x in diff2]), 0.5)
        up = sum([diff1[i] * diff2[i] for i in range(len(ratingA))])
        if up == 0:
            return 0
        return up / (root1 * root2)


    # ((business1_id, business2_id), pearson_similarity) business1_id < business2_id
    businessSimi = businessToRating.map(lambda x: (x[0], calSimi(x[1])))
    # print(userSimi.take(5))

    # (active_business_id, [(business1_id, similarity)])
    businessSimiMatrix = businessSimi.flatMap(lambda x: [(x[0][0], [(x[0][1], x[1])]), (x[0][1], [(x[0][0], x[1])])]) \
        .reduceByKey(lambda x, y: x + y)
    # print(userSimiMatrix.take(5))

    dicSimi = {}

    for e in businessSimiMatrix.collect():
        com = e[1]
        com.sort(key=lambda y: y[1], reverse=True)
        length = len(com)
        if length > 2:
            dicSimi[e[0]] = com[:2]
        else:
            dicSimi[e[0]] = com


    def predict(x):
        if x[1] not in dicSimi:
            if x[1] not in dicAvg:
                return (x, 3)
            return (x, dicAvg[x[1]])
        companion = dicSimi[x[1]]
        # allCom.sort(key=lambda y: y[1], reverse=True)
        # length = len(allCom)
        # companion = []
        # if length > 2:
        #     companion = allCom[:2]
        # else:
        #     companion = allCom
        avg = dicAvg[x[1]]
        sumi = 0
        down = 0
        if len(companion) == 0:
            print("no companion")
        for e in companion:
            tmp = (x[0], e[0])
            if tmp in dicTrain:
                rate = dicTrain[tmp]
                sumi += rate * e[1]
                down += abs(e[1])
        if down == 0:
            return (x, avg)
        return (x, sumi / down)


    def f(x):
        if x[1] > 5:
            rate = 5
        elif x[1] < 0:
            rate = 0
        else:
            rate = x[1]
        return (x[0], rate)


    result = testdata.map(lambda x: (int(vu.value[x[0]]), int(vb.value[x[1]]))).map(predict).map(f)

    ratesAndPreds = testdata.map(lambda x: ((vu.value[x[0]], vb.value[x[1]]), x[2])).join(result)
    MSE = ratesAndPreds.map(lambda r: (float(r[1][0]) - float(r[1][1])) ** 2).mean()
    RMSE = pow(MSE, 0.5)
    print("Root Mean Squared Error = " + str(RMSE))

    # predictions = result.map(lambda x:(user[int(x[0][0])], business[int(x[0][1])], str(x[1])))
    # re = predictions.collect()
    re = result.collect()

    f = open(sys.argv[4], "w")
    f.write("user_id" + ", " + "business_id" + ", " + "prediction" + "\n")
    for i in re:
        f.write(user[i[0][0]] + ", " + business[i[0][1]] + ", " + str(i[1]) + "\n")
    f.close()

    end = time.time()
    print("Running time:", end - start)

if case == 4:
    from pyspark import SparkContext
    import time
    import sys

    start = time.time()

    sc = SparkContext()
    rdd = sc.textFile(sys.argv[1])
    header = rdd.first()
    data = rdd.filter(lambda row: row != header).map(lambda x: x.split(","))

    # unique business_ids 24732
    business_ids = data.map(lambda x: x[1]).distinct().collect()
    # print(business_ids.take(5))
    business = list(business_ids)
    business.sort()

    # unique user_ids 11270
    user_ids = data.map(lambda x: x[0]).distinct().collect()
    user = list(user_ids)
    user.sort()

    # Test data
    testRdd = sc.textFile(sys.argv[2])
    testHeader = testRdd.first()
    testdata = testRdd.filter(lambda row: row != testHeader).map(lambda x: x.split(","))
    test_bid = testdata.map(lambda x: x[1]).distinct().collect()
    testBid = list(test_bid)
    testBid.sort()
    test_uid = testdata.map(lambda x: x[0]).distinct().collect()
    testUid = list(test_uid)
    testUid.sort()

    user = user + testUid
    business = business
    # print(user_ids.take(5))
    # print(user_ids.count())
    dicB = {}
    for i, e in enumerate(business):
        dicB[e] = i

    dicU = {}
    for i, e in enumerate(user):
        dicU[e] = i
    # print(len(dicB))
    # print(len(dicU))

    # def matrix(data):
    #     global business_ids
    #     data = list(data)
    #     uid = data[0]
    #     bid = list(data[1])
    #
    #     for i in business_ids:
    #         if i in bid:
    #             return (dicU[uid], dicM[i])
    #
    #
    # usr = data.groupByKey().map(lambda x:(x[0], list(x[1])))
    # # print(user_ids.take(5))
    # # print(user_ids.count())
    #
    # user_business = usr.map(matrix)
    # user = user_business.map(lambda x:x[0]).collect()
    # print(user_business.collect())
    # print(user)

    vu = sc.broadcast(dicU)
    vb = sc.broadcast(dicB)

    mat = data.map(lambda x: (x[1], [vu.value[x[0]]])).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0])
    matrix = mat.collect()

    print(matrix)

    traindata = data.map(lambda x: (int(vu.value[x[0]]), int(vb.value[x[1]]), float(x[2])))
    dicTrain = {}
    for e in traindata.collect():
        dicTrain[(e[0], e[1])] = e[2]

    businessAvg = traindata.map(lambda x: (x[1], [x[2]])).reduceByKey(lambda x, y: x + y).map(
        lambda x: (x[0], sum(x[1]) / len(x[1])))
    # print(userAvg.take(5))

    dicAvg = {}
    for e in businessAvg.collect():
        dicAvg[e[0]] = e[1]

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
              [199, 6607, 491], [241, 619, 1033], [983, 991, 587], [233, 41, 991], \
              [1303, 59, 5843], [419, 53, 9001]]

    signatures = mat.map(lambda x: (x[0], [minhash(x, has) for has in hashes]))
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


    def jaccard(x):
        a = set(matrix[dicB[x[0]]][1])
        b = set(matrix[dicB[x[1]]][1])
        inter = a & b
        union = a | b
        jacc = len(inter) / len(union)
        return (x[0], x[1], jacc)


    result = cand.map(jaccard).filter(lambda x: x[2] >= 0.5)
    # print(cand.count())
    # print(cand.take(5))

    # (business_id, (user_id, rating))
    userToBusiness = data.map(lambda x: (x[1], [(x[0], x[2])])).reduceByKey(lambda x, y: x + y)
    userToBusiness = userToBusiness.collect()
    userToBusiness = {x[0]: x[1] for x in userToBusiness}
    print(userToBusiness)


    def calSimi(x):
        business_id1 = x[0]
        business_id2 = x[1]
        ratingA = userToBusiness[business_id1]
        ratingB = userToBusiness[business_id2]
        ratingAkey = [x[0] for x in ratingA]
        ratingBkey = [x[0] for x in ratingB]
        ratingA = [float(x[1]) for x in ratingA if x[0] in ratingBkey]
        ratingB = [float(x[1]) for x in ratingB if x[0] in ratingAkey]

        print(len(ratingA))
        # if len(ratingA) == 0:
        #     return 0
        # ratingA = [x[0] for x in ratings]
        # ratingB = [x[1] for x in ratings]
        avg1 = sum(ratingA) / len(ratingA)
        avg2 = sum(ratingB) / len(ratingB)

        diff1 = [x - avg1 for x in ratingA]
        diff2 = [x - avg2 for x in ratingB]
        root1 = pow(sum([x ** 2 for x in diff1]), 0.5)
        root2 = pow(sum([x ** 2 for x in diff2]), 0.5)
        up = sum([diff1[i] * diff2[i] for i in range(len(ratingA))])
        if up == 0:
            return 0
        return up / (root1 * root2)


    # ((business1_id, business2_id), pearson_similarity) business1_id < business2_id
    businessSimi = result.map(lambda x: ((x[0], x[1]), calSimi(x)))
    # print(userSimi.take(5))

    # (active_business_id, [(business1_id, similarity)])
    businessSimiMatrix = businessSimi.flatMap(lambda x: [(x[0][0], [(x[0][1], x[1])]), (x[0][1], [(x[0][0], x[1])])]) \
        .reduceByKey(lambda x, y: x + y)
    # print(userSimiMatrix.take(5))

    dicSimi = {}

    for e in businessSimiMatrix.collect():
        com = e[1]
        com.sort(key=lambda y: y[1], reverse=True)
        length = len(com)
        if length > 2:
            dicSimi[e[0]] = com[:2]
        else:
            dicSimi[e[0]] = com


    def predict(x):
        if x[1] not in dicSimi:
            if x[1] not in dicAvg:
                return (x, 3)
            return (x, dicAvg[x[1]])
        companion = dicSimi[x[1]]
        # allCom.sort(key=lambda y: y[1], reverse=True)
        # length = len(allCom)
        # companion = []
        # if length > 2:
        #     companion = allCom[:2]
        # else:
        #     companion = allCom
        avg = dicAvg[x[1]]
        sumi = 0
        down = 0
        if len(companion) == 0:
            print("no companion")
        for e in companion:
            tmp = (x[0], e[0])
            if tmp in dicTrain:
                rate = dicTrain[tmp]
                sumi += rate * e[1]
                down += abs(e[1])
        if down == 0:
            return (x, avg)
        return (x, sumi / down)


    def f(x):
        if x[1] > 5:
            rate = 5
        elif x[1] < 0:
            rate = 0
        else:
            rate = x[1]
        return (x[0], rate)


    business = business + testBid
    # print(user_ids.take(5))
    # print(user_ids.count())
    dicB = {}
    for i, e in enumerate(business):
        dicB[e] = i
    vb = sc.broadcast(dicB)

    result = testdata.map(lambda x: (int(vu.value[x[0]]), int(vb.value[x[1]]))).map(predict).map(f)

    ratesAndPreds = testdata.map(lambda x: ((vu.value[x[0]], vb.value[x[1]]), x[2])).join(result)
    MSE = ratesAndPreds.map(lambda r: (float(r[1][0]) - float(r[1][1])) ** 2).mean()
    RMSE = pow(MSE, 0.5)
    print("Root Mean Squared Error = " + str(RMSE))

    # predictions = result.map(lambda x:(user[int(x[0][0])], business[int(x[0][1])], str(x[1])))
    # re = predictions.collect()
    re = result.collect()

    f = open(sys.argv[4], "w")
    f.write("user_id" + ", " + "business_id" + ", " + "prediction" + "\n")
    for i in re:
        f.write(user[i[0][0]] + ", " + business[i[0][1]] + ", " + str(i[1]) + "\n")
    f.close()

    end = time.time()
    print("Running time:", end - start)
