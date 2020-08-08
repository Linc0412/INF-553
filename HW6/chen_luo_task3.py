import tweepy
import random
from collections import defaultdict
import collections

class MyStreamListener(tweepy.StreamListener):

    def __init__(self):
        super(MyStreamListener, self).__init__()
        self.count = 0
        self.list = []
        # self.tag = {}
        self.tag = collections.Counter()

    def on_status(self, status):
        # print(status.text)
        # print("!!!!!!!!")
        self.count += 1
        if self.count > 100:
            rmd = random.randint(1, self.count)
            if rmd < 100:
                status_out = self.list.pop(rmd)

                self.list.append(status.text)
                words = status.text.split()
                tags = [x[1:] for x in words if len(x) > 1 and x[0] == "#"]
                # for x in words:
                #     if len(x)>1 and x[0] == "#":
                #         tags = x[1:]
                word_out = status_out.split()
                tag_out = [x[1:] for x in word_out if len(x) > 1 and x[0] == "#"]
                self.tag.update(tags)
                self.tag.subtract(tag_out)


                print("The number of tweets with tags from the beginning: "+ str(self.count))
                rank = sorted(self.tag.items(), key=lambda x: (-x[1], x[0]))
                result_list = defaultdict(list)
                for i in rank:
                    result_list[i[1]].append(i[0])

                top3 = 0
                for i in result_list:
                    if top3 == 3:
                        break
                    top3 = top3+1
                    for j in result_list[i]:
                        print(str(j) + ":" + str(i))
                # top3 = self.tag.most_common(3)
                # for e in top3:
                #     print(e[0]+": "+str(e[1]))
                print("\n")

        else:
            self.list.append(status.text)
            words = status.text.split()
            tags = [x[1:] for x in words if (len(x) > 1 and x[0] == "#")]
            self.tag.update(tags)





customerToken = "a7ZtdRkuWaq1HCt4PRshM1yF4"
customerSecret = "48CoKNjINFdZgNjR28Bv7H38BEcun8XvqxcRyMZXJZqSowkzSi"
accessToken = "1069385588925558784-LVESXTWYjILxHqbew23VzDcy0ttaEU"
accessSecret = "4G59SCxu5QA4i8Oy1WW9KKcfQCQZwaA3vwcCyqS6dClkF"

auth = tweepy.OAuthHandler(customerToken, customerSecret)
auth.set_access_token(accessToken, accessSecret)
api = tweepy.API(auth)
myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
tweets = myStream.filter(track=['#'])