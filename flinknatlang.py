import sys

from flink.plan.Environment import get_environment
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction

import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import PunktSentenceTokenizer
from nltk.corpus import stopwords
import re

ps = PorterStemmer()
stopWords = set(stopwords.words('english'))


class Tokenizer(FlatMapFunction):
    def flat_map(self, value, collector):
        for word in value.lower().split():
            word1 = re.sub(r'[\W_]+','',word)
            if len(word1)>0:
                if word1 not in stopWords:
                    try:
                        word2 = word1.split()
                        word2 = nltk.pos_tag(word2)
                        word2 = word2[0][1]
                    except:
                        continue
                    word1 = ps.stem(word1)
                    word1 = word1 + ', Original type: '+ word2
                    collector.collect((1, word1))
    

class Adder(GroupReduceFunction):
    def reduce(self, iterator, collector):
        count, word1 = iterator.next()
        count += sum([x[0] for x in iterator])
        collector.collect((count, word1))


if __name__ == "__main__":
    env = get_environment()
    if len(sys.argv) != 1 and len(sys.argv) != 3:
        sys.exit("Usage: ./bin/pyflink.sh WordCount[ - <text path> <result path>]")

    if len(sys.argv) == 3:
        data = env.read_text(sys.argv[1])
    else:
        data = env.from_elements("hello","world","hello","car","tree","data","hello")

    result = data \
        .flat_map(Tokenizer()) \
        .group_by(1) \
        .reduce_group(Adder(), combinable=True) \

    result.output()
    
###Set paralelism to number of cores minus number of nodes
    env.set_parallelism(XX)

    env.execute()
