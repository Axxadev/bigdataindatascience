from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession
import nltk
from nltk.stem import PorterStemmer

from nltk.tokenize import PunktSentenceTokenizer
from nltk.corpus import stopwords
import re

ps = PorterStemmer()
stopWords = set(stopwords.words('english'))


def newflatmapfunc(x):
    ret = []
    for word in x.lower().split():
        word1 = re.sub(r'[\W_]+','',word)
        if len(word1)>0:
            if word1 not in stopWords:
                try:
                    word2=word1.split()
                    word2 = nltk.pos_tag(word2)
                    word2 = word2[0][1]
                except:
                    continue
                word1 = ps.stem(word1)
                word1 = word1 + ', Original type: '+ word2
                ret.append(word1)
    return ret


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: newflatmapfunc(x)) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()

    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))


spark.stop()
