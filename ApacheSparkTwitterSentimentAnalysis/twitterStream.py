from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib 
from pylab import plot, title, xlabel, ylabel, savefig, legend, array

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
  
    counts = stream(ssc, pwords, nwords, 100)
    
    make_plot(counts)

def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    positive =[]
    negative =[]
    for count in counts:
        for item in count:
            if item[0] =="positive":
                positive.insert(len(positive),item[1])
            if item[0] =="negative":
                negative.insert(len(negative),item[1])
    allcounts = [tuple(positive),tuple(negative)]
    #print allcounts
    
    matplotlib.pyplot.figure()
    for temp in allcounts:
        plot(range(len(temp)), temp)
        title('positive,negative words count')
        xlabel('time step')
        ylabel('counts')
        legend(['positive', 'negative'], loc='upper right')
        savefig("word-counts.pdf")


def load_wordlist(filename):
    wholepath= "/Users/shangxue/Desktop/csc591/02.Apache.Spark.Streaming.Project-2.SentimentAnalysis-3.FINAL/" + filename
    print wholepath
    f = open(wholepath, 'r')
    list = []
    for line in f:
        list.insert(len(list),line.rstrip('\n'))
    return list


def determine(x,plist,nlist):

    if x in plist:
        return ("positive",1)
    if x in nlist:
        return ("negative",1)
    return ("neither",1)
def updateFunction(newValues, runningCount):
        if runningCount is None:
            runningCount = 0
        return sum(newValues, runningCount)  # add the new values with the previous running count to get the new count
    
def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list":'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    pnTweets = tweets.flatMap(lambda line: line.split(" "))
    pnTweetsPairs = pnTweets.map(lambda x: determine(x,pwords,nwords))
    wordCounts = pnTweetsPairs.reduceByKey(lambda x, y: x + y)
    
    totalCounts = pnTweetsPairs.updateStateByKey(updateFunction)
    totalCounts.pprint()
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    wordCounts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)
    # becaue counts include those neither ones
    
    newCounts = []
    for count in counts:
        newCount = [item for item in count if item[0] == "positive" or item[0] =="negative"]
        newCounts.insert(len(newCounts),newCount)
    
    return newCounts


if __name__=="__main__":
    main()


