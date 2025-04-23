#!/usr/bin/env python3
"""
This Spark-based MRJob reads TwitterGraph.txt as an edge list of “follower followed” pairs,
counts how many followers each user has, and writes out lines formatted as: user_id \t num_followers:
1. Load the input file into an RDD of raw text lines.
2. Split each line into tokens and filter out malformed rows.
3. For each valid pair, emit (followed_user, 1).
4. Sum the 1's per user to get total followers (reduceByKey).
5. Convert each (user, count) to a tab delimited string and save to HDFS.

run:
python spark-twitter-count.py -r hadoop hdfs:///user/admin/data/TwitterGraph.txt --output-dir=hdfs:///user/admin/data/TwitterGraph-output --conf-path=wordcount-mrjob.conf
"""

from operator import add
from mrjob.job import MRJob


class MRSparkTwitterCount(MRJob):
    def spark(self, input_path, output_path):
        from pyspark import SparkContext # Import pyspark so script still runs where Spark isn't available

        # Initializes SparkContext with a descriptive name
        sc = SparkContext(appName='mrjob Spark Twitter follower count')

        # 1. Read the edge list file into an RDD of lines
        lines = sc.textFile(input_path)

        # 2–4. Transform and count
        # There is a lot method chaining happening here so lets break down what's happening
        (lines
           # Split on whitespace into [follower, followed]
           .map(lambda ln: ln.strip().split())
           # drop bad rows
           .filter(lambda tokens: len(tokens) == 2)
           # For each pair, key by the followed user and assign count 1
           .map(lambda tokens: (tokens[1], 1))
           # Sum all the 1's per key to get follower totals
           .reduceByKey(add)
           # 5. Format each result as "user_id\tcount"
           .map(lambda kv: f"{kv[0]}\t{kv[1]}")
           # Write output to the specified HDFS directory
           .saveAsTextFile(output_path)
        )

        # Cleanly shut down Spark
        sc.stop()

if __name__ == '__main__':
    MRSparkTwitterCount.run()
