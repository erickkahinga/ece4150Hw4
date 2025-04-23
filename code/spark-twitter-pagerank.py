#!/usr/bin/env python3
"""
This Spark-based MRJob runs PageRank over TwitterGraph.txt:
1. Load each “a follows b” edge into an RDD.
2. Build an adjacency list mapping each user to whom they follow.
3. Initialize every user's rank to 1.0.
4. Iterate 10 times:
    a. Join current ranks with adjacency (dangling users get None).
    b. Split each user's rank evenly among their followees.
    c. Sum and redistribute any “dangling” rank evenly across all users.
    d. Apply the damping factor to compute updated ranks.
5. Format as “user_id\trank” and write to HDFS.
run:
python spark-twitter-pagerank.py -r hadoop hdfs:///user/admin/data/TwitterGraph.txt --output-dir=hdfs:///user/admin/data/TwitterGraph-PageRank-output --conf-path=wordcount-mrjob.conf
"""

from operator import add
from mrjob.job import MRJob

class PageRankJob(MRJob):
    def spark(self, input_path, output_path):
        from pyspark import SparkContext # Import pyspark so script still runs where Spark isn't available

        # Teleportation constant from the PageRank formula
        DAMP_FACTOR = 0.85

        # Initialize SparkContext with a descriptive name
        sc = SparkContext(appName='MRJob_Spark_PageRank')

        # Load the edge list: each line “follower followed”
        lines_rdd = sc.textFile(input_path)

        # Parse and clean edges, (follower, followee)
        edges = (
            lines_rdd
            .map(lambda ln: ln.strip().split())      # split on whitespace
            .filter(lambda parts: len(parts) == 2)   # drop malformed lines
            .map(lambda parts: (parts[0], parts[1])) # tuple (a, b)
        )

        # Build adjacency lists: (user, [list of users they follow])
        adjacency = edges.groupByKey().mapValues(list)

        # Collect every unique user (as follower or followee)
        users = edges.flatMap(lambda pair: pair).distinct()

        # Count users for distributing dangling rank evenly
        total_users = users.count()

        # Initialize each user's PageRank to 1.0
        ranks = users.map(lambda u: (u, 1.0))

        def iterate(ranks_rdd):
            # keep every key from the left RDD and attach adjacency, dangling users get None
            joined = ranks_rdd.leftOuterJoin(adjacency)

            # Distribute each user's rank across its followees
            contributions = joined.flatMap(
                lambda uf: [
                    (dest, uf[1][0] / len(uf[1][1]))
                    for dest in uf[1][1]
                ] if uf[1][1] else []
            )

            # Sum the rank of users with no followees (dangling mass)
            dangling_mass = (
                joined
                .filter(lambda x: not x[1][1]) # keep only dangling users
                .map(lambda x: x[1][0])        # extract their rank
                .sum()                         # total dangling rank
            )

            # Evenly distribute dangling mass to every user
            leak = dangling_mass / total_users

            # Aggregate contributions and apply PageRank formula
            return (
                contributions
                .reduceByKey(add)  # sum contributions per user
                .mapValues(
                    lambda contrib: 
                        (1 - DAMP_FACTOR)       # teleportation term
                        + DAMP_FACTOR * (contrib + leak)  # damped rank
                )
            )

        # Execute 10 iterations of PageRank
        for _ in range(10):
            ranks = iterate(ranks)

        # Prepare output as tab-separated strings
        output = ranks.map(lambda ur: f"{ur[0]}\t{ur[1]}")

        # Write results to the specified HDFS directory
        output.saveAsTextFile(output_path)

        # Cleanly shut down SparkContext
        sc.stop()

if __name__ == '__main__':
    PageRankJob.run()
