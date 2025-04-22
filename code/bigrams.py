#!/usr/bin/env python3
"""
Steps:
1. Mapper:
   - Split each line on whitespace (punctuation retained).
   - Pad the list with "" at start and end.
   - Emit ((word_i, word_{i+1}), 1) for every adjacent pair.
2. Reducer:
   - Sum counts for each bigram.
   - Output (bigram, total_count).
3. Run:
   python bigrams.py -r hadoop hdfs:///user/admin/data/book.txt --output-dir=hdfs:///user/admin/data/book-output-bigrams --conf-path=wordcount-mrjob.conf
"""

from mrjob.job import MRJob


class MRBigrams(MRJob):
    def mapper(self, _, line):
        wordlist = line.split()
        word = [''] + wordlist + ['']
        for i in range(len(word) - 1):
            yield (word[i], word[i + 1]), 1

    def reducer(self, bigram, counts):
        yield bigram, sum(counts)


if __name__ == '__main__':
    MRBigrams.run()
