#!/usr/bin/env python3
"""
This MRJob counts every adjacent word pair (bigram) in the input text:
1. Split each line on whitespace (keeps punctuation attached to words).
2. Pad the word with '' at the start and end to capture edge bigrams.
3. Mapper emits ((word_i, word_{i+1}), 1) for each adjacent pair.
4. Reducer sums these 1's to compute total occurrences per bigram.

Run:
python bigrams.py -r hadoop hdfs:///user/admin/data/book.txt --output-dir=hdfs:///user/admin/data/book-output-bigrams --conf-path=wordcount-mrjob.conf
"""

from mrjob.job import MRJob

class MRBigrams(MRJob):
    def mapper(self, _, line):
        # Split the line into words on whitespace
        word = line.split()
        # Pad with empty strings so we capture first and last bigrams
        word = [''] + word + ['']
        # Emit a count of 1 for each adjacent word pair
        for i in range(len(word) - 1):
            # Key = (current_word, next_word)
            yield (word[i], word[i + 1]), 1

    def reducer(self, bigram, counts):
        # Sum all the 1's for this bigram to get its total count
        total = sum(counts)
        yield bigram, total

if __name__ == '__main__':
    MRBigrams.run()
