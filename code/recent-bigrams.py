#!/usr/bin/env python3
"""
This MRJob scans the Google Books 2-gram CSV to emit every bigram whose
first recorded appearance is after 1992:
1. Mapper:
   - Wrap each tab separated line for Python's csv.reader.
   - Parse (bigram, year) pairs, skipping malformed or non-integer years.
   - Emit (bigram, year).
2. Reducer:
   - Find the minimum year for each bigram.
   - Emit only those with first year > 1992.
Run:
python recent-bigrams.py -r hadoop hdfs:///user/admin/data/googlebooks-eng-2gram.txt --output-dir=hdfs:///user/admin/data/recent-bigrams-output --conf-path=wordcount-mrjob.conf
"""

import csv
from io import StringIO
from mrjob.job import MRJob

class RecentBigrams(MRJob):

    def mapper(self, _, raw_line):
        # Wrap the raw text in a file-like buffer so csv.reader can parse tabs & quotes
        buffer = StringIO(raw_line)
        for cols in csv.reader(buffer, delimiter='\t', quotechar='"'):
            # Skip rows that don’t have at least [bigram, year]
            if len(cols) < 2:
                continue

            phrase = cols[0]
            year_str = cols[1]

            # Convert year to integer; drop rows with non-integer years
            try:
                year_int = int(year_str)
            except ValueError:
                continue

            # Emit the bigram and its occurrence year
            yield phrase, year_int

    def reducer(self, phrase, year_iterable):
        # Determine the earliest year this bigram appears
        first_inst = min(year_iterable)
        # Only output if that first appearance is after 1992
        if first_inst > 1992:
            yield phrase, first_inst

if __name__ == '__main__':
    RecentBigrams.run()
