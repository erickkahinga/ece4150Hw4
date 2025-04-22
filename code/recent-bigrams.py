#!/usr/bin/env python3
"""
Processes `googlebooks-eng-2gram.txt` to list bigrams first recorded after 1992.

Steps:
1. Mapper:
   - Read each tab separated line.
   - Extract (bigram, year) pairs.
2. Reducer:
   - Find the minimum year per bigram.
   - Emit only those with first_year > 1992.
3. Run:
   python recent-bigrams.py -r hadoop hdfs:///user/admin/data/googlebooks-eng-2gram.txt --output-dir=hdfs:///user/admin/data/recent-bigrams-output --conf-path=wordcount-mrjob.conf
"""


import csv
from io import StringIO
from mrjob.job import MRJob


class RecentBigrams(MRJob):

    def mapper(self, _, raw_line):
        # Wrap line so csv.reader can parse it as tab separated fields
        buffer = StringIO(raw_line)
        for cols in csv.reader(buffer, delimiter='\t', quotechar='"'):
            # Skip badly formatted rows
            if len(cols) < 2:
                continue

            phrase = cols[0]
            year_str = cols[1]

            try:
                year_int = int(year_str)
            except ValueError:
                continue # Skip non‑integer year entries

            yield phrase, year_int

    def reducer(self, phrase, year_iterable):
        # Find the earliest year this bigram appears
        first_year = min(year_iterable)
        if first_year > 1992:
            yield phrase, first_year

if __name__ == '__main__':
    RecentBigrams.run()
