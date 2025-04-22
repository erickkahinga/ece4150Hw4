#!/usr/bin/python

from mrjob.job import MRJob

class MRmyjob(MRJob):
    def mapper(self, _, line):
        # first non-self parameter is key, but in practice None
        wordlist = line.split()
        for word in wordlist:
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRmyjob.run()
