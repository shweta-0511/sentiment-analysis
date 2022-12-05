from mrjob.job import MRJob
from mrjob.step import MRStep
import re 

class getKeyword(MRJob):
    def steps(self):
        return [
            MRStep (mapper=self.mapper_keyword_count, reducer=self.reducer_keyword_count)
            ]

    def mapper_keyword_count(self, _, line):
        (keyword)= line.split(',')
        yield keyword,1
    
    def reducer_keyword_count(self, key, values):
        yield key,sum(values)
    
if __name__ == "__main__":
    getKeyword.run()
