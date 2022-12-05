from mrjob.job import MRJob
from mrjob.step import MRStep
import re 

class getTop10Business(MRJob):
    def steps(self):
        return [
            MRStep (mapper=self.mapper_get_business_count, reducer=self.reducer_get_business_count),
            MRStep (reducer=self.reducer_top_business)
            ]

    def mapper_get_business_count(self, _, line):
        (business_id,city,name,review_count,category )= line.split('\t')
        yield category,1
    
    def reducer_get_business_count(self, key, values):
        yield None, (sum(values), key)
    
    def reducer_top_business (self, _, businesses_counts) :   
        i=0
        for count, key in sorted(businesses_counts, reverse=True):
            yield ('%d' % int(count), key)
            i=i+1
            if (i == 10):
                break

if __name__ == "__main__":
    getTop10Business.run()
