from mrjob.job import MRJob
from mrjob.step import MRStep
import re 

class getTop10Cities(MRJob):
    def steps(self):
        return [
            MRStep (mapper=self.mapper_get_cities_count, reducer=self.reducer_get_cities_count),
            MRStep (reducer=self.reducer_top_cities)
            ]

    def mapper_get_cities_count(self, _, line):
        (business_id,city,name,review_count,category )= line.split('\t')
        yield city,1
   
    def reducer_get_cities_count(self, key, values):
        yield None, (sum(values), key)
    
    def reducer_top_cities(self, _, cities_counts) :   
        i=0
        for count, key in sorted(cities_counts, reverse=True):
            yield ('%d' % int(count), key)
            i=i+1
            if (i == 10):
                break

if __name__ == "__main__":
    getTop10Cities.run()
