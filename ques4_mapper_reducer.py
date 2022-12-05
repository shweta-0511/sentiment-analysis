from mrjob.job import MRJob
from mrjob.step import MRStep

class getPositivetoNegativeReviewRatio(MRJob):
    def steps(self):
        return [MRStep (mapper=self.mapper_reviews,reducer=self.reducer_reviews) ]


    def mapper_reviews(self, _, line):
        (business_id, name, sentiment_label)= line.split('\t')
        yield name+'~'+sentiment_label,1


    def reducer_reviews(self,key,values):
        yield key, sum(values)


if __name__ == "__main__":
    getPositivetoNegativeReviewRatio.run()
