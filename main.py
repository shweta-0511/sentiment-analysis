import boto3
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

spark = SparkSession.builder \
    .appName('application') \
    .config("spark.hadoop.fs.s3a.access.key", '****') \
    .config("spark.hadoop.fs.s3a.secret.key", '####') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
    .config("fs.s3a.endpoint", "s3.amazonaws.com")\
    .getOrCreate()

s3_client =boto3.client('s3')
s3_bucket_name='x21209251-nci-project'
s3 = boto3.resource('s3',aws_access_key_id= '****',aws_secret_access_key='####')

def sentiment_scores(x):
    sid_obj = SentimentIntensityAnalyzer()
    business_id = x.business_id
    text = x.text
    sentence = x.text
    sentiment_dict = sid_obj.polarity_scores(sentence)
    sentiment_score = sentiment_dict['compound']
    return (business_id, text, sentiment_score)

def sentiment_words(x):
    sid_obj = SentimentIntensityAnalyzer()
    word = x.keywords
    sentiment_dict = sid_obj.polarity_scores(word)
    if sentiment_dict['pos'] == 1.0:
        label = 'positive'
    elif sentiment_dict['neg'] == 1.0:
        label = 'negative'
    else :
        label = 'neutral' 
    return (word, label)


if __name__ == '__main__':
    my_bucket=s3.Bucket(s3_bucket_name)
    bucket_list = []
    for file in my_bucket.objects.filter(Prefix = 'dia'):
        file_name=file.key
        if file_name.find(".json")!=-1:
            bucket_list.append(file.key)
    
    path_business = 's3a://'+s3_bucket_name+'/'+bucket_list[0]
    path_review = 's3a://'+s3_bucket_name+'/'+bucket_list[1]    
    
    df_business = spark.read.json(path_business)
    df_review = spark.read.json(path_review)
    
    print("Question 1 and Question 2")
    print("Preprocessing business data")
    print("Dropping unwanted columns")
    business = df_business.drop('attributes','latitude','longitude','postal_code','address','hours','is_open','stars','state')
    print("Checking for null")
    business_null = business.select([count(when((col(c) == '' ) | \
                                col(c).isNull() | \
                                isnan(c), c 
                            )).alias(c)
                        for c in business.columns])
    print("Dropping records with NULL categories")
    business = business.filter("categories is not null")
    print("Checking for duplicate business ids")
    business \
        .groupby(['business_id']) \
        .count() \
        .where('count > 1') \
        .sort('count', ascending=False) \
       
    business = business.withColumn("category",split('categories',',')[0])
    business = business.drop('categories')
    business.select('category').show(truncate=False) 
    
    print("Preprocessing reviews data")
    print("Dropping unwanted columns")
    review = df_review.drop('cool','funny','useful','user_id','stars','date')
    print("Checking for null")
    review_null = review.select([count(when((col(c) == '' ) | \
                                col(c).isNull() | \
                                isnan(c), c 
                            )).alias(c)
                        for c in review.columns])

    print("Checking for duplicate review ids")
    review \
        .groupby(['review_id']) \
        .count() \
        .where('count > 1') \
        .sort('count', ascending=False)
    
    print("Dropping review id column")
    review = review.drop('review_id')
    
    print("Question 3")
    review_list = review.rdd.map(lambda x: sentiment_scores(x))
    schema = ["business_id", "text", "sentiment_score"]
    sentiment_df = review_list.toDF(schema)
    
    sentiment = sentiment_df.withColumn("sentiment_label", when((sentiment_df.sentiment_score >= 0.05), lit("Positive")).when((sentiment_df.sentiment_score < 0.05), lit("Negative")))

    positive = sentiment.where(sentiment.sentiment_label=='Positive')
    negative = sentiment.where(sentiment.sentiment_label=='Negative')
    
    positive = positive.withColumn('keywords', explode(split(col('text'), ' ')))
    positive_list = positive.rdd.map(lambda x: sentiment_words(x))
    
    schema = ["keyword", "label"]
    positive_keyword = positive_list.toDF(schema)
    
    positive_keyword = positive_keyword.where(positive_keyword.label=='positive').select("keyword")

    negative = negative.withColumn('keywords', explode(split(col('text'), ' ')))
    negative_list = negative.rdd.map(lambda x: sentiment_words(x))
    
    schema = ["keyword", "label"]
    negative_keyword = negative_list.toDF(schema)
    
    negative_keyword = negative_keyword.where(negative_keyword.label=='negative').select("keyword")    
    
    print("Question 4")
    print("Selecting two restaurants with maximum reviews for sentiment analysis")
    res = business.where(business.category == 'Restaurants')
    res.createOrReplaceTempView('res')
    res_top2 = spark.sql('select * from res order by review_count desc limit 2')
    
    print("Renaming business id column for joining the two dataframes business and review")
    sentiment_upd = sentiment.withColumnRenamed('business_id','bus_id')
    
    df_joined = res_top2.join(sentiment_upd, res_top2.business_id == sentiment_upd.bus_id,"inner").select('business_id','name','sentiment_label')
    
    print("Data for question 1 and question2")
    business.coalesce(1).write.mode('overwrite').options(delimiter="\t").csv("hdfs://localhost:54310/21209251/input/ques1/")
    
    print("Data for question 3 : Positive")
    positive_keyword.coalesce(1).write.mode('overwrite').options(delimiter="\t").csv("hdfs://localhost:54310/21209251/input/ques3_pos/")
    
    print("Data for question 3 : Negative")
    negative_keyword.coalesce(1).write.mode('overwrite').options(delimiter="\t").csv("hdfs://localhost:54310/21209251/input/ques3_neg/")
    
    print("Data for question 4")
    df_joined.coalesce(1).write.mode('overwrite').options(delimiter="\t").csv("hdfs://localhost:54310/21209251/input/ques4/")
    
    print("Dropping dataframes to free up memory")
    del df_joined
    del negative_keyword
    del positive_keyword
    del sentiment_upd
    del res_top2
    del res
    del negative
    del positive
    del sentiment
    del sentiment_df
    del business
    del review
    del business_null
    del review_null
    del df_business
    del df_review
