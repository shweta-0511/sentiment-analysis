# sentiment-analysis
Below steps should be followed step wise to run the code:

Download and place the files on S3.B
elow is the link to download files from:
https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset?select=yelp_academic_dataset_business.json
https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset?select=yelp_academic_dataset_review.json
Generate AWS keys and setup IAM role and attach the same to your EC2 instance.
Make below changes in main.py:
1. Update bucket and folder name in main.py.
2. Replace **** with AWS access_key and #### with secret_key.
3. Save and close the file.
4. Login with hduser. Start the datanodes and namenodes.
5. Run main.py script >> spark-submit main.py
6. Verify that files are generated at defined location on hdfs.
7. Run mapper reducer script with below commands:
	i. python3 ques1_mapper_reducer.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop-streaming-3.3.1.jar hdfs://localhost:54310/21209251/input/ques1/ --output hhdfs://localhost:54310/21209251/output/ques1/
	ii. python3 ques2_mapper_reducer.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop-streaming-3.3.1.jar hdfs://localhost:54310/21209251/input/ques1/ --output hdfs://localhost:54310/21209251/output/ques2/
	iii. python3 ques3_mapper_reducer.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop-streaming-3.3.1.jar hdfs://localhost:54310/21209251/input/ques3_pos/ --output hhdfs://localhost:54310/21209251/output/ques3_pos/
	iv. python3 ques3_mapper_reducer.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop-streaming-3.3.1.jar hdfs://localhost:54310/21209251/input/ques3_neg/ --output hdfs://localhost:54310/21209251/output/ques3_neg/
	v. python3 ques4_mapper_reducer.py -r hadoop --hadoop-streaming-jar /usr/lib/hadoop-streaming-3.3.1.jar hdfs://localhost:54310/21209251/input/ques4/ --output hdfs://localhost:54310/21209251/output/ques4/
