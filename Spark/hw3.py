# You need to import everything below
import pyspark
from pyspark import SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F

from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics

import lime 
from lime import lime_text
from lime.lime_text import LimeTextExplainer

import numpy as np
import csv
import math
import ast
from operator import add

sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("hw3") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

########################################################################################################
# Load data
categories = ["alt.atheism", "soc.religion.christian"]
LabeledDocument = pyspark.sql.Row("category", "text")

def categoryFromPath(path):
    return path.split("/")[-2]

    
def prepareDF(typ):
    rdds = [sc.wholeTextFiles("/user/tbl245/20news-bydate-" + typ + "/" + category)\
              .map(lambda x: LabeledDocument(categoryFromPath(x[0]), x[1]))\
            for category in categories]
    return sc.union(rdds).toDF()

train_df = prepareDF("train").cache()
test_df  = prepareDF("test").cache()

#####################################################################################################
""" Task 1.1
a.	Compute the numbers of documents in training and test datasets. Make sure to write your code here and report
    the numbers in your txt file.
b.	Index each document in each dataset by creating an index column, "id", for each data set, with index starting at 0. 

""" 
# Your code starts here
cnt_train = train_df.count()
print("size of training set: {}".format(cnt_train))
cnt_test = test_df.count()
print("size of test set: {}".format(cnt_test))

train_df = train_df.withColumn("id1", F.monotonically_increasing_id())
test_df = test_df.withColumn("id1", F.monotonically_increasing_id())
train_df.createOrReplaceTempView("train_df")
test_df.createOrReplaceTempView("test_df")

query = """
select category, text, row_number() over (order by id1) as id
from train_df
"""

train_df = spark.sql(query)
train_df = train_df.withColumn('id', F.col('id')-1)

query = """
select category, text, row_number() over (order by id1) as id
from test_df
"""

test_df = spark.sql(query)
test_df = test_df.withColumn('id', F.col('id')-1)
test_df.show(5)

########################################################################################################
# Build pipeline and run
indexer   = StringIndexer(inputCol="category", outputCol="label")
tokenizer = RegexTokenizer(pattern=u'\W+', inputCol="text", outputCol="words", toLowercase=False)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
idf       = IDF(inputCol="rawFeatures", outputCol="features")
lr        = LogisticRegression(maxIter=20, regParam=0.001)

# Builing model pipeline
pipeline = Pipeline(stages=[indexer, tokenizer, hashingTF, idf, lr])

# Train model on training set
model = pipeline.fit(train_df)   #if you give new names to your indexed datasets, make sure to make adjustments here

# Model prediction on test set
pred = model.transform(test_df)  # ...and here

# Model prediction accuracy (F1-score)
pl = pred.select("label", "prediction").rdd.cache()
metrics = MulticlassMetrics(pl)
metrics.fMeasure()

#####################################################################################################
""" Task 1.2
a.	Run the model provided above. 
    Take your time to carefully understanding what is happening in this model pipeline.
    You are NOT allowed to make changes to this model's configurations.
    Compute and report the F1-score on the test dataset.
b.	Get and report the schema (column names and data types) of the model's prediction output.

""" 
# Your code for this part, IF ANY, starts here
print("the F1-score of the first model: {}".format(metrics.fMeasure()))
print(pred)

#######################################################################################################
#Use LIME to explain example
class_names = ['Atheism', 'Christian']
explainer = LimeTextExplainer(class_names=class_names)

# Choose a random text in test set, change seed for randomness 
test_point = test_df.sample(False, 0.1, seed = 10).limit(1)
test_point_label = test_point.select("category").collect()[0][0]
test_point_text = test_point.select("text").collect()[0][0]

def classifier_fn(data):
    spark_object = spark.createDataFrame(data, "string").toDF("text")
    pred = model.transform(spark_object)   #if you build the model with a different name, make appropriate changes here
    output = np.array((pred.select("probability").collect())).reshape(len(data),2)
    return output

exp = explainer.explain_instance(test_point_text, classifier_fn, num_features=6)
print('Probability(Christian) =', classifier_fn([test_point_text])[0][0])
print('True class: %s' % class_names[categories.index(test_point_label)])
exp.as_list()

#####################################################################################################
""" 
Task 1.3 : Output and report required details on test documents with ID’s 0, 275, and 664.
Task 1.4 : Generate explanations for all misclassified documents in the test set, sorted by conf in descending order, 
           and save this output (index, confidence, and LIME's explanation) to netID_misclassified_ordered.csv for submission.
"""
# Your code starts here
# 1.3
pred.createOrReplaceTempView("pred")

query = """
select id, category, probability, prediction
from pred
where id = '0' or id = '275' or id = '664'
"""

res = spark.sql(query)
res.show()

def task1_3(id):
	test_point = test_df.filter(F.col('id') == id)
	test_point_label = test_point.select("category").collect()[0][0]
	test_point_text = test_point.select("text").collect()[0][0]
	exp = explainer.explain_instance(test_point_text, classifier_fn, num_features=6)
	print('Probability(Christian) =', classifier_fn([test_point_text])[0][0])
	print('True class: %s' % class_names[categories.index(test_point_label)])
	exp.as_list()
	print(exp.as_list())

task1_3(0)
task1_3(275)
task1_3(664)

# 1.4
def explanation(x, num):
    return explainer.explain_instance(x, classifier_fn, num_features=num).as_list()

def listToCSVFile(fileName, list0):
    with open(fileName, 'w', newline='') as myfile:
        wr = csv.writer(myfile, delimiter = ',', quoting=csv.QUOTE_ALL)
        for row in list0:
            wr.writerow(row)

def task1_4(pred, num_features):
	misclassified = pred.rdd.filter(lambda x: x[3] != x[9])
	rdd0 = misclassified.map(lambda x: (x[2], abs(x[8][0] - x[8][1]), x[1]))
	list0 = rdd0.collect()
	# for each misclassified document
	list1 = []
	for md in list0:
		list1.append([md[0], md[1], explanation(md[2], num_features)])
	return list1

list1 = task1_4(pred, 6)

list1.sort(key=lambda x: x[1], reverse=True)
fileName0 = 'xz2456_misclassified_ordered.csv'
listToCSVFile(fileName0, list1)

########################################################################################################
""" Task 1.5
Get the word and summation weight and frequency
"""
# Your code starts here
def task1_5(list0):
    list1 = []
    for md in list0:  # for each misclassified document
        for words in md[2]:  # md: (id, conf, text)
            list1.append([words[0], words[1], 1])
    rdd1 = sc.parallelize(list1)
    count = rdd1.map(lambda x: (x[0], x[2])).reduceByKey(add)
    weight = rdd1.map(lambda x: (x[0], abs(x[1]))).reduceByKey(add)
    # sort by frequency
    rdd1 = weight.join(count).map(lambda x: (x[0], x[1][1], x[1][0]))
    return rdd1

rdd1 = task1_5(list1)
# sort by frequency
rdd1 = rdd1.sortBy(lambda x: x[1], ascending=False)
list2 = rdd1.collect()
fileName1 = 'xz2456_words_weight.csv'
listToCSVFile(fileName1, list2)

########################################################################################################
""" Task 2
Identify a feature-selection strategy to improve the model's F1-score.
Codes for your strategy is required
Retrain pipeline with your new train set (name it, new_train_df)
You are NOT allowed make changes to the test set
Give the new F1-score.
"""
#Your code starts here
def getF1Score(model, test_df):
    pred = model.transform(test_df)
    pl = pred.select("label", "prediction").rdd.cache()
    metrics = MulticlassMetrics(pl)
    f1score = metrics.fMeasure()
    print("the F1-score of the model is : {}".format(f1score))
    return f1score

def filterByConf(list0, threshold):
    """
    filter out the misclassified documents which conf < threshold
    :param threshold:
    :return: a list
    """
    n1 = len(list0)
    end = 0
    for i in range(n1)[::-1]:
        if list0[i][1] >= threshold:
            end = i + 1
            break
    return list0[:end]

def sortByAvgWeight(rdd):
    return rdd.map(lambda x: [x[0], x[1], x[2], x[2]/x[1]]).sortBy(lambda x: x[3], ascending=False)

def sortByFrequncey(rdd):
    return rdd.sortBy(lambda x: x[1], ascending=False)

def sortByWeight(rdd):
	return rdd.sortBy(lambda x: x[2], ascending=False).map(lambda x: [x[0], x[2], x[1]])

def myRecombine(x, words_to_remove):
    # remove the words that help misclassification
    res = []
    words = x[1].split()
    for word in words:
        if word not in words_to_remove:
            res.append(word)
    text = ' '.join(res)
    return (x[0], text, x[2])

def remove_words(train_df, words_to_remove):
    return train_df.rdd.map(lambda x: myRecombine(x, words_to_remove)).toDF().selectExpr("_1 as category", "_2 as text", "_3 as id")

def countMisclassification(model, test_df):
    pred = model.transform(test_df)
    return pred.rdd.filter(lambda x: x[3] != x[9]).count()

def getWordsToRemove(list0, list1, t1, t2, avg_weight):
	n = len(list0)
	l0 = set()
	l1 = set()
	threshold1 = avg_weight * t2
	for i in range(n):
		if list0[i][1] >= t1:
			l0.add(list0[i][0])
		else:
			break
	for i in range(n):
		if list1[i][1] >= threshold1:
			l1.add(list1[i][0])
		else:
			break
	# print(l0)
	# print(l1)
	return set(l0).union(set(l1))

def task2(list0, list1, x, y, avg_weight):
	list2 = getWordsToRemove(list0, list1, x, y, avg_weight)
	train_df1 = remove_words(train_df, list2)
	model1 = pipeline.fit(train_df1)
	f1score1 = getF1Score(model1, test_df)
	return f1score1

def selectFeatures(list0, num=6):
	for line in list0:
		line[2] = line[2][:num]
	return

selectFeatures(list1, 4)
avg_count = 4

list3 = filterByConf(list1, 0.1)

# listToCSVFile('list3.csv', list3)
rdd3 = task1_5(list3)
avg_weight = (rdd3.map(lambda x: x[2]).sum())/(len(list3)*avg_count)

list4_1 = sortByFrequncey(rdd3).collect()
list4_2 = sortByWeight(rdd3).collect()

# listToCSVFile('list4_1.csv', list4_1)
# listToCSVFile('list4_2.csv', list4_2)

xmax = avg_count + 1
ymax = math.ceil(list4_2[0][1] / avg_weight) + 1
A = [[0 for _ in range(ymax)] for _ in range(avg_count+1)]
best_f1 = 0.0
best_x = 1
best_y = 0
res = []
list4 = set()

list4 = getWordsToRemove(list4_1, list4_2, 1, 0, avg_weight)

# find the best x and the best y, which f1-score is the highest
for x in range(avg_count, xmax):
	for y in range(ymax):
		list4.intersection(getWordsToRemove(list4_1, list4_2, x, y, avg_weight))



print(best_f1, best_x, best_y)
print(res)

# set0 = getWordsToRemove(list4_1, list4_2, 1, 0, avg_weight)
# for x, y in res:
# 	set0 = set0.intersection(getWordsToRemove(list4_1, list4_2, x, y, avg_weight))

# list4 = getWordsToRemove(list4_1, list4_2, best_x, best_y, avg_weight)

# list4_3 = getWordsToRemove(list4_1, list4_2, 3, 2, avg_weight)
# list4_4 = getWordsToRemove(list4_1, list4_2, 2, 3, avg_weight)
# list4 = list4_3.union(list4_4)



train_df1 = remove_words(train_df, list4)
model1 = pipeline.fit(train_df1)
print("Before removing the words, the F1-score is: {}".format(getF1Score(model, test_df)))
print("The highest F1-Score: {}".format(best_f1))
print("the number of misclassified documents of the previous model is: {}".format(countMisclassification(model, test_df)))
print("the number of misclassified documents of the current model is: {}".format(countMisclassification(model1, test_df)))

pred1 = model1.transform(test_df)
misclassified1 = pred1.rdd.filter(lambda x: x[3] != x[9])
rdd1 = misclassified1.map(lambda x: (x[2], abs(x[8][0]-x[8][1])))
misclassified = pred.rdd.filter(lambda x: x[3] != x[9])
rdd0 = misclassified.map(lambda x: (x[2], abs(x[8][0] - x[8][1]), x[1]))
rdd2 = rdd0.subtractByKey(rdd1)
list2 = rdd2.sortBy(lambda x: x[1], ascending=False).collect()
listToCSVFile('task2.csv', list2)
