# -*- coding: utf-8 -*-
"""
Created on Fri Oct 23 03:00:51 2020

@author: sneha
"""

from pyspark import SparkContext

if __name__=="__main__":
    sc = SparkContext()
    #generate the business data into list and extract the business id as key and (address,category) as value
    businessFile = sc.textFile("C:/Users/sneha/business.csv").map(lambda val: val.split("::")).map(lambda val: (val[0], (val[1], val[2])))
    #generate the reviews data , extract the business id as key and rating as value
    reviewsFile = sc.textFile("C:/Users/sneha/review.csv").map(lambda val: val.split("::")).map(lambda val: (val[2], val[3]))
    #Find the average of the ratings, and map those values having average
    avgReviews = reviewsFile.mapValues(lambda val: (float(val),1)).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])).mapValues(lambda val: val[0]/val[1]).map(lambda val: (val[0], val[1]))
    #Join the average rating and the business files to generate the business that fulfills average rating
    outputList = avgReviews.join(businessFile).distinct().sortBy(lambda x: x[1][0],False).take(10)
    # formatting the input
    output = sc.parallelize(outputList).map(lambda val: str(val[0] + '\t' + str(val[1][1][0]) + '\t' + str(val[1][1][1]) +'\t' + str(val[1][0])))
    output.coalesce(1).saveAsTextFile("C:/Users/sneha/Question4")
