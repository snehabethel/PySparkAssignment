# -*- coding: utf-8 -*-
"""
Created on Fri Oct 23 02:28:10 2020

@author: sneha
"""

from pyspark import SparkContext


if __name__=="__main__":
    sc = SparkContext()
    #generate the business data into list
    businessFile = sc.textFile("C:/Users/sneha/business.csv").map(lambda val: val.split("::"))
    #generate the data that contains stanford in its address, extract the business id and category as key-val pair
    stanfordFile = businessFile.filter(lambda x: "stanford" in x[1].lower()).map(lambda val:(val[0], val[2]))
    #generate the reviews data , extract the business id as key and (userid, rating) as value
    reviews = sc.textFile("C:/Users/sneha/review.csv").map(lambda val: val.split("::")).map(lambda val: (val[2], (val[1], val[3])))
    #Join the above files using business id and print the userid and rating 
    output = stanfordFile.join(reviews).distinct().map(lambda val:str(val[1][1][0]) + "\t" + str(val[1][1][1]))
    output.repartition(1).saveAsTextFile("C:/Users/sneha/Question3")

