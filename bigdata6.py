# -*- coding: utf-8 -*-
"""
Created on Fri Oct 23 03:12:28 2020

@author: sneha
"""

from pyspark import SparkContext

def generateKeyValue(val):
    categoryList = val[2].split(",")   
    keyVal = []
    for category in categoryList:
        cat = category.replace("List(", "")
        cat = cat.replace(")", "")
        cat = cat.strip()
        keyVal.append((cat,1))    
    return keyVal
    

if __name__=="__main__":
    sc = SparkContext()
    #generate key-value pair of (category,1) , add the count for each category 
    #and sort in descending order, and take the top 10 values
    businessDetails = sc.textFile("C:/Users/sneha/business.csv").map(lambda val: val.split("::")).flatMap(generateKeyValue).reduceByKey(lambda x,y: x+y).sortBy(lambda val: val[1],ascending=False).take(10)
    #print(businessDetails)
    sc.parallelize(businessDetails).coalesce(1).saveAsTextFile("C:/Users/sneha/Question6")