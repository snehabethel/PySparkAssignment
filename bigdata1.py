# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 03:18:46 2020

@author: sneha
"""

from pyspark import SparkContext

#This method generates the key-value pairs comprising of 
# key as (friend1-friend2) pair and
# value as list of friends
def generateKeyValue(val):
    friend1 = val[0]
    listOfFriends = val[1].split(",")
    keyVal = []
    for friend2 in listOfFriends:
        if friend1!= "" and friend2 != "":
            key = tuple(sorted([friend1,friend2]))
            keyVal.append((key, set(listOfFriends)))
    return keyVal

#This method prints the pair of friends and total count of their mutual friends   
def generateMutualCount(val):
    line = val[0][0] +","+ val[0][1] +": "+ str(len(val[1]))
    return line

if __name__=="__main__":
    sc = SparkContext()
    mutualFriendsFile = sc.textFile("C:/Users/sneha/Mutual_Friends.txt")
    output = mutualFriendsFile.map(lambda x: x.split("\t")).flatMap(generateKeyValue).reduceByKey(lambda x,y : x.intersection(y)).sortByKey().map(lambda x: generateMutualCount(x))
    output.coalesce(1).saveAsTextFile("C:/Users/sneha/Question1")
  

