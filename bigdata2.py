# -*- coding: utf-8 -*-
"""
Created on Fri Oct 23 01:08:41 2020

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
    
if __name__=="__main__":
    sc = SparkContext()
    mutualFriendsFile = sc.textFile("C:/Users/sneha/Mutual_Friends.txt")
    #Generate the pair of friends and count of mutual friends   
    mutualList = mutualFriendsFile.map(lambda x: x.split("\t")).flatMap(generateKeyValue).reduceByKey(lambda v1, v2: v1.intersection(v2)).map(lambda x: [x[0], len(x[1])])
    #find the max value of the count of mutual friends
    maxMutual = mutualList.max(key = lambda x: x[1])[1]
    #print(maxMutual)
    
    #filter out the output based on whether the count of mutual friends == max value
    mutualList = mutualList.filter(lambda x: x[1] == maxMutual)
    mutualList.coalesce(1).saveAsTextFile("C:/Users/sneha/Question2")
    
    
    
    





