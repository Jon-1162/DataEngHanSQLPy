# Problem 1: Data Deduplication with Limited Memory
# In data pipelines, a common issue is encountering duplicate records, especially when data originates
# from multiple sources. Write a Python function to remove duplicates from a massive dataset (too large
# to fit into memory). You have only 10% of the data size as available memory

# data_stream = [(1, "a"), (2, "b"), (1, "a"), (3, "c")]
# unique_ids = deduplicate_large_dataset(data_stream)

# Ideas/Thought process - - - - - - - - - - - - - - - - - - - - - - - 
#- Add all elements to a set and remove form list - no,  cant work, filling a set takes memory
#- Check for duplicates in batches of 9.99%(accounts for the main value being compared) in a 
#  loop for each val ~o(n^2)
#- Convert tuples to just their ints O(n), merge sort the ints o(nlogn) in groups of 10%, parse 
#  sorted data stream checking for sequential repeats removing them O(n), create sets of 
#  size <= 10% O(n),  total 3*O(n) + o(nlogn) = o(nlogn)
# - After some work its unclear to me how further use merge sort with the 10% constraint. So instead 
#   with the sub arrays of size <=10% I am going to binary search through them. Binary search has a 
#   time of O(logn). So iterating through all the values then searching for them in each subarray 
#   should be O(logn) * n so o(nlogn) which is within the previuos time complexity. When the duplicates
#   are found the will be deleted.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

from math import floor
import random
import string
from typing import Iterable, Tuple, Any, Set
from bisect import bisect_left # used chat gpt to find how to do binary search easily


#  https://www.tutorialspoint.com/python-n-random-tuples-list
data_stream = [(random.randint(1, 3), random.choice(string.ascii_lowercase)) for _ in range(200)]

# Test data
# data_stream = [(3, 'x'), (4, 'f'), (1, 'i'), (2, 'b'), (1, 'b'), (3, 'w'), (3, 'z'), (1, 'f'), (5, 'm'), (4, 'i'), (5, 'g'), (3, 'r'), (1, 'v'), (3, 'v'), (4, 'z'), (3, 'g'), (4, 'a'), (3, 'y'), (2, 'd'), (1, 'i'), (2, 'd'), (2, 'i'), (5, 't'), (2, 'i'), (1, 'e'), (5, 's'), (2, 'w'), (5, 'd'), (3, 'r'), (2, 's'), (3, 's'), (4, 'l'), (4, 'g'), (4, 'a'), (5, 'a'), (1, 'w'), (1, 'j'), (4, 's'), (3, 'w'), (3, 'y'), (1, 'y'), (4, 'g'), (1, 'b'), (5, 'k'), (1, 'g'), (3, 'q'), (5, 'i'), (1, 't'), (2, 's'), (1, 'q'), (2, 'd'), (5, 'r'), (4, 'i'), (5, 'v'), (2, 'd'), (2, 'z'), (3, 'z'), (5, 'n'), (5, 'r'), (2, 'n'), (3, 'f'), (1, 'w'), (4, 'm'), (5, 'j'), (5, 'j'), (5, 'x'), (2, 'y'), (3, 'a'), (1, 'c'), (4, 'x'), (2, 'w'), (3, 'k'), (5, 'g'), (4, 'h'), (3, 'a'), (5, 'l'), (4, 'i'), (4, 'u'), (1, 'w'), (5, 'z'), (3, 'y'), (3, 'o'), (1, 'i'), (1, 'r'), (1, 's'), (2, 'i'), (2, 'x'), (5, 'w'), (3, 'w'), (4, 'd'), (4, 'w'), (1, 'x'), (1, 's'), (5, 'u'), (2, 'g'), (3, 'c'), (1, 'z'), (5, 'h'), (2, 'r'), (3, 'r')]

# https://stackoverflow.com/questions/4528982/convert-alphabet-letters-to-number-in-python


def getTenPercIndexes(data_stream_size):
    sizeOfTenSlice = data_stream_size//10
    loopCount = floor(data_stream_size/sizeOfTenSlice)
    returnIndexes = []
    currEdgeIndex = 0
    for _ in range(loopCount):#use the right for loop vals
        low = currEdgeIndex
        high = low + sizeOfTenSlice -1
        returnIndexes.append((low, high))
        currEdgeIndex = high+1
    returnIndexes.append((currEdgeIndex, data_stream_size-1))
    return returnIndexes

# https://en.wikipedia.org/wiki/Merge_sort
# https://www.w3schools.com/dsa/dsa_algo_mergesort.php
def merge(left, right):

    result = []
    i = j = 0
    
    while i < len(left) and j < len(right):
        if left[i] < right[j]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
            
    result.extend(left[i:])
    result.extend(right[j:])
    
    return result

# https://en.wikipedia.org/wiki/Merge_sort
# https://www.w3schools.com/dsa/dsa_algo_mergesort.php
def mergeSort(arr):
    step = 1  # Starting with sub-arrays of length 1
    length = len(arr)
    
    while step < length:
        for i in range(0, length, 2 * step):
            left = arr[i:i + step]
            right = arr[i + step:i + 2 * step]
            
            merged = merge(left, right)
            
            # Place the merged array back into the original array
            for j, val in enumerate(merged):
                arr[i + j] = val
                
        step *= 2  # Double the sub-array length for the next iteration
        
    return arr

# Test if there are duplicates in an array
def check_for_duplicates(int_list):
    unique_integers = set(int_list)
    
    if len(unique_integers) < len(int_list):
        return True  
    else:
        return False  

# Returns a list of index of duplicates 
def noteIfFound(subArr, dataPoint, realLower):
    duplicateIndex = bisect_left(subArr, dataPoint)
    matches = []
    
    if duplicateIndex < len(subArr) and subArr[duplicateIndex] == dataPoint:
        
        while duplicateIndex < len(subArr) and subArr[duplicateIndex] == dataPoint:
            # print("Found duplicate of "+str(dataPoint)+" at "+str(duplicateIndex)+"("+str(duplicateIndex + realLower)+")")
            matches.append(duplicateIndex + realLower)
            duplicateIndex += 1

    if matches != []:
        return matches



# the deduplicate_large_dataset function
def deduplicate_large_dataset(data_stream: Iterable[Tuple[int, Any]]) -> Set[int]:
    data_streamSize = len(data_stream)
    dataTenPercCount = data_streamSize//10
    indexes = getTenPercIndexes(data_streamSize)
    slicesCount = len(indexes)
    copyOfDataStream = data_stream.copy()
    # print(indexes)
    

    # convert data to integer dataIDs 10% or less at a time
    for i in range(0, slicesCount):
        lowerBound = indexes[i][0]
        upperBound = indexes[i][1]
        for j in range(lowerBound, upperBound+1):
            newData = data_stream[j][0]
            data_stream[j] = newData
    debugIntIDArr = data_stream.copy()
    
    
    # print("The sub arrays content-------------------------------------------")
    # Merge sort the data_stream 10% at a time
    for i in range(0, slicesCount):
        lowerBound = indexes[i][0]
        upperBound = indexes[i][1]
        for j in range(lowerBound, upperBound):
            sortedSubArr = mergeSort(data_stream[lowerBound: upperBound+1])
            data_stream[lowerBound: upperBound+1] = sortedSubArr;
        # print(data_stream[lowerBound: upperBound+1])
    # print(dupeArrIndices)

    # Search for duplicates in every 10% slice
    dupeArrIndices = []  
    dupeIndicesSet = {-1}
    for i in range(len(data_stream)):
        dataPoint = data_stream[i]
        dpSet= {-1}
        # print("noteIfFound searching for "+str(dataPoint) + "----------------------------")
        for j in range(slicesCount):
            lowerBound = indexes[j][0]
            upperBound = indexes[j][1]
            
            matches = noteIfFound(data_stream[lowerBound: upperBound+1], dataPoint, lowerBound)
            
            # print(matches)
            if matches != None:
                # print(matches)
                dpSet.update(matches)
                dupeArrIndices += matches
        dpSet.remove(-1)
        dpSet.pop() #Removes one instance of the data point so it is not deleted
        if len(dpSet) > 0:
            # print(dpSet)
            dupeIndicesSet.update(dpSet)
    dupeIndicesSet.remove(-1)
    
    # print("dupeIndiciesSet-------------")
    # print(dupeIndicesSet)
    # print("list duplication check for deletion-------------")
    # print (check_for_duplicates(list(dupeIndicesSet)))

    # Take the list of duplicate indices, sort them(this is to not have data move indexes after deletions),
    #  then delete all the indecies from the data stream with the list of duplicate indecies
    sortedIndices = sorted(dupeIndicesSet, reverse=True)
    for i in sortedIndices:
        if i < len(data_stream):
            del data_stream[i]  
    # print("final list dupe check-------------")
    # print (check_for_duplicates(data_stream))
    # print(data_stream)


    # convert dataIDs to original data 10% or less. Accessing 1 index at a time to stay in the 10% limit

    # print(data_stream)

    # Return data in 10% sets at a time to data_stream_output
    data_stream_output = []
    subSet = set()  # Initialize an empty set
    for i in range(len(data_stream)):
        subSet.add(data_stream[i])
        if (i + 1) % dataTenPercCount == 0:
            data_stream_output.append(subSet.copy())  
            subSet = set() 
    # Add any remaining elements in the final subset
    if subSet:
        data_stream_output.append(subSet.copy())  

    print("orignal data stream - - - - - - - - - - - - - - - - - - - - - - - - - -")
    print (copyOfDataStream)
    print("debugIntIDArr - - - - - - - - - - - - - - - - - - - - - - - - - - - - -")
    print(debugIntIDArr)
    print("deduplicated data stream - - - - - - - - - - - - - - - - - - - - - - - - - -")
    print (data_stream_output)


deduplicate_large_dataset(data_stream)