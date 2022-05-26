# -*- coding: utf-8 -*-
"""
Split event logs across partitions and offsets for loading into Kafka
  - 1000 messages per log file
  - 10 log files per topic partition
  - number of partitions as required
"""

__author__ = "Kinshuk Rakshith"
__version__ = "1.0.0"
__license__ = "MIT"

import tarfile
import os.path

#list of suffixes of logfiles to read
readLogFilenum= [0,1]

#topics of interest based on "messageType" field in each log entry
# "messageType":1 trade
# "messageType":2 quote
# "messageType":9 close

#open one file per each topic to write
writeTradeLog = open("./kafkaLogs/trades/0-00000000000000000.json","w")
writeQuoteLog = open("./kafkaLogs/quotes/0-00000000000000000.json","w")
writeCloseLog = open("./kafkaLogs/closes/0-00000000000000000.json","w")

#keep track of filenumbers being written to
topicDict = {
    'trades':{'filePtr':writeTradeLog, 'path':"./kafkaLogs/trades/",'num':1, 'lines':0, 'partition':0, 'totalMsgs':0},
    'quotes':{'filePtr':writeQuoteLog, 'path':"./kafkaLogs/quotes/",'num':1, 'lines':0, 'partition':0, 'totalMsgs':0},
    'closes':{'filePtr':writeCloseLog, 'path':"./kafkaLogs/closes/",'num':1, 'lines':0, 'partition':0, 'totalMsgs':0}
    }

#go round in a loop processing logfiles
for seq in readLogFilenum:
    logfileName = 'data/160_050222_'+str(readLogFilenum[seq])+'.json'
    print('Processing '+ logfileName +'...')
    try:
        with open(logfileName,"r") as inputJson:
            #read eachline from logfile
            for line in inputJson:
                currentTopic = 'invalid'
                # identify topic
                if('\"messageType\":1,\"' in line):
                    #process it as a trade topic
                    currentTopic= 'trades'
                elif('\"messageType\":2,\"' in line):
                    #process it as a quote topic
                    currentTopic= 'quotes'
                elif('\"messageType\":9,\"' in line):
                    #process it as a quote topic
                    currentTopic= 'closes'
                #if 1000 lines in written file
                if currentTopic in topicDict.keys():
                    if(topicDict[currentTopic]['lines']==10000):
                        #close current writeLogFile
                        topicDict[currentTopic]['filePtr'].close()
                        #open new writeLogFile
                        topicDict[currentTopic]['filePtr']= open(topicDict[currentTopic]['path']+str(topicDict[currentTopic]['partition'])+"-00000000000000"+str(topicDict[currentTopic]['num']*1000)+".json","w")
                        #increment num for the next offset
                        topicDict[currentTopic]['num']+=1
                        #increment partition and reset num
                        if topicDict[currentTopic]['num']%11==0:
                            topicDict[currentTopic]['partition']+=1
                        #reset lines
                        topicDict[currentTopic]['lines']=0

                    # then write line to correct topic
                    if topicDict[currentTopic]['lines']==0:
                        topicDict[currentTopic]['filePtr'].write(line.strip('\n'))
                    else:
                        topicDict[currentTopic]['filePtr'].write('\n'+line.strip('\n'))
                    #increment number of lines in file
                    topicDict[currentTopic]['lines']+=1
                    #increment totalnumber of msgs for topic
                    topicDict[currentTopic]['totalMsgs']+=1

    except IOError as e:
        # report file error
        print('file error: ',e)

#close all open files
writeTradeLog.close()
writeQuoteLog.close()
writeCloseLog.close()
#print stats
for key in topicDict:
    print(key + " Files: "+ str(topicDict[key]['num']))
    print(key + " Msgs: "+ str(topicDict[key]['totalMsgs']))
    print(key + " Partitions: "+ str(topicDict[key]['partition'] + 1))

##create tar.gz of each of the topic subfolders
#for key in topicDict:
#    with tarfile.open(key+".tar.gz", "w:gz") as tar:
#        tar.add(key+".tar.gz", arcname=os.path.basename("./kafkaLogs/"+key))