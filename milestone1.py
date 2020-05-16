from bs4 import BeautifulSoup
from collections import defaultdict
from pymongo import MongoClient
import json
import sys
import re

client = MongoClient('localhost', 27017)
db = client.CS121_Index
index = db.HTML_Corpus_Index
print(db.command('dbstats'))
print(db.command('dbstats')['indexSize'])
f = open("result.txt", "w+")
number_of_results = 20
res = index.find({"_id": 'informatics'})
tok = res.next()
f.write("The number of document: 37497"+'\n')
f.write("The number of [unique] words: "+ str(db.command('dbstats')['objects'])+'\n')
f.write("The number of index size: " + str(db.command('dbstats')['indexSize'])+'\n')
f.write('\n')
f.write('----------------------------------------------------------------------------\n')
f.write('\n')
def milestone1(query):
    
    try:
        f.write("The urls retrieved for query '{}':\n".format(query))
        f.write('\n')
        query = query.lower()
        count = 0
        res = index.find({"_id": query})
        tok = res.next()
        
        for ids in tok["Doc_info"]:
            if(count == number_of_results):
                break 
            json_data = json.load(open("WEBPAGES_RAW/bookkeeping.json"))
            f.write('   '+ str(count+1)+' : '+ json_data[ids] + '\n')
            count += 1
            f.write('\n')

        f.write('-------------------------------------------------------------------- \n')
        f.write('\n')
    except StopIteration:
        print(query, "Not Found")

milestone1("Informatics")
milestone1("Mondego")
milestone1("Irvine")
f.close()
