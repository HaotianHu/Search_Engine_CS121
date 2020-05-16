from bs4 import BeautifulSoup
from collections import defaultdict
import json
import lxml
from pymongo import MongoClient
import re
import sys
import math
import time
import nltk
#nltk.download('all')
from nltk.corpus import stopwords

stopword = set(stopwords.words('english'))

class Indexer():
        def _setup(self, db_name : str, db_collection, keep_old_index : bool):

                self._db_host = MongoClient('localhost', 27017)
                self._db = self._db_host[db_name]
                self._collection = self._db[db_collection] 
                if( not keep_old_index ): 
                        self._collection.drop()

        def __init__(self, html_loc_json, db_name : str, db_collection : str, keep_old_index : bool):
                self._corpus_json = html_loc_json 
                self._total_documents = 0 
                self._inverted_index = defaultdict(dict) 
                self._setup( db_name, db_collection, keep_old_index)

        def _parse_html(self, text: str, tokens_dict: dict):

                for token in re.findall(re.compile(r"[A-Za-z0-9]+"), text):
                        if (token in stopword): 
                                        continue
                        tokens_dict[token.lower()] += 1
                return tokens_dict

        def insert_into_db(self):
                self._collection.insert_many( self._inverted_index.values() )

        def create_index(self):
                corpus_data = json.load(open(self._corpus_json))

                for doc_id, url in corpus_data.items():
                       # if( self._total_documents > 20 ): 
                        #         break;
                        html_id_info = doc_id.split("/") 
                        file_name = "{}/{}/{}".format("WEBPAGES_RAW", html_id_info[0], html_id_info[1])
                        html_file = open(file_name, 'r', encoding = 'utf-8')
                        soup = BeautifulSoup(html_file, 'lxml')

                        tokens_dict = defaultdict(int)
                        self._parse_html(soup.get_text(), tokens_dict)
                        
                        doc_title_tag = soup.find("title") 
                        doc_h1_tag = soup.find("h1") 
                        doc_h2_tag = soup.find("h2") 
                        doc_h3_tag = soup.find("h3") 
                                                                
                        self._total_documents += 1
                        for (token, frequencies) in tokens_dict.items():
                                weight_multiplier = 1.0 
                                if( (doc_title_tag is not None) and (doc_title_tag.string is not None) and (token in doc_title_tag.string.lower()) ):
                                        weight_multiplier = weight_multiplier + 0.40
                                if( token in url.lower() ):
                                        weight_multiplier = weight_multiplier + 0.35                                                    
                                if( (doc_h1_tag is not None) and (doc_h1_tag.string is not None) and (token in doc_h1_tag.string.lower()) ):
                                        weight_multiplier = weight_multiplier + 0.30
                                if( (doc_h2_tag is not None) and (doc_h2_tag.string is not None) and (token in doc_h2_tag.string.lower()) ):
                                        weight_multiplier = weight_multiplier + 0.25
                                if( (doc_h3_tag is not None) and (doc_h3_tag.string is not None) and (token in doc_h3_tag.string.lower()) ):
                                        weight_multiplier = weight_multiplier + 0.20

                                if (token not in self._inverted_index):
                                        self._inverted_index[token] = {"_id" : token, "Doc_info" : defaultdict(dict) }
                                self._inverted_index[token]["Doc_info"][doc_id]["tf"] = frequencies
                                self._inverted_index[token]["Doc_info"][doc_id]["weight_multiplier"] = weight_multiplier

                        print("Parsed {} documents so far".format(self._total_documents))
                print(len(self._inverted_index))
        def search(self,query_list:list):
                for term1 in query_list:
                        term = term1.lower()
                        if term in self._inverted_index.keys():
                                print('token: ',term)
                                for doc_info in list(self._inverted_index[term]['Doc_info']):
                                        print('    ','doc_id: ',doc_info,', tf-idf: ', self._inverted_index[term]['Doc_info'][doc_info]['tf'])  
              
                
        def get_total_documents(self):
                return self._total_documents

        def get_total_tokens(self, tokens_dict: dict):
                return self._collection.count()

if __name__ == "__main__":
        index_builder = Indexer("WEBPAGES_RAW/bookkeeping.json", "CS121_Index", "HTML_Corpus_Index", False)
        index_builder.create_index()
        index_builder.insert_into_db()
        #index_builder.search(['Informatics','Mondego','Irvine'])
        
