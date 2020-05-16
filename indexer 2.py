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
        def _setup(self, db_name : str, db_collection, keep_idx : bool):

                self._db_host = MongoClient('localhost', 27017)
                self._db = self._db_host[db_name]
                self._collection = self._db[db_collection] 
                if( not keep_idx ): 
                        self._collection.drop()

        def __init__(self, json_html, db_name : str, db_collection : str, keep_idx : bool):
                self._corpus_json = json_html 
                self._total_docus = 0 
                self._inverted_idx = defaultdict(dict) 
                self._setup( db_name, db_collection, keep_idx)

        def parse(self, s: str, tok_dict: dict):
                for tok in re.findall(re.compile(r"[A-Za-z0-9]+"), s):
                    if (tok in stopword): 
                        continue
                    tok_dict[tok.lower()] += 1
                return tok_dict

        def update_db_scores(self):
            for token_info in self._inverted_idx.values():
                for doc_info in token_info["Doc_info"].values():
                    doc_info["idf"] = self._total_docus / len(token_info["Doc_info"])
                    doc_info["tf-idf"] = (1+math.log10(doc_info["tf"])) * math.log10(doc_info["idf"])

        def db_insert(self):
                self._collection.insert_many( self._inverted_idx.values() )

        def create_index(self):
                corpus = json.load(open(self._corpus_json))

                for doc_id, url in corpus.items():
                        html_info = doc_id.split("/") 
                        fname = "{}/{}/{}".format("WEBPAGES_RAW", html_info[0], html_info[1])
                        html_file = open(fname, 'r', encoding = 'utf-8')
                        soup = BeautifulSoup(html_file, 'lxml')

                        tokens_dict = defaultdict(int)
                        self.parse(soup.get_text(), tokens_dict)
                        
                        title = soup.find("title") 
                        h1 = soup.find("h1") 
                        h2 = soup.find("h2") 
                        h3 = soup.find("h3") 
                        b = soup.find("b")
                        strong = soup.find("strong")
                                                                
                        self._total_docus += 1
                        for (tok, freq) in tokens_dict.items():
                                weight = 1.0 
                                if( tok in url.lower() ):
                                    weight = weight + 0.35 
                                if( (title is not None) and (title.string is not None) and (tok in title.string.lower()) ):
                                    weight = weight + 0.40                                                   
                                if( (h1 is not None) and (h1.string is not None) and (tok in h1.string.lower()) ):
                                    weight = weight + 0.30
                                if( (h2 is not None) and (h2.string is not None) and (tok in h2.string.lower()) ):
                                    weight = weight + 0.25
                                if( (h3 is not None) and (h3.string is not None) and (tok in h3.string.lower()) ):
                                    weight = weight + 0.20
                                if( (b is not None) and (b.string is not None) and (tok in b.string.lower()) ):
                                    weight = weight + 0.1
                                if( (strong is not None) and (strong.string is not None) and (tok in strong.string.lower()) ):
                                    weight = weight + 0.1

                                if (tok not in self._inverted_idx):
                                        self._inverted_idx[tok] = {"_id" : tok, "Doc_info" : defaultdict(dict) }
                                self._inverted_idx[tok]["Doc_info"][doc_id]["tf"] = freq
                                self._inverted_idx[tok]["Doc_info"][doc_id]["weight_multiplier"] = weight

                        print("Parsed {} documents so far".format(self._total_docus))
                print(len(self._inverted_idx))

if __name__ == "__main__":
        indexer = Indexer("WEBPAGES_RAW/bookkeeping.json", "CS121_Index", "HTML_Corpus_Index", False)
        indexer.create_index()
        indexer.update_db_scores()
        indexer.db_insert()
        
