from pymongo import MongoClient
import json

def ti_score( key_value_tuple ):
        return key_value_tuple[1]["tf-idf"]

class Search:
        def _db_setup(self, db_name, db_collection):
                self._db_host = MongoClient('localhost', 27017)
                self._db = self._db_host[db_name] 
                self._collection = self._db[db_collection]

        def __init__(self, db_name, db_collection):
                self._db_setup(db_name, db_collection)

        def query(self, s: str, ui1: str)->list:
                s1 = s.split()
                urls = {} 
                urls_tiscore = {}
                res = [] 
                bk = json.load(open("WEBPAGES_RAW/bookkeeping.json"))
                
                for query in s1:
                        result = self._collection.find({"_id": query})
                        try:
                                token_value = result.next()
                                docs_dict = token_value["Doc_info"]
                                count = 0
                                for doc_id, attributes in sorted(docs_dict.items(), key = ti_score, reverse = True):
                                        if(bk[doc_id] in urls_tiscore):
                                                urls_tiscore[bk[doc_id]][0] += 1
                                                urls_tiscore[bk[doc_id]][1] += docs_dict[doc_id]["tf-idf"]
                                
                                        else:
                                                urls_tiscore[bk[doc_id]] = [1,docs_dict[doc_id]["tf-idf"]]
                                        count += 1
                                        if (count == int(ui1)):
                                                break
                        except StopIteration:
                                pass

                count = len(s1)
                while(True):
                        if(len(urls) >= int(ui1) or count == 0): 
                                break
                        for url,tiscore in list(urls_tiscore.items()):
                                if( tiscore[0] != count):
                                    continue
                                else:
                                    urls[url] = tiscore
                        count -= 1
                info = sorted(urls.items(),  key=lambda x: (x[1][0],x[1][1]), reverse = True)
                for url,tiscore in info:
                        if(len(res) >= int(ui1)):
                                break
                        else:
                                res.append((url,tiscore))
                return res
       

        def result_list(self, urls):
                l = []
                rank  = 1
                for url, tiscore in urls:
                        r_url = "{})  url: {}".format(rank, url)
                        score = '     tf‐idf scoring: '+ str(tiscore[1])
                        l.append(r_url)
                        l.append(score)
                        rank+=1
                return l
                        


if __name__ == "__main__":
        print("Starting Search Engine...")
        print("Search Engine Ready")
        print("Hello there")
        search = Search("CS121_Index", "HTML_Corpus_Index")
        while True:
                ui = input("Enter a query to search or enter 'quit' to exit the search engine: ")
                if (ui == "quit"):
                        break
                ui1 = input("How many results you want to see?:")
                
                res_url = search.query(ui.lower(), ui1)
                l = search.result_list(res_url)
                for i in l:
                        print(i)

        print("Bye, have a good day")
