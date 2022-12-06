# -*- coding: utf-8 -*-
"""
@author: Luis Racca & Joseph Alyndree

"""

import requests
import json
import time
import os
import random
from time import strftime, gmtime

# from threading import Thread
# import queue
import concurrent.futures

from bs4 import BeautifulSoup

#Inherit from Thread class to be able parallelize requests using all available cores 
class Requester(object) : 
    '''
    Make the requests needed and call Parsers object to process raw html and extract data from it
    '''
    def __init__(self,  alpha_tickers, beta_tickers,proxies, nb_threads, step_time, save_time) :
        
        #List of ticker that the Requester must request for from kraken public endpoints
        self.ticker = alpha_tickers
        self.total_tickers = alpha_tickers + beta_tickers
        print(self.total_tickers)
        
        #List of proxies usable by the Requester object
        self.proxies = proxies
        
        #Number of threads to use for parallel processing
        self.nb_threads = nb_threads
        
        #Set timers for time separated requests
        self.second_timer = time.time()
        self.hour_timer = time.time()
        self.half_day_timer = time.time()
        self.day_timer = time.time()

        #Init the starting time of the 6 hours period before having to change saving file
        self.start_time = time.time()
        
        self.step_time = step_time
        self.save_time = save_time
        
        #The header is needed for some of the requests
        self.header = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
        
        self.clean = True
        
        self.logs = ""
        
        self.Results_dic = {}
        
        while True : 
            #Dictionnary where data is stocked at each iterations
        
            #Toutes les 5 secondes 
            if time.time() >= int(self.second_timer) + self.step_time :
                #If the delta between 2 iterations is superior to 10s , set the timestamps to the old one + 10 --> Allow the timestamps to remain regular and to catch back the normal pace thanks to the faster iterations
                if time.time() - int(self.second_timer) >= step_time+1 :
                    self.second_timer = self.second_timer + step_time
                else : 
                    self.second_timer = str(time.time()).split(".")[0]
                    
                start = time.time()
                self.Process_Queries()
                print("Temps pour faire les requêtes : ", time.time() - start)
                
                start_2 = time.time()
                self.Process_raws()
                print("Temps pour faire le traitement : ", time.time() - start_2)
                
                #Save the process time for the considered timestamp to ease identifying outliers due mainly to connectivity problems
                process_time = time.time() - start
                self.Results_dic[self.second_timer].update({"process_time":process_time})
                
                self.Save_infos()
                
                print("Total time for 1 iteration : ",time.time() - start)
                print("Nombre de proxies encore dispos : ", len(self.proxies))
                print("Nombre de timestamps : ", len(self.Results_dic.keys()))
                print("Timestamp : ", self.second_timer)
                print("\n")
                #self.Save_results() --> Ouvre le fichier en local, met à jour avec le dic de résultats et gère les backups (il va falloir faire 1 fichier par jour ou qulque chose comme ça parce que sinon ça va être trop long à ouvrir/fermer à chaque timestamp)
                
        
    def Process_Queries(self) : 
        #https://docs.python.org/3/library/concurrent.futures.html Doc sur le multithreading avec concurrent.futures
        
        #Init of these parameters before queries
        self.raw_OHLC = {}
        self.raw_Orderbook = {}
        self.raw_Last_Trades = {}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.nb_threads) as executor:
            # Start the load operations and mark each future with its URL
            self.start = time.time()
            
            OHLC_generator = {executor.submit(self.Query_OHLC, ticker) for ticker in self.total_tickers}
            Orderbook_generator = {executor.submit(self.Query_Orderbook, ticker) for ticker in self.ticker}
            Last_trades_generator = {executor.submit(self.Query_Last_Trades, ticker) for ticker in self.ticker}
            Yahoo_Queries_generator = {executor.submit(self.Query_FGI),
                                      executor.submit(self.Query_Yahoo_Indices),
                                      executor.submit(self.Query_Yahoo_HighTech),
                                      executor.submit(self.Query_Yahoo_Internet),
                                      executor.submit(self.Query_Yahoo_Matieres),
                                      executor.submit(self.Query_Yahoo_Devises),
                                      executor.submit(self.Query_Yahoo_Aero),
                                      executor.submit(self.Query_Yahoo_Auto),
                                      executor.submit(self.Query_Yahoo_Energy),
                                      executor.submit(self.Query_Yahoo_BTP),
                                      executor.submit(self.Query_Yahoo_Industry),
                                      executor.submit(self.Query_Yahoo_Health),
                                      executor.submit(self.Query_Yahoo_Logistic),
                                      executor.submit(self.Query_Yahoo_Telecoms),
                                      }

    def Process_raws(self) :
        
        #Timer init with treatment to pass from float to int
        
        #Tag the data-point as clean if there is no error during raws treatment
        self.clean = True
        
        #Init of the result dic for the considered timestamp
        self.Results_dic.update({self.second_timer:{}})
        
        #Dic_init for results save
        self.dic_HighTech = {}
        self.dic_Internet = {}
        self.dic_Aero = {}
        self.dic_Auto = {}
        self.dic_Energy = {}
        self.dic_BTP = {}
        self.dic_Industry = {}
        self.dic_Health = {}
        self.dic_Logistic = {}
        self.dic_Telecoms = {}
        self.dic_Matieres = {}
        self.dic_Indices = {}
        self.dic_Devises = {}
        
        #Attributes list for all queries needed to instanciate the Generators for raws treatment parallelization
        attrs_list = [(self.dic_HighTech, self.raw_HighTech, "HighTech"),
                    (self.dic_Internet, self.raw_Internet, "Internet"),
                    (self.dic_Aero, self.raw_Aero, "Aero"),
                    (self.dic_Auto, self.raw_Auto, "Auto"),
                    (self.dic_Energy, self.raw_Energy, "Energy"),
                    (self.dic_BTP, self.raw_BTP, "BTP"),
                    (self.dic_Industry, self.raw_Industry, "Industry"),
                    (self.dic_Health, self.raw_Health, "Health"),
                    (self.dic_Logistic, self.raw_Logistic, "Logistic"),
                    (self.dic_Telecoms, self.raw_Telecoms, "Telecoms"),
                    (self.dic_Matieres, self.raw_Matieres, "Matieres_premieres"),
                    (self.dic_Indices, self.raw_Indices, "Indices"),
                    (self.dic_Devises, self.raw_Devises, "Devises")]
        
        #Raw treatment Parallelization
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.nb_threads) as executor:
            Process_raw_Stocks_Generator = {executor.submit(self.Process_raw_Stocks, attrs[0], attrs[1], attrs[2]) for attrs in attrs_list[:-3]}
            Process_raw_Not_Stocks_Generator = {executor.submit(self.Process_raw_Not_Stocks, attrs[0], attrs[1], attrs[2]) for attrs in attrs_list[-3:-1]}
            Process_raw_Devises_Generator = {executor.submit(self.Process_raw_Stocks, attrs_list[-1][0], attrs_list[-1][1], attrs_list[-1][2])}
        
        self.Results_dic[self.second_timer].update({"OHLC":self.raw_OHLC})
        self.Results_dic[self.second_timer].update({"Last_Trades":self.raw_Last_Trades})
        self.Results_dic[self.second_timer].update({"Orderbook":self.raw_Orderbook})
        
        self.Results_dic[self.second_timer].update({"Clean":self.clean})
        
    def Save_infos(self) : 
        
        if time.time() - self.start_time > self.save_time :
            #Reinitialize the 6 hours period
            self.start_time = time.time()
            
            #Change the name of the file to save to the begining of the new 6 hours period
            self.file_to_save = time.strftime("%a, %d %b %Y %H:%M:%S", gmtime()).replace(" ", "_").replace(",","").replace(":","-") +".json"
        
            #Create the new file and initialize it with an empty dic
            file = open(self.file_to_save, "w")
            file.write("{}")
            file.close()
        
            start = time.time()
            try : 
                file = open(self.file_to_save, "r+")
                json_file = json.load(file)
                json_file.update(self.Results_dic)
                file.seek(0)
                json.dump(json_file, file)
                file.close()
            except : 
                file.close()
            print("Temps de sauvegarde : ", time.time() - start)
            self.Results_dic = {}

    
    def Query_OHLC(self, ticker) :
        
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        #Since argument in query allows to reduce a lot the duplicate data, too high value allows to get only the last OHLC in resp
        resp = requests.get('https://api.kraken.com/0/public/OHLC?pair={0}&since={1}'.format(ticker, int(self.second_timer) - 20), proxy)
        self.raw_OHLC.update({ticker:json.loads(resp.text)})
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
    
    def Query_Orderbook(self, ticker) :
        
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        resp = requests.get('https://api.kraken.com/0/public/Depth?pair={0}'.format(ticker), proxy)
        self.raw_Orderbook.update({ticker:json.loads(resp.text)})
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
    
    def Query_Last_Trades(self, ticker) : 
        
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        resp = requests.get('https://api.kraken.com/0/public/Trades?pair={0}'.format(ticker), proxy)
        self.raw_Last_Trades.update({ticker:json.loads(resp.text)})
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_FGI(self) : 
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_FGI = requests.get('https://alternative.me/crypto/fear-and-greed-index/', proxy)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
        # if resp.status_code == 200 : 
        #     soup = BeautifulSoup(resp.text, "html.parser")
        #     _divs = soup.find_all("div")
        #     for div in _divs : 
        #         try : 
        #             if div["class"][0] == "fng-circle" : 
        #                 self.fear_greed = div.string
        #                 break #On en récupère que le dernier, il y en a 4 en tout en passant par là
        #         except : 
        #             pass
    
    def Query_Yahoo_Indices(self) : 
        
        #Indices mondiaux 
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_Indices = requests.get("https://fr.finance.yahoo.com/indices-mondiaux/", proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_Yahoo_HighTech(self) : 
        
        #High-Tech
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_HighTech = requests.get("https://fr.finance.yahoo.com/industries/High-Tech/",proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_Yahoo_Internet(self) :         
        #Internet
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_Internet = requests.get("https://fr.finance.yahoo.com/industries/Internet/",proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_Yahoo_Matieres(self) :         
        #Matières premières
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_Matieres = requests.get("https://fr.finance.yahoo.com/matierespremieres/",proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_Yahoo_Devises(self) :         
        #Devises
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_Devises = requests.get("https://fr.finance.yahoo.com/devisas/",proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_Yahoo_Aero(self) :         
        #Aero
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_Aero = requests.get("https://fr.finance.yahoo.com/industries/Aeronautique/",proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_Yahoo_Auto(self) :         
        #Auto
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_Auto = requests.get("https://fr.finance.yahoo.com/industries/Automobile/",proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_Yahoo_Energy(self) :         
        #Energy
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_Energy = requests.get("https://fr.finance.yahoo.com/industries/Energie-et-environnement/",proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_Yahoo_BTP(self) :         
        #BTP
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_BTP = requests.get("https://fr.finance.yahoo.com/industries/Immobilier-et-BTP/",proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_Yahoo_Industry(self) :         
        #Industries lourdes
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_Industry = requests.get("https://fr.finance.yahoo.com/industries/Industrie-lourde/",proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_Yahoo_Health(self) :         
        #Health and chemicals
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_Health = requests.get("https://fr.finance.yahoo.com/industries/Sante-et-Chimie/",proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_Yahoo_Logistic(self) :         
        #Services et distribution
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_Logistic = requests.get("https://fr.finance.yahoo.com/industries/Services-et-distribution/",proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
            
    def Query_Yahoo_Telecoms(self) :         
        #Telecoms
        proxy = self.proxies[random.randint(0,len(self.proxies))]
        self.raw_Telecoms = requests.get("https://fr.finance.yahoo.com/industries/Telecoms/",proxy, headers = self.header)
        if time.time() - self.second_timer > self.step_time + 3 :
            self.proxies.pop(self.proxies.index(proxy))
        
    def Process_raw_Stocks(self, dic, raw, tag) : 
        if raw.status_code == 200 :
            soup = BeautifulSoup(raw.text, "html.parser")
            body = soup.find("tbody")
            trs = body.find_all("tr")
            for tr in trs : 
                Symbol = tr.find("a").text
                dic.update({Symbol:{}})
                for td in tr.find_all("td") : 
                    try : 
                        if td["aria-label"] == "Dernier cours" :
                            dic[Symbol].update({"Prix": td.find("fin-streamer")["value"]})
                    except : 
                        try : 
                            if td.find("span").text == "S.O." :
                                dic[Symbol].update({"Prix": "S.O."})
                                dic[Symbol].update({"Volume": "S.O."})
                                dic[Symbol].update({"capitalisation_boursiere": "S.O."})
                                break
                        except : 
                            self.clean = False
                        
                    if td["aria-label"] == "Volume" :
                       dic[Symbol].update({"Volume": td.find("fin-streamer")["value"]})
                        
                    if td["aria-label"] == "capitalisation boursière" :
                        dic[Symbol].update({"capitalisation_boursiere": td.find("fin-streamer")["value"]})
                      
            self.Results_dic[self.second_timer].update({tag:dic})
    
    def Process_raw_Not_Stocks(self, dic, raw, tag) : 
        if raw.status_code == 200 :
            soup = BeautifulSoup(raw.text, "html.parser")
            body = soup.find("tbody")
            trs = body.find_all("tr")
            for tr in trs : 
                Symbol = tr.find("a").text
                dic.update({Symbol:{}})
                for td in tr.find_all("td") : 
                    try : 
                        if td["aria-label"] == "Dernier cours" :
                            dic[Symbol].update({"Prix": td.find("fin-streamer")["value"]})
                    except : 
                        try : 
                            if td.find("span").text == "S.O." :
                                dic[Symbol].update({"Prix": "S.O."})
                                dic[Symbol].update({"Volume": "S.O."})
                                break
                        except : 
                            self.clean = False
                        
                    if td["aria-label"] == "Volume" :
                       dic[Symbol].update({"Volume": td.find("fin-streamer")["value"]})
                
            self.Results_dic[self.second_timer].update({tag:dic})
    
    def Process_raw_Devises(self, dic, raw, tag) : 
        if raw.status_code == 200 :
            soup = BeautifulSoup(raw.text, "html.parser")
            body = soup.find("tbody")
            trs = body.find_all("tr")
            for tr in trs : 
                Symbol = tr.find("a").text
                dic.update({Symbol:{}})
                for td in tr.find_all("td") : 
                    try : 
                        if td["aria-label"] == "Dernier cours" :
                            dic[Symbol].update({"Prix": td.find("fin-streamer")["value"]})
                    except : 
                        try : 
                            if td.find("span").text == "S.O." :
                                dic[Symbol].update({"Prix": "S.O."})
                                break
                        except : 
                            self.clean = False    
                
            self.Results_dic[self.second_timer].update({tag:dic})
    
    
def Import_proxies() :
    text = open("ips-finance_scraper.txt", "r")
    
    proxies = []
    for proxy in text : 
        splited_proxy = proxy.split(":")
        _id = splited_proxy[2]
        password = "j93nltoihafl"
        host_name = splited_proxy[0]
        host_port = splited_proxy[1]
        _http = "http://" + _id + ":" + password + "@" + host_name + ":" + host_port
        _https = "https://" + _id + ":" + password + "@" + host_name + ":" + host_port
        proxy = {"http": _http, "https": _https}
        proxies.append(proxy)
    return proxies

#Those for whom we query OHLC, Last trades and Orderbook
alpha_tickers = ["BTCUSD", "ETHUSD", "USDTUSD"]

#Those for whom we query only OHLC
beta_tickers = ["XRPUSD", "ADAUSD", "MATICUSD", "DOTUSD", "LTCUSD", "SHIBUSD", "TRXUSD", "LUNAUSD"]

#os.chdir("/home/luis/Bureau/Scraper")
proxies = Import_proxies()
S = Requester(alpha_tickers, beta_tickers,
              proxies, 
              nb_threads = 16,
              step_time = 10,
              save_time = 3600)
