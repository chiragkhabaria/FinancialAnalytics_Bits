import yfinance as yf
import sconfig
import json
import os.path
import os
import pandas as pd 

class DJIAStockPriceExtractor():

    __secretAPI = None
    __jsonDJIACompanies = None
    __djiaCompaniesStockPriceDF = None 
    __djiaCompaniesSummarizedNewsDF = None

    def __init__(self) -> None:
        self.__secretAPI =  sconfig.secretsConfig()
    
    @property 
    def DJIAcompanies(self) -> list:
        if (self.__jsonDJIACompanies == None):
            with open(self.__secretAPI.DataLocations.djiacompanies, 'r') as f:
                self.__jsonDJIACompanies = json.load(f)
        return self.__jsonDJIACompanies

    @property 
    def DJIACompaniesStockPriceDF(self) -> None:
        if (self.__djiaCompaniesStockPriceDF is None):
            self.__djiaCompaniesStockPriceDF = pd.read_parquet(self.__secretAPI.DataLocations.djiatickerdata)
        return self.__djiaCompaniesStockPriceDF    
    

    def extractDJIAStockPriceMax(self) -> None:
        try:
            ## 1 get all the djia companies 
            for djiaCompany in self.DJIAcompanies:
                print(f"Processig company {djiaCompany['symbol']}")
                histDataFileName = f"{self.__secretAPI.DataLocations.djiatickerdata}{djiaCompany['symbol']}\{djiaCompany['symbol']}_hist.parquet" 
                ## if file already exists then drop the file 
                if (os.path.isfile(histDataFileName)):
                    print(f"Removing {djiaCompany['symbol']} history file {histDataFileName}")
                    os.remove(histDataFileName)
                
                ##create the folder 
                os.makedirs(os.path.dirname(histDataFileName),exist_ok=True)

                ## fetch the data from yahoo finance 
                ticker = yf.Ticker(djiaCompany['symbol'])
                tickerHistPriceDF = ticker.history('max')
                ## add the return column 
                tickerHistPriceDF['companyname'] = djiaCompany['name']
                tickerHistPriceDF['symbol'] = djiaCompany['symbol']
                ## Store ticker data in the file 
                tickerHistPriceDF.to_parquet(histDataFileName, engine='fastparquet')


        except Exception as e:
            raise Exception(f'Error occurred while extracting DJIA Max Storkc Price. Error Details :: {str(e)}')

