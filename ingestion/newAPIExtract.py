import symbol
from newsapi import NewsApiClient
import sconfig
import json
import pandas as pd 
import datetime
import os

class NewsAPIExtract():
    __newsAPIClient = None 
    __secretAPI = None
    __jsonDJIACompanies = None

    def __init__(self) -> None:
        self.__secretAPI =  sconfig.secretsConfig()
        self.__newsAPIClient = NewsApiClient(api_key=self.__secretAPI.Tokens.newspi)

    @property 
    def DJIAcompanies(self) -> list:
        if (self.__jsonDJIACompanies == None):
            with open(self.__secretAPI.DataLocations.djiacompanies, 'r') as f:
                self.__jsonDJIACompanies = json.load(f)
        return self.__jsonDJIACompanies
    
    def extractDJIACompaniesNews(self, startDate =(datetime.datetime.now() - datetime.timedelta(29)).strftime("%Y-%m-%d"),
                                        endDate = datetime.datetime.now().strftime("%Y-%m-%d")
                                ) -> None:
        try:
            
            ## Loop through all the companies and extract the news articles 
            for djiaCompany in self.DJIAcompanies:
                ## Setting up the path for storing the parquet files 
                path = f"{self.__secretAPI.DataLocations.djianewsapi}{djiaCompany['symbol']}\{djiaCompany['symbol']}_{datetime.datetime.now().strftime('%Y%m%d')}.parquet"   

                ##create folder for the file if not exists 
                os.makedirs(os.path.dirname(path),exist_ok=True)

                self.extractCompanyNews(
                    companyName=djiaCompany['name']
                    ,companySymbol=djiaCompany['symbol']
                    ,startDate=startDate
                    ,endDate=endDate
                    ,path=path
                )

        except Exception as e:
            raise Exception (f'Error occurred while extracting DJIA Companies News . Error Details :: {str(e)}')
    
    def extractCompanyNews(self, companyName,companySymbol, startDate, endDate , path ) -> None:
        try:
            ## get the new using newsAPI client 
            print(f'Extracting Data for Company :: {companyName}. startDate :: {startDate}. endDate :: {endDate}. path :: {path}')
            
            all_news = self.__newsAPIClient.get_everything(q=companyName,
                                                            language='en',
                                                            from_param=startDate,
                                                            to=endDate
                                                          )
            
            ## convert the news into pandas object and flatten it out 
            newAPIDF = pd.json_normalize(all_news,["articles"])
            newAPIDF['companyname'] = companyName
            newAPIDF['symbol'] = companySymbol
            ## store the data in pandas 
            newAPIDF.to_parquet(path, engine='fastparquet')

        except Exception as e:
            raise Exception(f'Error occurred while extracting company news using NewAPI. Error Details :: {str(e)}')



