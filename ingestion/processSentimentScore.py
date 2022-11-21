import nltk
from newAPIExtract import NewsAPIExtract
import pandas as pd 
import os
import shutil
import sconfig
import newAPIExtract
from nltk.sentiment.vader import SentimentIntensityAnalyzer

class ProcessSentimentScore():
    __newsAPIExtract = None 
    __secretAPI = None 
    __newsAPISentiColsDF = None 
    __kaggleNewsSentiColsDF = None 
    __kaggleNewsRenameColumns = {
        'ticker' : 'symbol',
        'title' : 'title',
        'content' : 'description',
        'release_date' : 'publishedAt',
        'title_neg' :'title_neg', 
        'title_neu' : 'title_neu',
        'title_pos' : 'title_pos',
        'title_compound' : 'title_compound', 
        'content_neg' : 'description_neg', 
        'content_neu' : 'description_neu', 
        'content_pos' : 'description_pos',
        'content_compound': 'description_compound'
    }
    __newsAPISentimentAnalysisCols = ['symbol','title', 'description','publishedAt','title_neg', 'title_neu', 'title_pos', 'title_compound','description_neg', 'description_neu', 'description_pos','description_compound']
    __kaggleSentimentAnalysisCols = ['ticker', 'title', 'content', 'release_date','title_neg', 'title_neu', 'title_pos','title_compound', 'content_neg', 'content_neu', 'content_pos','content_compound']
    def __init__(self) -> None:
        self.__secretAPI =  sconfig.secretsConfig()

    @property
    def newsApiExtractObject(self) -> newAPIExtract:
        if (self.__newsAPIExtract is None):
            self.__newsAPIExtract = NewsAPIExtract()
        return self.__newsAPIExtract 
    
    @property
    def kaggleNewsSentiColsDF (self) -> pd.DataFrame:
        if(self.__kaggleNewsSentiColsDF is None):
            path = f"{self.__secretAPI.DataLocations.sentimentscoredata}kaggle\\" 
            kaggledf =pd.read_parquet(path)
            self.__kaggleNewsSentiColsDF = kaggledf[self.__kaggleSentimentAnalysisCols]
            self.__kaggleNewsSentiColsDF = self.__kaggleNewsSentiColsDF.rename(columns = self.__kaggleNewsRenameColumns, errors = "raise")
            self.__kaggleNewsSentiColsDF['news_source'] = 'kaggle'
        return self.__kaggleNewsSentiColsDF
    
    @property
    def newsAPISentiColsDF (self) -> pd.DataFrame:
        if(self.__newsAPISentiColsDF is None):
            path = f"{self.__secretAPI.DataLocations.sentimentscoredata}newsAPI\\" 
            newsAPIDF =pd.read_parquet(path)
            self.__newsAPISentiColsDF = newsAPIDF[self.__newsAPISentimentAnalysisCols]
            self.__newsAPISentiColsDF['news_source'] = 'NewsAPI'
        return self.__newsAPISentiColsDF

    def getSentimentPolarity(self, text):
        SIA = SentimentIntensityAnalyzer()
        return SIA.polarity_scores(text)
    
    def applySentimentScoreToKaggleDF(self, df:pd.DataFrame) -> pd.DataFrame:
        try:
            #print('Processing Sentient for Title')
            sentimentScores = self.getSentimentPolarity(df['title'])
            for s in sentimentScores:
                df[f"title_{s}"] = sentimentScores[s]
            
            #print('Processing Sentiment for Content')
            sentimentScoreContent = self.getSentimentPolarity(df['content'][:200] if len(df['content']) >=200 else df['content'][:len(df['content'])])
            for s in sentimentScoreContent:
                df[f"content_{s}"] = sentimentScoreContent[s]
            
            return df
        except Exception as e:
            raise Exception (f'Error occurred while applying sentiment scores to df. Error details : {str(e)}')

    def processKaggleSentimentScore(self):
        try:

            kaggleDF = self.newsApiExtractObject.KaggleDJIANewsDF
            ## apply sentiment score 

            ## save the data 
            ##create folder for the file if not exists 
            path = f"{self.__secretAPI.DataLocations.sentimentscoredata}kaggle\\"    
            os.makedirs(os.path.dirname(path),exist_ok=True)
            ## since the process is taking longer for the running all the companies to gather , splitting the files based on the individual djia companies 
            for x in self.newsApiExtractObject.DJIAcompanies:
                print(f'Processing Symbol : {x["symbol"]}')
                filterDF = kaggleDF.where(
                    kaggleDF["ticker"] == x["symbol"]
                ).dropna()

                filterDF = filterDF.apply(
                            self.applySentimentScoreToKaggleDF, 
                            axis=1 
                        )
                
                ## Saving the data 
                print(f'Saving the file with the location :: {path} + {x["symbol"]}.parquet')
                filterDF.to_parquet(path + x["symbol"], engine='fastparquet')

        except Exception as e:
            raise Exception(f"Error occurred while processing Kaggle sentiment Score. Error Details : {str(e)}")
    
    def applySentimentScoreToNewsAPIDF(self, df:pd.DataFrame) -> pd.DataFrame:
        try:
            #print('Processing Sentient for Title')
            sentimentScores = self.getSentimentPolarity(df['title'])
            for s in sentimentScores:
                df[f"title_{s}"] = sentimentScores[s]
            
            #print('Processing Sentiment for Content')
            sentimentScoreContent = self.getSentimentPolarity(df['description'][:200] if len(df['description']) >=200 else df['description'][:len(df['description'])])
            for s in sentimentScoreContent:
                df[f"description_{s}"] = sentimentScoreContent[s]
            
            return df
        except Exception as e:
            raise Exception (f'Error occurred while applying sentiment scores to df. Error details : {str(e)}')

    def processNewsAPISentimentScore(self):
        try:

            newsAPIDF = self.newsApiExtractObject.DJIACompaniesNewsAPIDf
            ## apply sentiment score 

            ## save the data 
            ##create folder for the file if not exists 
            path = f"{self.__secretAPI.DataLocations.sentimentscoredata}newsAPI\\"    
            os.makedirs(os.path.dirname(path),exist_ok=True)
            ## since the process is taking longer for the running all the companies to gather , splitting the files based on the individual djia companies 
            for x in self.newsApiExtractObject.DJIAcompanies:
                print(f'Processing Symbol : {x["symbol"]}')
                filterDF = newsAPIDF.where(
                    newsAPIDF["symbol"] == x["symbol"]
                ).dropna()

                filterDF = filterDF.apply(
                            self.applySentimentScoreToNewsAPIDF, 
                            axis=1 
                        )
                
                ## Saving the data 
                parquetFilePath = path + x["symbol"] + "\\"
                print(f'Saving the file with the location :: {parquetFilePath}{x["symbol"]}.parquet')
                shutil.rmtree(path = parquetFilePath)
                os.makedirs(os.path.dirname(parquetFilePath),exist_ok=True)

                filterDF.to_parquet(parquetFilePath + f"{x['symbol']}.parquet", engine='fastparquet')

        except Exception as e:
            raise Exception(f"Error occurred while processing Kaggle sentiment Score. Error Details : {str(e)}")
    

