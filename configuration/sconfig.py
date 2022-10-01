import yaml
import os 
from types import SimpleNamespace

class secretsConfig:
    __ymlfile = None
    __secrets ={}
    __tokens = None 
    __dataLocations = None 

    def __init__(self) -> None:
        self.__secrets = None 
        self.__ymlfile = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "\configuration\yaml\config.yml" #"..\configuration\yaml\secrets.yml"
        self.__tokens = None 
        self.__dataLocations = None
        

    @property
    def secretsValue(self):
        if (self.__secrets == None):
            print(f"loading yaml file {self.__ymlfile}")
            with open(self.__ymlfile) as f:
                self.__secrets = yaml.safe_load(f)
        return self.__secrets

    @property
    def Files(self):
        files = [f for f in os.listdir('.') if os.path.isfile(f)]
        for f in files:
            print(f)
        return files

    @property
    def Tokens(self):
        if (self.__tokens == None):
            self.__tokens = SimpleNamespace(**self.secretsValue['tokens'])
        return self.__tokens
    
    @property
    def DataLocations(self):
        if (self.__dataLocations == None):
            self.__dataLocations = SimpleNamespace(**self.secretsValue['datapath'])
        return self.__dataLocations

    
