import os
import csv
import sqlite3
import requests
import json

import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

engine = sqlalchemy.create_engine("sqlite:///apiML.db")
base = declarative_base()

from config import config

script_path = os.path.dirname(os.path.realpath(__file__))

config_path_name = os.path.join(script_path, 'config.ini')
dataset = config('dataset', config_path_name)



class ML(base):
    __tablename__ = "ml"
    id= Column(String,primary_key=True)
    site_id= Column(String)
    titulo= Column(String)
    price = Column(Integer)
    currency_id = Column(Integer)
    initial_quantity = Column(Integer)
    sold_quantity = Column(Integer)


    def __repr__(self):
        return f"ml{self.id},site{self.site_id},titulo{self.titulo},precio{self.price},concurrencia{self.currency_id},inicial{self.initial_quantity},sol{self.sold_quantity}"




    




   



def create_schema():

    base.metadata.drop_all(engine)

    base.metadata.create_all(engine)


reponse = requests.get('https://api.mercadolibre.com/items?ids=MLA845041373')
data = reponse.json()
print(json.dumps(data, indent=4))
id = data[0]["body"]["id"] 
site_id = data[0]["body"]["site_id"]
titulo = data[0]["body"]["title"]
price = data[0]["body"]["price"]
currency_id = data[0]["body"]["currency_id"]
initial_quantity= data[0]["body"]["initial_quantity"]
available_quantity = data[0]["body"]["available_quantity"]
sold_quantity = data[0]["body"]["sold_quantity"]
print(id,site_id,titulo,price,currency_id,initial_quantity,available_quantity,sold_quantity)




def fill():

    with open(dataset['ml']) as fi:
        data = list(csv.DictReader(fi))
        
        
    for item in data:
        join_id =f'{item["site"]}{item["id"]}'
        join_id.split(",")[0]
        print(join_id)
    if join_id != id:
        print(join_id)
    

    
        

def insert_ml(id,site_id,titulo,price,currency_id,initial_quantity,sold_quantity):
    
    Session = sessionmaker(bind=engine)
    session = Session()

    registro = ML (
        id=id,
        site_id=site_id,
        titulo=titulo,
        price=price,
        currency_id=currency_id,
        initial_quantity=initial_quantity,
        sold_quantity=sold_quantity )
    
    

    
    

    session.add(registro)
    session.commit()
    print(registro)




def fetch(id):

    Session = sessionmaker(bind=engine)
    session = Session()


    if id is None:
        print ("no existe el id")


    query = session.query(ML).filter(ML.id=='MLA725997501')
    for id in query:
        print(id)
    
    

    






    



        

    


    


     

    


    

    
    



    

    





    




    

if __name__ == '__main__':
    create_schema()
    fill()
    insert_ml(id,site_id,titulo,price,currency_id,initial_quantity,sold_quantity)
    fetch(id)
 
    
  
    

    
    

     

   

        
    










    
    
    
    
    

    
    
    
    
    



