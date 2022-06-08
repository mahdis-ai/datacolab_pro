from concurrent.futures import thread
import requests
import json
import psycopg2
from threading import Thread
from dotenv import dotenv_values
config = dotenv_values(".env")
rc_list=[]
cn_list=[]
#connect to database
# put the actual password on password field
conn = psycopg2.connect(
   database=config["DATABASE"], user=config["DATABASE_USER"], password=config["DATABASE_PASSWORD"], host=config["DATABASE_HOST"], port= config["DATABASE_PORT"]
)


cursor = conn.cursor()
query="""
    CREATE TABLE IF NOT EXISTS company
    (
        companyname character varying(255)  NOT NULL,
        riccode character varying(255),
        CONSTRAINT company_pkey PRIMARY KEY (companyname)
    );
    CREATE TABLE IF NOT EXISTS esgscores
    (
    companyname character varying(255),
    enviroment integer,
    social integer,
    governance integer,
    ranking integer,
    totalindustries integer,
    tscore integer,
    CONSTRAINT esgscores_companyname_fkey FOREIGN KEY (companyname)
        REFERENCES company(companyname)
    );
    """
cursor.execute(query)
#iterate through companies and append them to  a list
with open('esgsearchsuggestions.json','r') as f:
    data = json.load(f)
for d in data:
    rc_list.append(d['ricCode'])
    cn_list.append(d['companyName'])
    
#crawl func which scrape data and store them in database
def crawl(rc_list,cn_list,head,tail):
    for i in range(head,tail) : 
        rcode=rc_list[i]
        #sending request to the website 
        r=requests.get('https://www.refinitiv.com/bin/esg/esgsearchresult?ricCode='+str(rcode))
        jres=r.json()
        #specifying fields of data  that we need to store
        rank=jres['industryComparison']['rank']
        tindustries=jres['industryComparison']['totalIndustries']
        enviroment=jres['esgScore']['TR.EnvironmentPillar']['score']
        social=jres['esgScore']['TR.SocialPillar']['score']
        governance=jres['esgScore']['TR.GovernancePillar']['score']
        total=jres['esgScore']['TR.TRESG']['score']
        #insert values to company table in db
        comp_insert_query="""INSERT INTO company(companyname,riccode) VALUES (%s , %s)"""
        comp_record_to_insert=(cn_list[i],rc_list[i])
        cursor.execute(comp_insert_query,comp_record_to_insert)
        conn.commit()
        #insert values to esgscores table in db
        esg_insert_query="""INSERT INTO esgscores(companyname,enviroment,social,governance,ranking,totalindustries,tscore) VALUES (%s , %s, %s, %s, %s, %s, %s)"""
        esg_record_to_insert=(cn_list[i],enviroment,social,governance,rank,tindustries,total)
        cursor.execute(esg_insert_query,esg_record_to_insert)
        conn.commit()
#implementing multithread for crawling data to make this process more efficient 
t1=Thread(target=crawl,args=(rc_list,cn_list,0,20))
t2=Thread(target=crawl,args=(rc_list,cn_list,20,40))
t3=Thread(target=crawl,args=(rc_list,cn_list,40,60))
t4=Thread(target=crawl,args=(rc_list,cn_list,60,80))
t5=Thread(target=crawl,args=(rc_list,cn_list,80,100))
#starting threads
t1.start()
t2.start()
t3.start()
t4.start()
t5.start()


    