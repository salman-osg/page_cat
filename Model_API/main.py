from kafka import KafkaProducer
from fastapi import FastAPI, HTTPException
import mysql.connector
from pydantic import BaseModel
from pdp_model import *
import json
import configparser


class Input_Data(BaseModel):
    topic: str
    respId: str
    surveyId: str
    questionId: str
    token: str
    href: str
    htmlLink: str
    videoLink: str

config = read_config()

print(config['MySQLSettings']['host'])

db = mysql.connector.connect(host=config['MySQLSettings']['host'], user="root", passwd=config['MySQLSettings']['password'], port=int(config['MySQLSettings']['port']))
database_cursor = db.cursor()
database_cursor.execute("show databases")
databases = []
for i in database_cursor:
    databases.append(i[0])


if "O360_PDP" not in databases:
    database_cursor.execute("CREATE DATABASE O360_PDP;")
    db.commit()
    database_cursor.close()

mydb = mysql.connector.connect(host=config['MySQLSettings']['host'], user="root", passwd=config['MySQLSettings']['password'], port=int(config['MySQLSettings']['port']), database=config['MySQLSettings']['database'])
table_cursor = mydb.cursor()
table_cursor.execute("show tables")
tables = []
for i in table_cursor:
    tables.append(i[0])

if "ClickStream_Output" not in tables:
    table_cursor.execute("CREATE TABLE `O360_PDP`.`ClickStream_Output` (`ID` INT NOT NULL AUTO_INCREMENT,`topic` VARCHAR(200) NULL,`respId` VARCHAR(150) NULL,`surveyId` VARCHAR(50) NULL,`questionId` VARCHAR(50) NULL,`token` VARCHAR(250) NULL,`href` VARCHAR(2000) NULL,`htmlLink` VARCHAR(2000) NULL,`videoLink` VARCHAR(2000) NULL,`page_category` VARCHAR(100) NULL,PRIMARY KEY (`ID`));")
    print("ClickStream_Output table created")
    mydb.commit()
    table_cursor.close()

app = FastAPI()

def read_config():
    config = configparser.ConfigParser()
    config.read('config/configurations.ini', encoding='utf-8')
    return config


@app.post("/pdp/")
async def pdp(input_data:Input_Data):
    config = read_config()
    topic= input_data.topic
    respId= input_data.respId
    surveyId= input_data.surveyId
    questionId= input_data.questionId
    token= input_data.token
    href= input_data.href
    htmlLink= input_data.htmlLink
    videoLink= input_data.videoLink
    htmlPath = htmlLink[htmlLink.find("/downloads/")+11:]

    # Call the PDP Model
    page_tag = get_pdp(href, htmlPath)

    # Sending data to Seisens though Kafka
    output_dict = {'topic':topic, 'respId':respId, 'surveyId':surveyId, 'questionId':questionId, 'token':token, 'href':href, 'htmlLink':htmlLink, 'videoLink':videoLink,'PageCategory':page_tag}
    producer = KafkaProducer(bootstrap_servers=eval(config['KafkaSettings']['bootstrap_servers']),
                        value_serializer=lambda x: 
                        json.dumps(x).encode('utf-8'))
    producer.send(config['KafkaSettings']['Produce_topic'], output_dict)

    # Sending Data to MySQL Database
    db = mysql.connector.connect(host=config['MySQLSettings']['host'], user="root", passwd=config['MySQLSettings']['password'], port=int(config['MySQLSettings']['port']), database=config['MySQLSettings']['database'])
    cursor = db.cursor()
    query = "INSERT into ClickStream_Output(topic,respId,surveyId,questionId,token,href,htmlLink,videoLink,page_category) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    record = [(topic,respId, surveyId, questionId, token, href, htmlLink, videoLink,page_tag)]
    cursor.executemany(query, record)
    db.commit()
    return output_dict
    