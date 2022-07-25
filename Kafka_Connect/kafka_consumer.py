from kafka import KafkaConsumer
import mysql.connector
import json
import requests
import configparser

def read_config():
  config = configparser.ConfigParser()
  config.read('configurations.ini', encoding='utf-8')
  return config

config = read_config()

db = mysql.connector.connect(host=config['MySQLSettings']['host'], user=config['MySQLSettings']['user'], passwd=config['MySQLSettings']['password'], port=int(config['MySQLSettings']['port']))
database_cursor = db.cursor()
database_cursor.execute("show databases")
databases = []
for i in database_cursor:
  databases.append(i[0])


if "O360_PDP" not in databases:
  start_cursor = db.cursor()
  start_cursor.execute("CREATE DATABASE O360_PDP;")
  print("created O360_PDP database")
  start_cursor.execute("CREATE TABLE `O360_PDP`.`Clickstream_Input`(`ID` INT NOT NULL AUTO_INCREMENT,`UUID` VARCHAR(100) NOT NULL,`Resp_id` VARCHAR(100) NOT NULL,`URL` VARCHAR(2000) NOT NULL,`Html_Link` VARCHAR(2000) NULL,PRIMARY KEY (`ID`));")
  print("created Clickstream_Input table")
  start_cursor.execute("CREATE TABLE `O360_PDP`.`Clickstream_PDP_Flag`(`ID` INT NOT NULL AUTO_INCREMENT,`UUID` VARCHAR(100) NOT NULL,`Resp_id` VARCHAR(100) NOT NULL,`URL` VARCHAR(2000) NOT NULL,`Html_Link` VARCHAR(2000) NULL,`PDP_Flag` VARCHAR(50) NULL,PRIMARY KEY (`ID`));")
  print("created Clickstream_PDP_Flag table")
  db.commit()
  start_cursor.close()

consumer = KafkaConsumer(bootstrap_servers=eval(config['KafkaSettings']['bootstrap_servers']))
consumer.subscribe(eval(config['KafkaSettings']['Listen_topic']))

print("Listening to topic: " + config['KafkaSettings']['Listen_topic'])

for msg in consumer:
  try:
    config = read_config()
    consumer.subscribe(eval(config['KafkaSettings']['Listen_topic']))
    msg_json = json.loads(msg.value.decode('utf-8'))
    mydb = mysql.connector.connect(host=config['MySQLSettings']['host'], user="root", passwd=config['MySQLSettings']['password'], port=int(config['MySQLSettings']['port']), database=config['MySQLSettings']['database'])
    cursor = mydb.cursor()
    query = "INSERT into Clickstream_Input(UUID,Resp_id,URL,Html_Link) values(%s,%s,%s,%s)"
    record = [( msg_json['UUID'],msg_json['Resp Id'], msg_json['URL'], msg_json['htmlLink'])]
    cursor.executemany(query, record)
    mydb.commit()
    cursor.close()
    subdict = {'uuid':msg_json['UUID'], 'resp_id':msg_json['Resp Id'], 'url':msg_json['URL'], 'htmlLink':msg_json['htmlLink']}
    response_api = requests.post('http://127.0.0.1:8000/pdp/', data=json.dumps(subdict)).json()
    print(response_api)
  except Exception as e:
    print(e)
    continue