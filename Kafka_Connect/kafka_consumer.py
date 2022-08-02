from kafka import KafkaConsumer
import mysql.connector
import json
import requests
import configparser
import threading


def read_config():
  config = configparser.ConfigParser()
  config.read('config/configurations.ini', encoding='utf-8')
  return config

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

if "ClickStream_Input" not in tables:
    table_cursor.execute("CREATE TABLE `O360_PDP`.`ClickStream_Input` (`ID` INT NOT NULL AUTO_INCREMENT,`topic` VARCHAR(200) NULL,`respId` VARCHAR(150) NULL,`surveyId` VARCHAR(50) NULL,`questionId` VARCHAR(50) NULL,`token` VARCHAR(250) NULL,`href` VARCHAR(2000) NULL,`htmlLink` VARCHAR(2000) NULL,`videoLink` VARCHAR(2000) NULL,PRIMARY KEY (`ID`));")
    print("ClickStream_Input table created")
    mydb.commit()
    table_cursor.close()

consumer = KafkaConsumer(bootstrap_servers=eval(config['KafkaSettings']['bootstrap_servers']))
topic_list = list(consumer.topics())
listening_topics = []
for topic in topic_list:
  if topic.startswith(config['KafkaSettings']['Listen_topic_pattern']):
    listening_topics.append(topic)
consumer.subscribe(listening_topics)

class BackgroundTasks(threading.Thread):
    def run(self):
        while True:
          topic_list = list(consumer.topics())
          listening_topics = []
          for topic in topic_list:
            if topic.startswith(config['KafkaSettings']['Listen_topic_pattern']):
              listening_topics.append(topic)
          consumer.subscribe(listening_topics)

t = BackgroundTasks()
t.start()

for msg in consumer:
  try:
    config = read_config()
    msg_json = json.loads(msg.value.decode('utf-8'))
    topic = msg.topic
    mydb = mysql.connector.connect(host=config['MySQLSettings']['host'], user="root", passwd=config['MySQLSettings']['password'], port=int(config['MySQLSettings']['port']), database=config['MySQLSettings']['database'])
    cursor = mydb.cursor()
    query = "INSERT into Clickstream_Input(topic,respId,surveyId,questionId,token,href,htmlLink,videoLink) values(%s,%s,%s,%s,%s,%s,%s,%s)"
    record = [( str(topic),str(msg_json['respId']), str(msg_json['surveyId']), str(msg_json['questionId']), str(msg_json['token']), str(msg_json['href']), str(msg_json['htmlLink']), str(msg_json['videoLink']))]
    cursor.executemany(query, record)
    mydb.commit()
    cursor.close()
    subdict = {'topic':str(topic), 'respId':str(msg_json['respId']), 'surveyId':str(msg_json['surveyId']), 'questionId':str(msg_json['questionId']), 'token':str(msg_json['token']), 'href':str(msg_json['href']), 'htmlLink':str(msg_json['htmlLink']), 'videoLink':str(msg_json['videoLink'])}
    print(config['APIServer']['pdp_endpoint'])
    response_api = requests.post(config['APIServer']['pdp_endpoint'], data=json.dumps(subdict)).json()
    print(response_api)
  except Exception as e:
    print(e)
    continue