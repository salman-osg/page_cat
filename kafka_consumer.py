from kafka import KafkaConsumer
import mysql.connector
import json

db = mysql.connector.connect(host="Analytics_pod_db", user="root", passwd="Analytics_POD", port=3306)
database_cursor = db.cursor()
database_cursor.execute("show databases")
databases = []
for i in database_cursor:
    databases.append(i[0])


if "O360_PDP" not in databases:
  start_cursor = db.cursor()
  start_cursor.execute("CREATE DATABASE O360_PDP;")
  start_cursor.execute("CREATE TABLE `O360_PDP`.`clickstreem` (`ID` INT NOT NULL AUTO_INCREMENT,`UUID` VARCHAR(100) NOT NULL,`Resp_id` VARCHAR(100) NOT NULL,`URL` VARCHAR(2000) NOT NULL,`Html_Link` VARCHAR(2000) NULL,PRIMARY KEY (`ID`));")
  db.commit()

bootstrap_servers = ['54.175.181.170:9092']
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
consumer.subscribe('test_python_5')
mydb = mysql.connector.connect(host="Analytics_pod_db", user="root", passwd="Analytics_POD", port=3306, database="O360_PDP")

for msg in consumer:
    message = msg.value.decode('utf-8')
    msg_json = json.loads(message)
    cursor = mydb.cursor()
    query = "INSERT into clickstreem(UUID,Resp_id,URL,Html_Link) values(%s,%s,%s,%s)"
    record = [( msg_json['UUID'],msg_json['Resp Id'], msg_json['URL'], msg_json['htmlLink'])]
    cursor.executemany(query, record)
    mydb.commit()