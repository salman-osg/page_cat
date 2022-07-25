from kafka import KafkaProducer
from fastapi import FastAPI, HTTPException
import mysql.connector
from pydantic import BaseModel
from pdp_model import *
import json
import configparser


class Input_Data(BaseModel):
    uuid: str
    resp_id: str
    url: str
    htmlLink: str

app = FastAPI()

def read_config():
    config = configparser.ConfigParser()
    config.read('configurations.ini', encoding='utf-8')
    return config


@app.post("/pdp/")
async def pdp(input_data:Input_Data):
    try:
        config = read_config()
        uuid = input_data.uuid
        resp_id = input_data.resp_id
        url = input_data.url
        htmlLink = input_data.htmlLink
        htmlPath = htmlLink[htmlLink.find(resp_id):]

        # Call the PDP Model
        pdp_tag = get_pdp(url, htmlPath)

        # Sending data to Seisens though Kafka
        output_dict = {'UUID':uuid, 'Resp Id':resp_id, 'URL':url, 'HTML_Link':htmlLink, 'PDP_Tag':pdp_tag}
        producer = KafkaProducer(bootstrap_servers=eval(config['KafkaSettings']['bootstrap_servers']),
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))
        producer.send(config['KafkaSettings']['Produce_topic'], output_dict)

        # Sending Data to MySQL Database
        db = mysql.connector.connect(host=config['MySQLSettings']['host'], user="root", passwd=config['MySQLSettings']['password'], port=int(config['MySQLSettings']['port']), database=config['MySQLSettings']['database'])
        cursor = db.cursor()
        query = "INSERT into Clickstream_PDP_Flag(UUID,Resp_id,URL,Html_Link,PDP_Flag) values(%s,%s,%s,%s,%s)"
        record = [(uuid, resp_id, url, htmlLink, pdp_tag)]
        cursor.executemany(query, record)
        db.commit()
        return output_dict
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    