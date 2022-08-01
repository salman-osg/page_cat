from bs4 import BeautifulSoup
import boto3
import configparser

def read_config():
    config = configparser.ConfigParser()
    config.read('config/configurations.ini', encoding='utf-8')
    return config

def get_pdp(URL,html_path):
    try:
        config = read_config()
        if len(URL) < 35:
            class_pdp = "skipped"
        else:
            search_page= eval(config['search_pages']['searchPages_keys'])
            for key in search_page:
                if URL.count(key,0,35) > 0:
                    class_pdp = "search_page"
                    break
                else:
                    url_keys = eval(config['PDP_Yes']['url_keys'])
                    for key in url_keys:
                        matching = all(x in URL for x in key)
                        if matching:
                            class_pdp = "YES"
                            break
                    else:
                        other_keys = eval(config['Other_pages']['otherPages_keys'])
                        for key in other_keys:
                            matching = all(x in URL for x in key)
                            if matching:
                                class_pdp = "Other_pages"
                                break
                            else:
                                client = boto3.client(
                                    service_name='s3',
                                    region_name='us-east-1',
                                    aws_access_key_id=config['S3Settings']['aws_access_key_id'],
                                    aws_secret_access_key=config['S3Settings']['aws_secret_access_key'])
                                html_obj = client.get_object(Bucket=config['S3Settings']['bucket'], Key=html_path)
                                body = html_obj['Body']
                                html_content = body.read()
                                soup = BeautifulSoup(html_content, 'html.parser')
                                text = soup.find_all(text=True)
                                if text == None:
                                    class_pdp = "No Data"
                                else:
                                    new = []
                                    for i in text:
                                        new.append(i.lower())
                                    keys = eval(config['PDP_Keywords']['keywords'])
                                    pdp = [i for i in new if i in keys]
                                    if len(pdp) > 0 and len(pdp)< 10:
                                        class_pdp = "YES"
                                        break
                                    else:
                                        class_pdp = "NO"
                                        break
        return class_pdp
    except Exception as e:
        print(e)
        return "error"
