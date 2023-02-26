from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import json
import pyodbc
import uvicorn
import mysql.connector #Banco de dados

#Função para pegar os paramêtros de configuração do banco de dados
def get_config():
    config = json.loads(open('config.json').read())
    global server
    server = config['server']
    global database
    database = config['database']
    global username
    username = config['username']
    global password
    password = config['password']

#Função que faz a conexão com o banco de dados
def bd_connect():
    get_config()
    global connection
    global cursor
    try:
        connection = mysql.connector.connect(host=server, database=database, user=username, password=password)
        cursor = connection.cursor()
        print("Conexão realizada com sucesso.")
    except pyodbc.Error as ex:
        print(ex)

class Message(BaseModel):
    content: str

app = FastAPI()
@app.get("/API/V1/loaddata")
async def load_data(request: Request):
    bd_connect()
    dados = str(request.scope['query_string'].decode("utf-8")).split('&')
    if len(dados) == 9: #Conexao com OBD
        email = dados[0].replace('eml=','')
        id = dados[3].replace('id=','')
        longitude = dados[6].replace('lon=','')
        latitude = dados[5].replace('lat=','')

        sql = f"""insert into dados values('{email}',curtime(),'{id}','{longitude}','{latitude}','{0}','{0}','{0}',
                                            '{0}','{0}','{0}','{0}','{0}','{0}','{0}','{0}','{0}','{0}','{0}','{0}',
                                            '{0}','{0}','{0}','{0}')"""
        cursor.execute(sql)
        connection.commit()
    if len(dados) == 28: #Carro parado
        email = dados[0].replace('eml=','')
        id = dados[3].replace('id=','')
        longitude = dados[5].replace('kff1005=','')
        latitude = dados[6].replace('kff1006=','')
        vel_gps = dados[7].replace('kff1001=','')
        dir_gps = dados[8].replace('kff1007=','')
        porc_alcool = dados[9].replace('k52=','')
        carga_motor = dados[10].replace('k43=','')
        comb_utili = dados[11].replace('kff1271=','')
        efici_vol = dados[12].replace('kff1269=','')
        acur_gps = dados[13].replace('kff1239=','')
        km_por_litro = dados[14].replace('kff5202=','')
        vel_media = dados[17].replace('kff1272=','')
        nivel_combs = dados[18].replace('k2f=','')
        posicao_acelerador = dados[19].replace('k11=','')
        pressao_barometrica = dados[20].replace('k33=','')
        rpm = dados[21].replace('kc=','')
        sensor_acelerador = dados[22].replace('kff1223=','')
        razao_combs_ar = dados[23].replace('kff124d=','')
        razao_combus_minuto = dados[24].replace('kff125a=','')
        refrigeracao_motor = dados[25].replace('k5=','')
        tempo_viagem = dados[26].replace('kff1266=','')

        sql = f"""insert into dados values('{email}',curtime(),'{id}','{longitude}','{latitude}','{vel_gps}','{dir_gps}','{porc_alcool}','{carga_motor}',
                                            '{comb_utili}','{0}','{efici_vol}','{acur_gps}','{km_por_litro}','{vel_media}','{nivel_combs}','{posicao_acelerador}',
                                            '{pressao_barometrica}','{rpm}','{sensor_acelerador}','{razao_combs_ar}','{razao_combus_minuto}','{refrigeracao_motor}',
                                            '{tempo_viagem}')"""
        cursor.execute(sql)
        connection.commit()
    if len(dados) == 29: #Carro em movimento
        email = dados[0].replace('eml=',''),
        id = dados[3].replace('id=',''),
        longitude = dados[5].replace('kff1005=',''),
        latitude = dados[6].replace('kff1006=',''),
        vel_gps = dados[7].replace('kff1001=',''),
        dir_gps = dados[8].replace('kff1007=',''),
        porc_alcool = dados[9].replace('k52=',''),
        carga_motor = dados[10].replace('k43=',''),
        comb_utili = dados[11].replace('kff1271=',''),
        distancia = dados[12].replace('kff1204=',''),
        efici_vol = dados[13].replace('kff1269=',''),
        acur_gps = dados[14].replace('kff1239=',''),
        km_por_litro = dados[15].replace('kff5202=',''),
        vel_media = dados[18].replace('kff1272=',''),
        nivel_combs = dados[19].replace('k2f=',''),
        posicao_acelerador = dados[20].replace('k11=',''),
        pressao_barometrica = dados[21].replace('k33=',''),
        rpm = dados[22].replace('kc=',''),
        sensor_acelerador = dados[23].replace('kff1223=',''),
        razao_combs_ar = dados[24].replace('kff124d=',''),
        razao_combus_minuto = dados[25].replace('kff125a=',''),
        refrigeracao_motor = dados[26].replace('k5=',''),
        tempo_viagem = dados[27].replace('kff1266=','')

        sql = f"""insert into dados values('{email[0]}',curtime(),'{id[0]}','{longitude[0]}','{latitude[0]}','{vel_gps[0]}','{dir_gps[0]}','{porc_alcool[0]}','{carga_motor[0]}',
                                            '{comb_utili[0]}','{distancia[0]}','{efici_vol[0]}','{acur_gps[0]}','{km_por_litro[0]}','{vel_media[0]}','{nivel_combs[0]}',
                                            '{posicao_acelerador[0]}','{pressao_barometrica[0]}','{rpm[0]}','{sensor_acelerador[0]}','{razao_combs_ar[0]}',
                                            '{razao_combus_minuto[0]}','{refrigeracao_motor[0]}','{tempo_viagem[0]}')"""
        cursor.execute(sql)
        connection.commit()
    else:
        pass
    return JSONResponse(status_code=200, content="OK")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True)