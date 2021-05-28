from kafka.producer import KafkaProducer
from random import randrange, choice
from datetime import datetime
import argparse

parser = argparse.ArgumentParser(description='simple producer')
parser.add_argument('-u', '--url', dest='url', type=str)
args = parser.parse_args()
url_events = args.url

list_status = ["aprovada", "cancelada"]
lojas = ["casa das maquinas", "mercado do seu zé", "vivara joias", "restaurante coma bem", "restaurante tia jumira", "loxinha barata"]
now_timestamp = datetime.now()

producer = KafkaProducer(bootstrap_servers=url_events)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception


# produce asynchronously with callbacks
for _ in range(15):
    valor_contrato = randrange(1.00, 10500.00, 1.00)
    parcelas = randrange(1, 12, 1)
    valor_parcelas = float(valor_contrato/parcelas)
    status = choice(list_status)
    loja = choice(lojas)

    json_data_message = f"""{{"id_compra":{randrange(70000, 99999, 1)},"id_conta":{randrange(0, 10001, 1)},"id_cartao":{randrange(100000, 999999, 1)},"descricao_transacao":"Parcelado com juros - Visa","data_compra":{now_timestamp},"valor":{valor_contrato},"valor_contrato":{valor_contrato},"taxa":5.43,"parcelas":{parcelas},"valor_parcelas":{valor_parcelas},"authorization_code":{randrange(1, 150, 2)},"loja":{loja},"status":{status},"codigo_moeda":986,"mcc":5139}}"""
    producer.send('evento_compra',json_data_message.encode('utf-8'))\
         .add_callback(on_send_success).add_errback(on_send_error)
    #producer.send('compra', b'msg').add_callback(on_send_success).add_errback(on_send_error)

# block until all async messages are sent
producer.flush()

# configure multiple retries
#producer = KafkaProducer(retries=5)
