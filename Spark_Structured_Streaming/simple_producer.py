import requests, json, argparse
from random import randrange, choice
from time import process_time, sleep
from datetime import datetime

now_timestamp = datetime.now()

def send(number_of_events_to_send, url_to_send_events):
    try:
        table = "compra"
        list_status = ["aprovada", "cancelada"]
        lojas = ["casa das maquinas", "mercado do seu zé", "vivara joias", "restaurante coma bem", "restaurante tia jumira", "loxinha barata"]
        t1_start = process_time()
        print(f"Post message of {table} event!\n")
        for x in range(0, number_of_events_to_send):
            
            valor_contrato = randrange(1.00, 10500.00, 1.00)
            valor_contrato = randrange(1, 12, 1)
            valor_parcelas = float(valor_contrato/parcelas)
            status = choice(list_status)
            loja = choice(lojas)

            json_data_message = f"""{{"id_compra":{randrange(70000, 99999, 1)},"id_conta":{randrange(0, 10001, 1)},"id_cartao":{randrange(100000, 999999, 1)},"descricao_transacao":"Parcelado com juros - Visa","data_compra":{now_timestamp},"valor":{valor_contrato},"valor_contrato":{valor_contrato},"taxa":5.43,"parcelas":{parcelas},"valor_parcelas":{valor_parcelas},"authorization_code":{randrange(1, 150, 2)},"loja":{loja},"status":{status},"codigo_moeda":986,"mcc":5139}}"""
            header = {'type': table, 'Content-Type': 'text/plain'}
            r = requests.post(url_to_send_events, data=json_data_message, headers=header)
            print(r.text)
            print(f"Message of event: {table} send successfull with status code: {r.status_code}")  # To check if status is 200, 404 and 500 are error
        t1_stop = process_time()
        print("Total time spending in single-thread program:")
        print("Elapsed time Start main:", t1_start)
        print("Elapsed time Stop main:", t1_stop)
        print("Elapsed time during the whole program in seconds:", (t1_stop - t1_start))
    except Exception as e:
        print(e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='simple producer')
    parser.add_argument('-u', '--url', dest='url', type=str)
    args = parser.parse_args()
    url_events = args.url
    parser.add_argument('-n', '--number_of_events', dest='number_of_events', type=str)
    number_of_events = args.number_of_events
    send(number_of_events, url_events)
