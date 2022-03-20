import random
import time

from twitter import Twitter, OAuth

from db_conn import PostgresConnection
from twitter_info import *

ALREADY_FOLLOWED_FILE = "already-followed.csv"
SENDED_USERS_FILE = "sended_users.csv"

t = Twitter(auth=OAuth(OAUTH_TOKEN, OAUTH_SECRET,
            CONSUMER_KEY, CONSUMER_SECRET))


def search_tweets(q, count=100, result_type="recent"):
    result = t.geo.search(query="BR", granularity="country")
    place_id = result['result']['places'][0]['id']

    result = t.search.tweets(q=f"{q} AND place:{place_id}", result_type=result_type, count=count)
    return result


def send_message(message, q, count, postgres_connection, result_type='recent'):
    count_sended = 0
    second_while = False
    while True:
        if second_while:
            time.sleep(60)
        result = search_tweets(q, USERS_PER_ROUND, result_type)
        sended_users = postgres_connection.get_all_user_sended()

        print(f"searched results: {len(result['statuses'])}")
        print(f"Sended users: {str(len(sended_users))}")
        if count_sended == count:
            break

        for tweet in result["statuses"]:
            second_while = True
            message_to_send = f"{message}"
            print(f"message: {message_to_send}")
            
            if int(tweet["user"]["id"]) not in sended_users:
                print(f"Tentando enviar mensagem para {tweet['user']['name']}.")
                try:
                    t.direct_messages.events.new(
                    _json={
                        "event": {
                            "type": "message_create",
                            "message_create": {
                                "target": {
                                    "recipient_id": tweet["user"]["id"]},
                                "message_data": {
                                    "text": message_to_send}}}})
                    print(f"Mensagem enviada para {tweet['user']['name']}.")
                    postgres_connection.insert_user_in_table(tweet["user"]["id"], q, message_to_send)
                    sended_users = postgres_connection.get_all_user_already_in_tag(TAG)
                    count_sended += 1
                    seconds = random.randint(70, MAX_SECCONDS)
                    print(f"Aguardando {seconds} segundos para o próximo envio.")
                    time.sleep(seconds)
                except Exception as ex:
                    print(f"mensagem não enviada: {ex}")
                    postgres_connection.insert_user_in_table(tweet["user"]["id"], q, message_to_send, False)
                if count_sended == count:
                    break

try:
    postgres_connection = PostgresConnection()
    postgres_connection.create_user_table()
    print('Começando envio de mensagens.')
    for send in range(0, 24):
        send_message(
            MESSAGE,
            TAG,
            COUNT_PER_ROUND,
            postgres_connection,
            'mixed')
        print("aguardando 15 minutos para continuar o loop")
        time.sleep(3600)
except Exception as e:
    print(e)

