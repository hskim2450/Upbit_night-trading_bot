import os
import jwt
import pyupbit
from pandas import Series, DataFrame
import uuid
import hashlib
from urllib.parse import urlencode
import requests
import schedule
import time

#ip주소 확인
ip = requests.get("https://api.ipify.org").text
print("My public IP address : ", ip)

#업비트 API
os.environ['UPBIT_OPEN_API_ACCESS_KEY'] = 'user_access_key'
os.environ['UPBIT_OPEN_API_SECRET_KEY'] = 'user_secret_key'
access_key = os.environ['UPBIT_OPEN_API_ACCESS_KEY']
secret_key = os.environ['UPBIT_OPEN_API_SECRET_KEY']
server_url = "https://api.upbit.com"

payload = {
    'access_key': access_key,
    'nonce': str(uuid.uuid4()),
}

#계좌 조회
jwt_token = jwt.encode(payload, secret_key)
authorize_token = 'Bearer {}'.format(jwt_token)
headers = {"Authorization": authorize_token}
res = requests.get(server_url + "/v1/accounts", headers=headers)
df_balance = DataFrame(res.json())
balance_KRW = float(df_balance.head(1)["balance"])

#리스트 종목 시장가 매수 함수
def bid():
    print('bid_timing')
    #시세 조회 // 하위 5개종목 리스트 생성
    krw_tickers = pyupbit.get_tickers("KRW")
    url = "https://api.upbit.com/v1/ticker"
    querystring = {"markets": krw_tickers}
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    df_markets = DataFrame(response.json())
    market_list = list(df_markets.sort_values("signed_change_rate").head(5)["market"])
    for market in market_list:
        print(balance_KRW)
        print(market)
        bid_price = str(int(balance_KRW/len(market_list)/1000)*1000) #매수규모 커지면 수수료 충분히 빼놔야 할듯
        try:
            query = {
                'market': market,
                'side': 'bid',
                'price': bid_price,
                'ord_type': 'price',
                }
            query_string = urlencode(query).encode()

            m = hashlib.sha512()
            m.update(query_string)
            query_hash = m.hexdigest()

            payload = {
                'access_key': access_key,
                'nonce': str(uuid.uuid4()),
                'query_hash': query_hash,
                'query_hash_alg': 'SHA512',
                }

            jwt_token = jwt.encode(payload, secret_key)
            authorize_token = 'Bearer {}'.format(jwt_token)
            headers = {"Authorization": authorize_token}
            res = requests.post(server_url + "/v1/orders", params=query, headers=headers)
        except:
            print('error')
        time.sleep(0.2)

#리스트 종목 시장가 매도 함수
def sell():
    print('sell_timing')
    payload = {
    'access_key': access_key,
    'nonce': str(uuid.uuid4()),
    }
    jwt_token = jwt.encode(payload, secret_key)
    authorize_token = 'Bearer {}'.format(jwt_token)
    headers = {"Authorization": authorize_token}
    res = requests.get(server_url + "/v1/accounts", headers=headers)
    df_balance = DataFrame(res.json())
    for market in market_list:
        is_market = 'KRW-' + df_balance['currency'] == market
        market_index = df_balance.index[is_market].tolist()[0]
        volume = df_balance.at[market_index, 'balance']
        print('balance_KRW = ' + str(balance_KRW))
        print('market = ' + str(market))
        print('volume = ' + str(volume))
        try:
            query = {
                'market': market,
                'side': 'ask',
                'volume': volume,
                'ord_type': 'market',
            }
            query_string = urlencode(query).encode()

            m = hashlib.sha512()
            m.update(query_string)
            query_hash = m.hexdigest()

            payload = {
                'access_key': access_key,
                'nonce': str(uuid.uuid4()),
                'query_hash': query_hash,
                'query_hash_alg': 'SHA512',
            }

            jwt_token = jwt.encode(payload, secret_key)
            authorize_token = 'Bearer {}'.format(jwt_token)
            headers = {"Authorization": authorize_token}
            res = requests.post(server_url + "/v1/orders", params=query, headers=headers)
        except:
            print('error')
        time.sleep(0.2)

#app 시작시간
cur_time = time.ctime()
print("시작시간 = " + cur_time)

#main
schedule.every().day.at("00:00").do(bid)
schedule.every().day.at("09:00").do(sell)

while True:
    schedule.run_pending()
    time.sleep(1)
