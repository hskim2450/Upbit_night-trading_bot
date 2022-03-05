import os
import jwt
import pyupbit
from pandas import DataFrame
import uuid
import hashlib
from urllib.parse import urlencode
import requests
import schedule
import datetime
import time
from datetime import datetime, timedelta
import json

#Slack메세지 함수, 프로그램 시작 전송
def post_to_slack(text):
    slack_webhook_url = "webhook-url"
    headers = { "Content-type": "application/json" }
    data = { "text" : text }
    res = requests.post(slack_webhook_url, headers=headers, data=json.dumps(data))
    if res.status_code != 200:
        print("message_error")

post_to_slack("#upbit trading bot started")

#업비트 api
os.environ['UPBIT_OPEN_API_ACCESS_KEY'] = 'access-key'
os.environ['UPBIT_OPEN_API_SECRET_KEY'] = 'secret-key'
access_key = os.environ['UPBIT_OPEN_API_ACCESS_KEY']
secret_key = os.environ['UPBIT_OPEN_API_SECRET_KEY']
server_url = "https://api.upbit.com"

#현재 계좌 상태 데이터베이스 생성 함수
def cur_balance():
    payload = {
        'access_key': access_key,
        'nonce': str(uuid.uuid4()),
    }
    jwt_token = jwt.encode(payload, secret_key)
    authorize_token = 'Bearer {}'.format(jwt_token)
    headers = {"Authorization": authorize_token}
    res = requests.get(server_url + "/v1/accounts", headers=headers)
    return DataFrame(res.json())

#현재 계좌 내 원화 slack 전송 함수
def post_cur_KRW():
    df_balance = cur_balance()
    balance_KRW = float(df_balance.head(1)["balance"])
    post_to_slack("cur_balance_KRW= " + str(balance_KRW))
    return balance_KRW

#전날 상승 상위 5개 종목 추리는 함수
def raised_list():
    try:
        yesterday = datetime.now()-timedelta(days=1) 
        time_yesterday = yesterday.strftime('%Y-%m-%d %H:%M:%S') #strftime은 localtime을 원하는 형태의 문자열로 바꿔주는 함수. (%H: 24h단위 시간)
        yes_market_list = list()
        yes_rate_list = list()
        krw_tickers = pyupbit.get_tickers("KRW")
        for market in krw_tickers:
            url = "https://api.upbit.com/v1/candles/days?market={}&to={}&count=1".format(market, time_yesterday)
            headers = {"Accept": "application/json"}
            response = requests.request("GET", url, headers=headers)
            yes_market_list.append(market)
            yes_rate_list.append(response.json()[0]['change_rate'])
            time.sleep(0.1)
        df_yes_market = DataFrame({'market': yes_market_list, 'change_rate': yes_rate_list})
        raised_market_list = list(df_yes_market.sort_values('change_rate', ascending=False).head(5)["market"])
        return raised_market_list
    except Exception as e:
        post_to_slack(e)

#리스트 종목 시장가 매수 함수
def bid():
    cur_time = time.ctime()
    post_to_slack('매수시작. 시작시간 = ' + cur_time)

    global balance_KRW_before
    balance_KRW_before = post_cur_KRW()

    #시세 조회 // 하위 5개종목 리스트 생성
    krw_tickers = pyupbit.get_tickers("KRW")
    url = "https://api.upbit.com/v1/ticker"
    querystring = {"markets": krw_tickers}
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    df_markets = DataFrame(response.json())
    global market_list
    market_list = list(df_markets[df_markets['acc_trade_price'] >= 1e10].sort_values("signed_change_rate").head(5)["market"])
    post_to_slack("market list =" + str(market_list))

    raised_market_list = raised_list()
    post_to_slack("exception list = "+str(raised_market_list))
    market_list = [market for market in market_list if market not in raised_market_list] #전날 등락률 상위 5개 종목 제거
    post_to_slack("targeted market list =" + str(market_list))
    if not market_list:
        return
    for market in market_list:
        bid_price = str(int(balance_KRW_before/len(market_list)/1000)*1000) #매수규모 커지면 수수료 충분히 빼놔야 할듯
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
            post_to_slack('bought ' + str(market))
            post_cur_KRW()
        except Exception as e:
            post_to_slack('error while bidding: ' + e)
            pass
        time.sleep(0.2)
    post_to_slack('매수 종료')

#리스트 종목 시장가 매도 함수
def sell():
    cur_time = time.ctime()
    post_to_slack('매도시작. 시작시간 = ' + cur_time)

    df_balance = cur_balance()
    if not market_list:
        return
    for market in market_list:
        is_market = 'KRW-' + df_balance['currency'] == market
        market_index = df_balance.index[is_market].tolist()[0]
        volume = df_balance.at[market_index, 'balance']
        post_to_slack('targeted market = ' + str(market))
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
            post_to_slack('sold ' + str(market))
        except Exception as e:
            post_to_slack('error while selling: ' + e)
            pass
        time.sleep(0.2)
    post_to_slack('매도 종료')

    balance_KRW_after = post_cur_KRW()
    profit = balance_KRW_after - balance_KRW_before
    post_to_slack('당일 수익 = ' + str(profit) + '원')
    post_to_slack('당일 수익률 = ' + str(profit/balance_KRW_before*100) + '%')


#main
schedule.every().day.at("15:30").do(bid)
schedule.every().day.at("23:45").do(sell)

while True:
    schedule.run_pending()
    time.sleep(1)
