'''
Команда для запуска:
workon binance_venv && cd /home/user/Binance/ && python main.py -json some_email@gmail.com.json

Алгоритм торговли реализован на двух потоках: один только принимает сообщения, другой делает все остальное
(обрабатывает принятые сообщения, вычисляет индикаторы, принимает решение о покупке, покупает,
следит за позициями). В первые 10 секунд каждой минуты второй поток заблокирован, чтобы первый не
пропустил сообщений о новых свечках.
'''


from binance import Client, ThreadedWebsocketManager
from threading import Thread
import requests
import numpy as np
import pandas as pd
import pandas_ta as ta
import datetime
import time
import urllib
import hmac
import hashlib
import os
from loguru import logger
import warnings
import argparse
import json
from modules import *
pd.options.mode.chained_assignment = None
warnings.simplefilter(action='ignore', category=FutureWarning)


current_time = pd.Timestamp.now().strftime(format='%Y-%m-%d_%H-%M-%S_%f')
logger.add(f'binance.{current_time}.log', rotation="1 MB")


class Bot:
    def __init__(self, creds):
        logger.info(f'Бот запущен на аккаунте {creds["email"]}')
        self.today = datetime.date.today()
        self.path = os.getcwd()
        self.working_period = creds['working_period']
        self.stop_time = pd.Timestamp.now() + pd.Timedelta(self.working_period)
        self.tickers = creds['tickers']
        self.tf = creds['tf']
        self.tickers_std = pd.Series(np.zeros(len(self.tickers)), dtype='int', index=self.tickers)
        self.base_url = "https://testnet.binancefuture.com"
        self.api_key = creds['key']
        self.api_secret = creds['secret']
        self.equity = None
        self.cash = None
        self.client = binance.Client(self.api_key, self.api_secret)
        self.ws = binance.ThreadedWebsocketManager(api_key=self.api_key, api_secret=self.api_secret)
        self.message_collector = []
        self.new_info = False
        self.orders_to_keep_trying = []
        self.stop_loss = creds['stop_loss'] # %
        self.take_profit = creds['take_profit'] # %
        self.conditions = pd.Series([creds['open_long'], creds['open_short']], index=['Open_long', 'Open_short'], dtype="str")
        info = [item for item in self.client.futures_exchange_info()['symbols'] if item['symbol'] in self.tickers]
        self.precision = pd.DataFrame(info).set_index('symbol')[['pricePrecision', 'quantityPrecision']]
        self.client.futures_create_order = try_submit_order()(self.client.futures_create_order)
        self.close_all_positions()
        self.stocks = pd.DataFrame()
        cols = [
            'Open_time','Open','High','Low', 'Close','Volume','Close time',
            'Quote asset volume','Number of trades','Taker buy base asset volume',
            'Taker buy quote asset volume','Ignore'
        ]
        start_str = str(pd.Timestamp.now() - pd.Timedelta(creds['lookback_period']))[:10] # 1W
        for s in self.tickers:
            price = self.client.futures_historical_klines(symbol=s, interval=f'{self.tf}m', start_str=start_str)
            df = pd.DataFrame(price, columns=cols)
            df['Open_time'] = pd.to_datetime(df['Open_time'], unit='ms')
            df.set_index('Open_time', inplace = True)
            df.columns = pd.MultiIndex.from_product([df.columns, [s]], names=['Indicators','Tickers'])
            self.stocks = pd.concat([self.stocks, df[['Open','High','Low', 'Close','Volume']]], axis=1)
        self.stocks = self.stocks.astype(float)
        logger.info(f'Stocks:\n{self.stocks}')
        self.client.get_server_time = try_get_server_time()(self.client.get_server_time)
        for t in self.tickers:
            self.client.futures_change_leverage(symbol=t, leverage=1)
        self.positions = pd.Series(np.zeros(len(self.tickers)), dtype='int', index=self.tickers)
        self.take_profit_prices = {}
        # logger.debug(self.positions)


    def close_all_positions(self):
        logger.info('Проверка и закрытие заявок')
        for t in self.tickers:
            self.custom_api_request('/fapi/v1/allOpenOrders', 'delete', {'symbol': t})
            logger.success(f'Отменены заявки по тикеру {t}')
        logger.info('Проверка и закрытие позиций')
        self.fill_positions()
        for t, qty in self.positions[self.positions != 0].to_dict().items():
            self.client.futures_create_order(
                symbol=t, side='SELL' if qty>0 else 'BUY', type='MARKET', quantity=np.abs(qty),
                reduceOnly=True # Не дает "провалиться" в противоположную из лонга в шорт и наоборот
            )
            logger.success(f'Закрыта позиция по тикеру {t}')
        self.fill_positions()


    def add_signature_to_params(self, params):
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(
            str.encode(self.api_secret), str.encode(query_string), hashlib.sha256
        ).hexdigest()
        params = {**params, **{'signature': signature}}
        return params


    def custom_api_request(self, url_part, request_type='get', remaining_params={}, extract_from_list=False):
        st = self.client.get_server_time()['serverTime']
        remaining_params.update({
            'recvWindow': 5000,
            'timestamp': st,
        })
        remaining_params = self.add_signature_to_params(remaining_params)
        headers = {
            'X-MBX-APIKEY': self.api_key,
        }
        ans = eval(f'requests.{request_type}(self.base_url + url_part, params=remaining_params, headers=headers)')
        ans = ans.json()
        if extract_from_list:
            ans = [d for d in ans if d['updateTime'] != 0]
            if len(ans) == 1:
                ans = ans[0]
        return ans


    def on_message(self, message):
        try:
            message = message['data']['k']
            if message['x']:
                # Каждое сообщение складывается в специальный список.
                # Затем, когда все сообщения собраны, они обрабатываются
                self.message_collector.append(message)
                self.new_info = True
        except Exception:
            self.ws.stop()
            self.run_process = False
            logger.exception('An error stopped the script')


    def screen(self):
        i = len(self.stocks) - 1
        self.tickers_std = 100 * self.stocks['Close'][i-30:i].std() / self.stocks['Close'][i-30:i].mean()
        self.tickers_std.sort_values(ascending=False, inplace=True)


    def collect_info(self):
        if len(self.message_collector) == 0:
            empty_ind = pd.Timestamp.now().round(freq='T') - pd.Timedelta(f'{self.tf} min')
            self.stocks.loc[empty_ind] = np.nan
        else:
            df = pd.DataFrame(self.message_collector)
            df.set_index('s', inplace=True)
            df['t'] = pd.to_datetime(df['t'], unit='ms')
            self.message_collector = []
            for ind in df['t'].unique():
                subdf = df.loc[df['t'] == ind]
                if ind not in self.stocks.index.tolist():
                    self.stocks.loc[ind] = np.nan
                for t in subdf.index:
                    # FIXME
                    self.stocks['Open', t][ind] = float(subdf['o'][t])
                    self.stocks['Close', t][ind] = float(subdf['c'][t])
                    self.stocks['High', t][ind] = float(subdf['h'][t])
                    self.stocks['Low', t][ind] = float(subdf['l'][t])
                    self.stocks['Volume', t][ind] = float(subdf['v'][t])
        logger.debug(f'Stocks after:\n{self.stocks}')
        self.stocks = self.stocks.iloc[1:, :]
        # self.stocks.to_excel(os.path.join(self.path, f'xlsx/Info test {self.today}.xlsx'))


    def evaluate_indicators(self):
        for t in self.tickers:
            # MA
            self.stocks['Short_term_avg', t] = ta.sma(self.stocks['Close', t], length = 4)
            self.stocks['Long_term_avg', t] = ta.sma(self.stocks['Close', t], length = 80)
            # RSI
            self.stocks['RSI', t] = ta.rsi(self.stocks['Close', t], length = 4)
            # MACD
            self.stocks['MACD_diff', t] = ta.macd(self.stocks['Close', t], fast = 12, slow = 26, signal = 9).iloc[:,1]
            # ADX
            adx = ta.adx(high=self.stocks['High', t], close=self.stocks['Close', t], low=self.stocks['Low', t], length = 6, lensig = 6)
            self.stocks['DI+', t] = adx.iloc[:,1]
            self.stocks['DI-', t] = adx.iloc[:,2]
            self.stocks['ADX', t] = adx.iloc[:,0]


    def send_orders(self, side, qty, t, price):
        price_precision = self.precision['pricePrecision'][t]
        opposite_side = "SELL" if side == "BUY" else "BUY"
        stop_loss_coef = 1 if side == "BUY" else -1
        orders = [
            {
                "symbol" : t,
                "side" : side,
                "type" : "MARKET",
                "quantity" : qty
            },
            {
                "symbol" : t,
                "side" : opposite_side,
                "type" : "STOP_MARKET",
                "reduceOnly": True,
                "quantity" : qty,
                "stopPrice": np.round(price * (100 - self.stop_loss * stop_loss_coef) / 100, price_precision)
            },
            # {
            #     "symbol" : t,
            #     "side" : opposite_side,
            #     "type" : "LIMIT",
            #     "quantity" : qty,
            #     "price": np.round(price * (100 + self.take_profit * stop_loss_coef) / 100, price_precision),
            #     "timeInForce": "GTC"
            # }
        ]
        sent_first_order = False
        try:
            self.client.futures_create_order(**orders[0])
            sent_first_order = True
            time.sleep(2)
        except:
            logger.error(f'Не удалось открыть позицию {t}')
        if sent_first_order: # Если отправлена заявка на открытие позиции, то отправляем стоп лосс заявку
            try:
                self.client.futures_create_order(**orders[1])
                self.take_profit_prices[t] = np.round(price * (100 + self.take_profit * stop_loss_coef) / 100, price_precision)
            except:
                self.close_position(t)


    def close_position(self, t):
        self.custom_api_request('/fapi/v1/allOpenOrders', 'delete', {'symbol': t})
        logger.success(f'Отменены заявки по тикеру {t}')
        self.fill_positions()
        qty = self.positions[t]
        opposite_side = 'SELL' if qty > 0 else 'BUY'
        if qty != 0:
            try:
                o = {
                    "symbol": t,
                    "side": opposite_side,
                    "type": 'MARKET',
                    "quantity": qty,
                    "reduceOnly": True
                }
                self.client.futures_create_order(**o)
                logger.success(f'Закрыта позиция по тикеру {t}')
            except:
                logger.error(f'Закрытие позиции по тикеру {t} не удалось. Заявка будет отправлена еще раз позднее')
                self.orders_to_keep_trying.append(o)
        self.fill_positions()


    def keep_trying_to_send_orders(self):
        for o in self.orders_to_keep_trying:
            logger.info(f'Повторная попытка выполнить ордер {o}')
            try:
                self.client.futures_create_order(**o)
                self.orders_to_keep_trying.remove(o)
            except:
                pass


    def make_decision(self):
        i = len(self.stocks) - 1
        deals = 0
        account = self.custom_api_request('/fapi/v2/account')
        self.equity = float(account["totalWalletBalance"])
        self.cash = float(account["availableBalance"])
        logger.info(f'Общее состояние счета: {self.equity} $')
        logger.info(f'Доступно средств: {self.cash} $')
        update_cash = False
        for t in self.tickers_std.index:
            if update_cash:
                account = self.custom_api_request('/fapi/v2/account')
                self.cash = float(account["availableBalance"])
                logger.info(f'Доступно средств: {self.cash} $')
                update_cash = False
            qty_precision = self.precision['quantityPrecision'][t]
            # price_precision = self.precision['pricePrecision'][t]
            price = self.stocks['Close', t][i]
            if eval(self.conditions.Open_long) and (len(self.positions) - np.sum(self.positions == 0) + deals < 5) and self.positions[t] == 0:
                deals += 1
                update_cash = True
                qty = np.round(
                    np.min([
                        self.equity * 15 / 100,
                        self.cash
                    ]) * 1.1 / price, qty_precision)
                if qty * price > 10 and qty * price < self.cash and qty != 0:
                    self.send_orders("BUY", qty, t, price)
            elif eval(self.conditions.Open_short) and (len(self.positions) - np.sum(self.positions == 0) + deals < 5) and self.positions[t] == 0:
                deals += 1
                update_cash = True
                qty = np.round(
                    np.min([
                        float(self.equity) * 15 / 100,
                        float(self.cash)
                    ]) * 1.1 / price, qty_precision)
                if qty * price > 10 and qty * price < self.cash and qty != 0:
                    self.send_orders("SELL", qty, t, price)


    def fill_positions(self):
        ans = self.custom_api_request('/fapi/v2/positionRisk', extract_from_list=True)
        pos = pd.DataFrame(ans).set_index('symbol')['positionAmt']
        pos = pos[pos.index.isin(self.tickers)].astype(float)
        logger.debug(f'POS: {pos}')
        rest_ind = [s for s in self.tickers if s not in pos.index]
        rest_pos = pd.Series(np.zeros(len(rest_ind)), dtype='float', index=rest_ind)
        self.positions = rest_pos.append(pos)
        logger.info(f'Позиции:\n{self.positions[self.positions != 0].to_dict()}')


    def process(self):
        while self.run_process:
            now = pd.Timestamp.now()
            if pd.Timestamp.now() >= self.stop_time:
                self.ws.stop()
                self.run_process = False
                logger.info('Срок работы бота закончился')
            if self.new_info and now.second >= 11:
                if np.mod(now.minute, self.tf) == 0:
                    self.collect_info()
                    self.evaluate_indicators()
                    self.fill_positions()
                    self.make_decision()
                    self.keep_trying_to_send_orders()
                    logger.info('__________________________________________________')
                    logger.info('')
                    self.new_info = False
            #         self.stocks.to_excel(os.path.join(self.path, f'xlsx/Info {self.today}.xlsx'))
            time.sleep(71 - datetime.datetime.now().second)


    def run(self):
        try:
            self.run_process = True
            self.ws.start()
            streams = [
                t.lower() + '@kline_' + str(self.tf) + 'm' for t in self.tickers
            ]
            self.ws.start_multiplex_socket(streams=streams, callback=self.on_message)
            self.thread2 = Thread(target=self.process)
            self.thread2.start()
            self.ws.join()
            # self.thread2.join()
        except Exception:
            self.ws.stop()
            self.run_process = False
            logger.exception('An error stopped the script')


def start_bot():
    parser = argparse.ArgumentParser()
    parser.add_argument('-json', default=None, help='Путь к файлу, в котором содержатся параметры запуска Binance бота')
    params = parser.parse_args()
    f = open(params.json, "r")
    creds = json.loads(f.read())
    f.close()
    bot = Bot(creds)
    bot.run()
    # bot.debug()


if __name__ == '__main__':
    start_bot()









