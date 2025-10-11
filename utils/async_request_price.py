import aiohttp, asyncio, pandas, datetime

def data_format(response_data):
    ticker_data = {"date": [], "open": [], "close": [], "low": [], "high": [], "trade_price": [], "volume": []}
    
    for data in response_data[::-1]:
        ticker_data["date"] += [data["candle_date_time_kst"]]
        ticker_data["open"] += [data["opening_price"]]
        ticker_data["close"] += [data["trade_price"]]
        ticker_data["low"] += [data["low_price"]]
        ticker_data["high"] += [data["high_price"]]
        ticker_data["trade_price"] += [data["candle_acc_trade_price"]]
        ticker_data["volume"] += [data["candle_acc_trade_volume"]]

    df = pandas.DataFrame(ticker_data)
    df['date'] = pandas.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    return df

async def fetch_price(session, ticker, date, minutes=None):
    url = f"https://api.upbit.com/v1/candles/days"
    
    if minutes is not None:
        url = f"https://api.upbit.com/v1/candles/minutes/{minutes}"
            
    params = {
        'market': ticker,
        'to': date.strftime("%Y-%m-%dT%H:%M:%S"),
        'count': 24
    }

    async with session.get(url, params=params) as response:
        data = await response.json()
        if response.status == 429:
            await asyncio.sleep(1)
            return await fetch_price(session, ticker, date, minutes)
        
        data_frame = data_format(data)
        
        return [ticker, data_frame]
    

async def fetch_prices(tickers, end_date, minutes=None):
    async with aiohttp.ClientSession() as session:
        tasks = []
        dictionary_info = {}

        for ticker in tickers:
            tasks.append(fetch_price(session, ticker, end_date, minutes))

        prices = await asyncio.gather(*tasks)
        for price in prices:
            dictionary_info[price[0]] = price[1]

        return dictionary_info

def get_ticekr_info(target_date, ticker_list, minutes=None):
    tickers_info = asyncio.run(fetch_prices(ticker_list, target_date, minutes))
    return tickers_info

if __name__ == "__main__":
    # 분 단위 요청을 하면 UTC 기준으로 데이터가 반환된다 
    # 함수에 분 단위로 요청이 들어오면 9시간을 빼서 구현
    tickers_info = get_ticekr_info(datetime.datetime(2025, 1, 9), ["KRW-BTC"], 60)
    for ticker in tickers_info:
        print(ticker, tickers_info[ticker])
    
