import aiohttp, asyncio

def data_format(response_data):
    for data in response_data:
        return data["acc_trade_price_24h"]

async def fetch_volume(session, ticker):
    url = f"https://api.upbit.com/v1/ticker"
    
    params = {
        'markets': ticker,
    }

    async with session.get(url, params=params) as response:
        data = await response.json()
        if response.status == 429:
            await asyncio.sleep(1)
            return await fetch_volume(session, ticker)
        
        data_frame = data_format(data)
        
        return [ticker, data_frame]
    

async def fetch_volumes(tickers):
    async with aiohttp.ClientSession() as session:
        tasks = []
        dictionary_info = {}

        for ticker in tickers:
            tasks.append(fetch_volume(session, ticker))

        prices = await asyncio.gather(*tasks)
        for price in prices:
            dictionary_info[price[0]] = price[1]

        return dictionary_info

def get_ticekr_volume_info(ticker_list):
    tickers_info = asyncio.run(fetch_volumes(ticker_list))
    return tickers_info

if __name__ == "__main__":
    tickers_info = get_ticekr_volume_info(["KRW-BTC"])
    for ticker in tickers_info:
        print(ticker, tickers_info[ticker])
    
