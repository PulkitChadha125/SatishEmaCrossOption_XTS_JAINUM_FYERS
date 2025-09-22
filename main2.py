import pandas as pd
import datetime  # full module
import json
# from datetime import datetime, timedelta
from pandas.compat.pyarrow import pa
import polars as pl
import polars_talib as plta
import time
import traceback
import sys
import math

# Ensure the SDK path is included for import
sys.path.append('.')
# Now import the SDK
from xtspythonclientapisdk.Connect import XTSConnect
import threading
from FyresIntegration import *


FyerSymbolList=[]
xts_marketdata = None
xt=None
Future_instrument_id_list=[]
Equity_instrument_id_list=[]


def normalize_to_step(price, step):
    if price is None or step in (None, 0):
        return price
    step = float(step)
    # nearest (half-up): 22325->22350, 22322->22300
    return step * math.floor((float(price) + step / 2.0) / step)


import json

def get_ltp(nfo_ins_id):
    response = xts_marketdata.get_quote(
        Instruments=[{"exchangeSegment": 2, "exchangeInstrumentID": nfo_ins_id}],
        xtsMessageCode=1502,
        publishFormat='JSON'
    )



    
    
    # Extract the listQuotes first element
    list_quotes = response['result']['listQuotes'][0]
    
    # Parse the JSON string
    quote_data = json.loads(list_quotes)
    
    # Get LastTradedPrice
    ltp = quote_data['Touchline']['LastTradedPrice']
    
    return ltp

def get_ask(nfo_ins_id):
    response = xts_marketdata.get_quote(
        Instruments=[{"exchangeSegment": 2, "exchangeInstrumentID": nfo_ins_id}],
        xtsMessageCode=1502,
        publishFormat='JSON'
    )
    
    list_quotes = response['result']['listQuotes'][0]
    quote_data = json.loads(list_quotes)
    
    # Best Ask = first entry in Asks (lowest price)
    ask_price = quote_data['Asks'][0]['Price']
    
    return ask_price


def get_bid(nfo_ins_id):
    response = xts_marketdata.get_quote(
        Instruments=[{"exchangeSegment": 2, "exchangeInstrumentID": nfo_ins_id}],
        xtsMessageCode=1502,
        publishFormat='JSON'
    )
    
    list_quotes = response['result']['listQuotes'][0]
    quote_data = json.loads(list_quotes)
    
    # Best Bid = first entry in Bids (highest price)
    bid_price = quote_data['Bids'][0]['Price']
    
    return bid_price

def place_order(nfo_ins_id,order_quantity,order_side,price,unique_key,symbol,ticksize):
    import random
    import math
    val=None
    # write_to_order_logs(f"Initiating order placement for : [{datetime.now()}] {symbol} {order_side} quantity: {order_quantity} price: {price}")
    one_percent = price * 0.01
    adjusted_price = price
    if order_side == "BUY":
        val=xt.TRANSACTION_TYPE_BUY
        adjusted_price = price + one_percent
        # Round up to nearest multiple of ticksize for buy orders
        if ticksize and ticksize > 0:
            adjusted_price = math.ceil(adjusted_price / ticksize) * ticksize
        
    elif order_side == "SELL":
        val=xt.TRANSACTION_TYPE_SELL
        adjusted_price = price - one_percent
        # Round down to nearest multiple of ticksize for sell orders
        if ticksize and ticksize > 0:
            adjusted_price = math.floor(adjusted_price / ticksize) * ticksize

    try:
        response=xt.place_order (
        exchangeSegment=xt.EXCHANGE_NSEFO,
        exchangeInstrumentID=nfo_ins_id,
        productType=xt.PRODUCT_MIS,
        orderType=xt.ORDER_TYPE_LIMIT,
        orderSide=val,
        timeInForce=xt.VALIDITY_DAY,
        disclosedQuantity=0,
        orderQuantity=order_quantity,
        limitPrice=adjusted_price,
        stopPrice=0,
        apiOrderSource="WEBAPI",
        orderUniqueIdentifier="454845",
        clientID="*****" )

        print("Place Order: ", response)
        write_to_order_logs(f"Broker Order Response: [{datetime.now()}] {symbol} {order_side} quantity: {order_quantity} price: {price} response: {response}")
        print("-" * 50) 
    except Exception as e:
        print(f"Error placing order: {str(e)}")
        write_to_order_logs(f"Error placing order {symbol} {order_side} quantity: {order_quantity} price: {price} error: {str(e)}")
        write_to_order_logs("-" * 50)
        traceback.print_exc()

def UpdateData():
    global result_dict

    for symbol, ltp in shared_data.items(): 
        for key, value in result_dict.items():
            if value.get('FyersFutSymbol') == symbol:
                value['FyersFutLtp'] = float(ltp)
                print(f"Updated {symbol} with LTP: {ltp}")
                break  # Optional: skip if you assume each symbol is unique
           



def convert_to_polars(df):
    # Make a copy to avoid modifying the original dataframe
    df_copy = df.copy()
    
    # Convert timezone-aware datetime to timezone-naive
    if 'date' in df_copy.columns and df_copy['date'].dtype.name.startswith('datetime'):
        df_copy['date'] = df_copy['date'].dt.tz_localize(None)
    
    # Reset index to preserve date information as a column
    if df_copy.index.name and df_copy.index.name != 'index':
        df_copy = df_copy.reset_index()
    
    # Ensure numeric columns are properly typed
    numeric_columns = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_columns:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
    
    # Convert to polars
    polars_df = pl.from_pandas(df_copy)
    
    # Ensure close column exists and is numeric
    if 'close' in polars_df.columns:
        polars_df = polars_df.with_columns([
            pl.col("close").cast(pl.Float64)
        ])
    
    return polars_df

def delete_file_contents(file_name):
    try:
        # Open the file in write mode, which truncates it (deletes contents)
        with open(file_name, 'w') as file:
            file.truncate(0)
        print(f"Contents of {file_name} have been deleted.")
    except FileNotFoundError:
        print(f"File {file_name} not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def get_api_credentials_Fyers():
    credentials_dict_fyers = {}
    try:
        df = pd.read_csv('FyersCredentials.csv')
        for index, row in df.iterrows():
            title = row['Title']
            value = row['Value']
            credentials_dict_fyers[title] = value
    except pd.errors.EmptyDataError:
        print("The CSV FyersCredentials.csv file is empty or has no data.")
    except FileNotFoundError:
        print("The CSV FyersCredentials.csv file was not found.")
    except Exception as e:
        print("An error occurred while reading the CSV FyersCredentials.csv file:", str(e))
    return credentials_dict_fyers

def get_user_settings():
    global result_dict, instrument_id_list, Equity_instrument_id_list, Future_instrument_id_list, FyerSymbolList
    from datetime import datetime
    import pandas as pd

    delete_file_contents("OrderLog.txt")

    try:
        csv_path = 'TradeSettings.csv'
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()

        result_dict = {}
        instrument_id_list = []
        Equity_instrument_id_list = []
        Future_instrument_id_list = []
        FyerSymbolList = []
        # unique_key = f"{symbol}_{OptionType}_{Strike}"

        for index, row in df.iterrows():
            symbol = row['Symbol']
            expiry = row['EXPIERY']  # Format: 29-05-2025

            # Convert expiry to API format: DDMonYYYY (e.g., 29May2025)
            expiry_api_format = datetime.strptime(expiry, "%d-%m-%Y").strftime("%d%b%Y")
            expiry_date = datetime.strptime(expiry, "%d-%m-%Y")

            OptionExpiery= datetime.strptime(row["OptionExpiery"], "%d-%m-%Y").strftime("%d%b%Y")
            expiry_date = datetime.strptime(row["OptionExpiery"], "%d-%m-%Y")

          
            if symbol == "NIFTY" or symbol == "BANKNIFTY":
                fut_response = xts_marketdata.get_future_symbol(
                    exchangeSegment=2,  # NSEFO
                    series='FUTIDX',
                    symbol=symbol,
                    expiryDate=expiry_api_format
                )
                # print(f"fut_response index {symbol}: {fut_response}")
            
            else:
                 # Fetch FUTSTK instrument ID
                fut_response = xts_marketdata.get_future_symbol(
                        exchangeSegment=2,  # NSEFO
                        series='FUTSTK',
                        symbol=symbol,
                        expiryDate=expiry_api_format
                    )
                # print(f"fut_response stock {symbol}: {fut_response}")
                    # print(f"fut_response: {fut_response}")
            


            if fut_response['type'] == 'success' and 'result' in fut_response:
                result_item = fut_response['result'][0]
                NSEFOinstrument_id = int(result_item['ExchangeInstrumentID'])
                lot_size = int(result_item.get('LotSize', 0))
                tick_size = float(result_item.get('TickSize', 0.0))  # Extract tick size from future response
            else:
                print(f"[ERROR] Could not get FUTSTK instrument ID for {symbol} {expiry_api_format}")
                NSEFOinstrument_id = None
                lot_size = None
                tick_size = None
            
            print("NSEFOinstrument_id: ", NSEFOinstrument_id)
            print("lot_size: ", lot_size)
            print("tick_size: ", tick_size)

           

            try:
    # Parse '26-06-2025' → datetime object
                expiry_date = datetime.strptime(expiry, '%d-%m-%Y')
    # Format as '25JUN'
                new_date_string = expiry_date.strftime('%y%b').upper()
                fyers_fut_symbol = f"NSE:{symbol}{new_date_string}FUT"
            except ValueError as e:
                print(f"[ERROR] Failed to parse expiry for symbol {symbol}: {expiry}. Error: {e}")
                fyers_fut_symbol = None



            symbol_dict = {
                "Symbol": symbol, 
                "OptionType": row['OptionType'],
                "unique_key": f"{symbol}_{row['OptionType']}_",
                "Expiry": expiry,   
                "TickSize": tick_size,  # Add tick size to symbol dictionary
                "lot_size": lot_size,
                "Quantity": int(row['Quantity']) * lot_size,
                "OptionExpiery":OptionExpiery,
                # "Timeframe": int(row['FyersTf']),
                "MA1": int(row['MA1']),
                "MA2": int(row['MA2']),
                # "expiryDate":expiry_api_format,
                "StartTime": datetime.strptime(row["StartTime"], "%H:%M:%S").time(),
                "StopTime": datetime.strptime(row["Stoptime"], "%H:%M:%S").time(),
                # "SquareOffTime": datetime.strptime(row["SquareOffTime"], "%H:%M").time(),
                "PlacementType": row['PlacementType'],
                "StrikeStep": row['StrikeStep'],
                "Trade":None,
                "StopLoss":float(row['StopLoss']),
                "StoplossValue":None,
                "Target":float(row['Target']),
                "TargetValue":None,
                "SquareOffExecuted":False,
                # "NSEFOexchangeInstrumentID": NSEFOinstrument_id,
                "Series": None, "Candletimestamp": None,
                "FyersTf": row['FyersTf'],
                # "FyresSymbol": f"NSE:{symbol}-EQ",
                "FyresLtp": None,
                "FyersFutSymbol": fyers_fut_symbol,"FyersFutLtp": None,"FutAsk": None,"FutBid": None,
                "Ce_Contract_Token":None,
                "Pe_Contract_Token":None,
                "ce_contract":None,
                "pe_contract":None,
                "passfuturecontract":None,
                "passfuturecontract_token":NSEFOinstrument_id,
                "Flexiblity":row['Flexiblity'],"CandleTimestamp":None,"option_tick_size":None
                

            }

            result_dict[symbol_dict["unique_key"]] = symbol_dict

           

            if NSEFOinstrument_id:
                Future_instrument_id_list.append({
                    "exchangeSegment": 2,
                    "exchangeInstrumentID": NSEFOinstrument_id
                })

          
            FyerSymbolList.append(symbol_dict["FyersFutSymbol"])


        print("result_dict: ", result_dict)
        print("-" * 50)
        print("Future_instrument_id_list: ", Future_instrument_id_list)
        print("-" * 50)
        print("Equity_instrument_id_list: ", Equity_instrument_id_list)
        print("-" * 50)

    except Exception as e:
        print("Error happened in fetching symbol", str(e))


def get_api_credentials():
    credentials = {}
    try:
        df = pd.read_csv('Credentials.csv')
        for index, row in df.iterrows():
            title = row['Title']
            value = row['Value']
            credentials[title] = value
    except pd.errors.EmptyDataError:
        print("The CSV file is empty or has no data.")
    except FileNotFoundError:
        print("The CSV file was not found.")
    except Exception as e:
        print("An error occurred while reading the CSV file:", str(e))
    return credentials

def login_interactive_api():
    global xt
    credentials = get_api_credentials()
    interactive_app_key = credentials.get("Interactive_App_Key")
    interactive_app_secret = credentials.get("Interactive_App_Secret")

    print("interactive_app_key: ", interactive_app_key)
    print("interactive_app_secret: ", interactive_app_secret)

    if not interactive_app_key or not interactive_app_secret:
        print("Missing Interactive API credentials in Credentials.csv")
        return None

    try:
        # Create XTSConnect object with correct parameters
        xt = XTSConnect(
            apiKey=interactive_app_key, 
            secretKey=interactive_app_secret, 
            source="WEBAPI", 
            root="https://smpc.jainam.in:14543/dashboard#!/login"
        )
        response = xt.interactive_login()
        print("Interactive Login Response:", response)

        if response and 'result' in response and 'token' in response['result']:
            print("Interactive login successful")
            # Start the net position fetcher thread
            return xt
        else:
            print("Interactive login failed: ", response)
            return None
    except Exception as e:
        print(f"Error during interactive login: {str(e)}")
        import traceback
        traceback.print_exc()
        return None



def login_marketdata_api():
    """
    Login to the Market Data API and return the XTSConnect object.
    """
    global xts_marketdata

    try:
        credentials = get_api_credentials()
        source = "WEBAPI"
        market_data_app_key = credentials.get("Market_Data_API_App_Key")
        market_data_app_secret = credentials.get("Market_Data_API_App_Secret")
        print("market_data_app_key: ", market_data_app_key)
        print("market_data_app_secret: ", market_data_app_secret)
        print("source: ", source)

        if not market_data_app_key or not market_data_app_secret:
            print("Missing Market Data API credentials in Credentials.csv")
            return None

        # Use the correct root URL (do NOT include /apimarketdata)
        xts_marketdata = XTSConnect(
            apiKey=market_data_app_key,
            secretKey=market_data_app_secret,
            source=source,
            root="https://smpc.jainam.in:14543"
        )

        print("xts_marketdata object created: ", xts_marketdata)
        response = xts_marketdata.marketdata_login()
        print("Market Data Login Response:", response)
        print("Market Data Login Response type: ", type(xts_marketdata))

        if response and response.get('type') == 'success':
            print("Market Data login successful")
            return xts_marketdata
        else:
            print("Market Data login failed: ", response)
            return None
        

    except Exception as e:
        print(f"Error during market data login: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def write_to_order_logs(message):
    with open('OrderLog.txt', 'a') as file:  # Open the file in append mode
        file.write(message + '\n')

def main_strategy():
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=10)

        start_time_str = start_date.strftime("%b %d %Y 090000")
        end_time_str = end_date.strftime("%b %d %Y 153000")

        now = datetime.now()
        now_time = now.time()
        UpdateData()
        time.sleep(1)
        fetch_start = time.time()

        for unique_key, params in result_dict.items():
            # initialize loop-specific variables to avoid UnboundLocalError
            symbol_name = params["FyersFutSymbol"]
            TIMESTAMP = datetime.now()
            now = datetime.now()
            now_time = now.time()

            if (now_time >= params.get("StopTime") and 
                not params.get("SquareOffExecuted", False) and 
                (params.get("Trade") == "BUY" or params.get("Trade") == "SELL") and
                params.get("FyersFutLtp") is not None ):
                
                params["SquareOffExecuted"] = True
                
                params["StoplossValue"] = None

                if params["Trade"] == "BUY":
                    if params["PlacementType"] == "FUTURETOOPTION":
                        if params["Flexiblity"]=="PREMIUMBUY":
                            place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],
                            order_side="SELL",price=get_bid(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            message = f"{TIMESTAMP} [EXIT] Square up time reached {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)
                        
                        if params["Flexiblity"]=="PREMIUMSELL":
                            place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            message = f"{TIMESTAMP} [EXIT]  Square up time reached {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)
                        
                        if params["Flexiblity"]=="SYNTHETIC":
                            place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            message = f"{TIMESTAMP} [EXIT]  Square up time reached  {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)
                            message = f"{TIMESTAMP} [EXIT]  Square up time reached  {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)

                    if params["PlacementType"] == "FUTURETOFUTURE":
                        place_order(nfo_ins_id=params["passfuturecontract_token"],symbol=params["passfuturecontract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["passfuturecontract_token"]),unique_key=None,ticksize=params["TickSize"])
                        message = f"{TIMESTAMP} Sqaure up executed for {params['Quantity']} @ {params['FyersFutLtp']} {params['passfuturecontract']} , Stoploss: {params['StoplossValue']} , Target: {params['TargetValue']}"
                        print(message)
                        write_to_order_logs(message)
                
                if params["Trade"] == "SELL":
                    if params["PlacementType"] == "FUTURETOOPTION":
                        if params["Flexiblity"]=="PREMIUMBUY":
                            place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            message = f"{TIMESTAMP} [EXIT] Square up time reached {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)
                        
                        if params["Flexiblity"]=="PREMIUMSELL":
                            place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            message = f"{TIMESTAMP} [EXIT] Square up time reached {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)
                        
                        if params["Flexiblity"]=="SYNTHETIC":
                            place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            message = f"{TIMESTAMP} [EXIT] Square up time reached {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)
                            message = f"{TIMESTAMP} [EXIT] Square up time reached {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)

                    if params["PlacementType"] == "FUTURETOFUTURE":
                        place_order(nfo_ins_id=params["passfuturecontract_token"],symbol=params["passfuturecontract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["passfuturecontract_token"]),unique_key=None,ticksize=params["TickSize"])
                        message = f"{TIMESTAMP} Sqaure up executed for {params['Quantity']} @ {params['FyersFutLtp']} {params['passfuturecontract']} , Stoploss: {params['StoplossValue']} , Target: {params['TargetValue']}"
                        print(message)
                        write_to_order_logs(message)

                params["Trade"] = "nomoretrades"
            
            # SECOND: If outside trading hours, skip strategy processing
            if not (params["StartTime"] <= now_time <= params["StopTime"]):
                continue


            # print("ltp: ",get_ltp(53001))
            # print("ask: ",get_ask(53001))
            # print("bid: ",get_bid(53001))

            print(
                f"\n[STATUS UPDATE] {params['Symbol']} | PlacementType={params['PlacementType']} | Flexibility={params['Flexiblity']}\n"
                f"   Trade State     : {params.get('Trade')}\n"
                f"   Future LTP      : {params.get('FyersFutLtp')}\n"
                f"   CE Token        : {params.get('Ce_Contract_Token')} | CE LTP: {get_ltp(params['Ce_Contract_Token']) if params.get('Ce_Contract_Token') else 'NA'}\n"
                f"   PE Token        : {params.get('Pe_Contract_Token')} | PE LTP: {get_ltp(params['Pe_Contract_Token']) if params.get('Pe_Contract_Token') else 'NA'}\n"
                f"   Stoploss Value  : {params.get('StoplossValue')}\n"
                f"   Target Value    : {params.get('TargetValue')}\n"
                f"   Entry Price     : {params.get('EntryPrice') if params.get('EntryPrice') else 'NA'}\n"
                f"   Stoploss        : {params.get('StopLoss')}\n"
                f"   Target          : {params.get('Target')}\n"
                # f"   PnL (approx)    : "
                # f"{round((params['FyersFutLtp'] - params['EntryPrice']) * params['Quantity'], 2) if params.get('EntryPrice') and params.get('FyersFutLtp') else 'NA'}\n"
                f"   Start Time      : {params.get('StartTime')} | Stop Time: {params.get('StopTime')} | SquareOff: {params.get('SquareOffExecuted')}\n"
            )


            # deleat later

            # place_order(params["passfuturecontract_token"],params["lot_size"],"BUY",params["FyersFutLtp"],unique_key=None)
            # Fetch FUTSTK instrument ID
            

            # print("celtp: ", celtp)
            # print("peltp: ", peltp)
            

            # deleat later

            fyersData=fetchOHLC(symbol_name,params["FyersTf"])
            # print("fyersData columns: ", fyersData.columns.tolist())
            # print("fyersData shape: ", fyersData.shape)
            
            fyersData_polars=convert_to_polars(fyersData)
            # print("fyersData_polars columns: ", fyersData_polars.columns)
            # print("fyersData_polars shape: ", fyersData_polars.shape)

            # Calculate EMA using the 'close' column
            try:
                fyersData_polars = fyersData_polars.with_columns([
                    pl.col("close").ta.ema(
                        timeperiod=int(params["MA1"])
                    ).alias("ema")
                ])
                # print(f"EMA calculated successfully with period {params['EmaPeriod']}")
            except Exception as e:
                print(f"Error calculating EMA: {e}")
                # Fallback: add a column with NaN values
                fyersData_polars = fyersData_polars.with_columns([
                    pl.lit(None).alias("ema")
                ])
            
            try:
                fyersData_polars = fyersData_polars.with_columns([
                    pl.col("close").ta.ema(
                        timeperiod=int(params["MA2"])
                    ).alias("ema2")
                ])
                # print(f"EMA calculated successfully with period {params['EmaPeriod']}")
            except Exception as e:
                print(f"Error calculating EMA: {e}")
                # Fallback: add a column with NaN values
                fyersData_polars = fyersData_polars.with_columns([
                    pl.lit(None).alias("ema2")
                ])
            
            fyersData_polars.write_csv(f"fyersData_polars{params['Symbol']}.csv")

            # print("fyersData_polars: ", fyersData_polars)
            
            
            Last4Candles=fyersData_polars.tail(4)
            dates_list = Last4Candles["date"].to_list()
            closes_list = Last4Candles["close"].to_list()
            opens_list = Last4Candles["open"].to_list()
            highs_list = Last4Candles["high"].to_list()
            lows_list = Last4Candles["low"].to_list()
            ema_list = Last4Candles["ema"].to_list()
            ema2_list = Last4Candles["ema2"].to_list()


            lastcandletime = dates_list[-1]
            SecondLastCandleTime = dates_list[-2]
            ThirdLastCandleTime = dates_list[-3]
            FourthLastCandleTime = dates_list[-4]


            # lastcandleclose = closes_list[-1]
            # SecondLastCandleClose = closes_list[-2]
            # ThirdLastCandleClose = closes_list[-3]
            
            # lastcandleopen = opens_list[-1]
            # SecondLastCandleOpen = opens_list[-2]
            # ThirdLastCandleOpen = opens_list[-3]
          

            # lastcandlehigh = highs_list[-1]
            # SecondLastCandleHigh = highs_list[-2]
            # ThirdLastCandleHigh = highs_list[-3]
          

            # lastcandlelow = lows_list[-1]
            # SecondLastCandleLow = lows_list[-2]
            # ThirdLastCandleLow = lows_list[-3]
           

            lastcandleema = ema_list[-1]
            SecondLastCandleEma = ema_list[-2]
            ThirdLastCandleEma = ema_list[-3]

            lastcandleema2 = ema2_list[-1]
            SecondLastCandleEma2 = ema2_list[-2]
            ThirdLastCandleEma2 = ema2_list[-3]


            # deleat later

            # place_order(params["passfuturecontract_token"],params["Quantity"],"BUY",params["FyersFutLtp"],unique_key=None)
            # Fetch FUTSTK instrument ID
            
            

            # deleat later

# BUY
            if (
                ThirdLastCandleEma<=ThirdLastCandleEma2 and
                 SecondLastCandleEma>SecondLastCandleEma2 and (params["Trade"] == None or params["Trade"] == "SELL") and 
                 params["CandleTimestamp"] != lastcandletime
                ):

                
                params["CandleTimestamp"] = lastcandletime
                message = f"{TIMESTAMP} [ENTRY] Buy in future Contract {params['Symbol']} {params['FyersFutLtp']} "
                print(message)
                write_to_order_logs(message)
                
                if params["PlacementType"] == "FUTURETOOPTION":
                    
                    if params["Flexiblity"]=="PREMIUMBUY":
                        if params["Trade"] == "SELL":
                            place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            message = f"{TIMESTAMP} [EXIT] Sell {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)

                        strike = normalize_to_step(params["FyersFutLtp"], params["StrikeStep"])
                        opt_series = 'OPTIDX' if params['Symbol'] in ('NIFTY', 'BANKNIFTY') else 'OPTSTK'
                        fut_response = xts_marketdata.get_option_symbol(
                        exchangeSegment=2,  # NSEFO
                        series=opt_series,
                        symbol=params['Symbol'],
                        expiryDate=params["OptionExpiery"],
                        optionType='CE',
                        strikePrice=strike
                        )
                        if fut_response['type'] == 'success' and 'result' in fut_response:
                            result_item = fut_response['result'][0]
                            params['Ce_Contract_Token'] = int(result_item['ExchangeInstrumentID'])
                            tick_size = float(result_item.get('TickSize', 0.0))  # Extract tick size from future response
                            params['option_tick_size']=tick_size
                        else:
                            print(f"[ERROR] Could not get FUTSTK instrument ID for {params['Symbol']} {params['expiryDate']}")
                        
                        params['ce_contract'] = f"NSE:{symbol_name}-{strike}-CE"

                        ce_ask = get_ask(params["Ce_Contract_Token"])
                        params["StoplossValue"] = ce_ask - params["Stoploss"]
                        params["TargetValue"] = ce_ask + params["Target"]
                        
                        place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="BUY",price=ce_ask,unique_key=None,ticksize=params["option_tick_size"])
                        message = f"{TIMESTAMP} [ENTRY] Buy {params['ce_contract']} {params['Quantity']} @ {ce_ask} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                    
                    if params["Flexiblity"]=="PREMIUMSELL":
                        if params["Trade"] == "SELL":
                            place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])  
                            message = f"{TIMESTAMP} [EXIT] Buy {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)

                        strike = normalize_to_step(params["FyersFutLtp"], params["StrikeStep"])
                        opt_series = 'OPTIDX' if params['Symbol'] in ('NIFTY', 'BANKNIFTY') else 'OPTSTK'
                        fut_response = xts_marketdata.get_option_symbol(
                        exchangeSegment=2,  # NSEFO
                        series=opt_series,
                        symbol=params['Symbol'],
                        expiryDate=params["OptionExpiery"],
                        optionType='PE',
                        strikePrice=strike
                        )
                        if fut_response['type'] == 'success' and 'result' in fut_response:
                            result_item = fut_response['result'][0]
                            params['Pe_Contract_Token'] = int(result_item['ExchangeInstrumentID'])
                            tick_size = float(result_item.get('TickSize', 0.0))  # Extract tick size from future response
                            params['option_tick_size']=tick_size
                        else:
                            print(f"[ERROR] Could not get FUTSTK instrument ID for {params['Symbol']} {params['expiryDate']}")
                        params['pe_contract'] = f"NSE:{symbol_name}-{strike}-PE"

                        pe_bid = get_bid(params["Pe_Contract_Token"])
                        params["StoplossValue"] = pe_bid + params["Stoploss"]
                        params["TargetValue"] = pe_bid - params["Target"]
                        
                        place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        message = f"{TIMESTAMP} [ENTRY] Sell {params['pe_contract']} {params['Quantity']} @ {pe_bid} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)

                    if params["Flexiblity"]=="SYNTHETIC":
                        if params["Trade"] == "SELL":
                            place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            message = f"{TIMESTAMP} [EXIT] Buy {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)
                            message = f"{TIMESTAMP} [EXIT] Sell {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)

                        strike = normalize_to_step(params["FyersFutLtp"], params["StrikeStep"])
                        opt_series = 'OPTIDX' if params['Symbol'] in ('NIFTY', 'BANKNIFTY') else 'OPTSTK'
                        fut_response = xts_marketdata.get_option_symbol(
                        exchangeSegment=2,  # NSEFO
                        series=opt_series,
                        symbol=params['Symbol'],
                        expiryDate=params["OptionExpiery"],
                        optionType='CE',
                        strikePrice=strike
                        )
                        if fut_response['type'] == 'success' and 'result' in fut_response:
                            result_item = fut_response['result'][0]
                            params['Ce_Contract_Token'] = int(result_item['ExchangeInstrumentID'])
                            tick_size = float(result_item.get('TickSize', 0.0))  # Extract tick size from future response
                            params['option_tick_size']=tick_size
                        else:
                            print(f"[ERROR] Could not get FUTSTK instrument ID for {params['Symbol']} {params['expiryDate']}")
                        
                        opt_series = 'OPTIDX' if params['Symbol'] in ('NIFTY', 'BANKNIFTY') else 'OPTSTK'
                        fut_response = xts_marketdata.get_option_symbol(
                        exchangeSegment=2,  # NSEFO
                        series=opt_series,
                        symbol=params['Symbol'],
                        expiryDate=params["OptionExpiery"],
                        optionType='PE',
                        strikePrice=strike
                        )
                        if fut_response['type'] == 'success' and 'result' in fut_response:
                            result_item = fut_response['result'][0]
                            params['Pe_Contract_Token'] = int(result_item['ExchangeInstrumentID'])
                            tick_size = float(result_item.get('TickSize', 0.0))  # Extract tick size from future response
                            params['option_tick_size']=tick_size
                        else:
                            print(f"[ERROR] Could not get FUTSTK instrument ID for {params['Symbol']} {params['expiryDate']}")
                        

                        params['ce_contract'] = f"NSE:{symbol_name}-{strike}-CE"
                        params['pe_contract'] = f"NSE:{symbol_name}-{strike}-PE"
                        place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        
                        params["StoplossValue"] = params["FyersFutLtp"] - params["Stoploss"]
                        params["TargetValue"] = params["FyersFutLtp"] + params["Target"]

                        message = f"{TIMESTAMP} [ENTRY] Buy {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                        message = f"{TIMESTAMP} [ENTRY] Sell {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)

                if params["PlacementType"] == "FUTURETOFUTURE":
                    if params["Trade"] == "SELL":
                        place_order(nfo_ins_id=params["passfuturecontract_token"],symbol=params["passfuturecontract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["passfuturecontract_token"]),unique_key=None,ticksize=params["TickSize"])
                        message = f"{TIMESTAMP} [EXIT] Buy {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)

                    passfuturecontract=params["FyersFutSymbol"]
                    params["passfuturecontract"]=params["FyersFutSymbol"]
                    params["StoplossValue"] = params["FyersFutLtp"] - params["Stoploss"]
                    params["TargetValue"] = params["FyersFutLtp"] + params["Target"]
                    place_order(nfo_ins_id=params["passfuturecontract_token"],symbol=params["passfuturecontract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["passfuturecontract_token"]),unique_key=None,ticksize=params["TickSize"])
                    message = f"{TIMESTAMP} [ENTRY] Buy {params['Quantity']} @ {params['FyersFutLtp']} {params['passfuturecontract']} , Stoploss: {params['StoplossValue']} , Target: {params['TargetValue']}"
                    print(message)
                    write_to_order_logs(message)
                
                params["Trade"]="BUY"
                


            # sell

            if (
                ThirdLastCandleEma>=ThirdLastCandleEma2 and
                 SecondLastCandleEma<SecondLastCandleEma2 and (params["Trade"] == None or params["Trade"] == "BUY") and 
                 params["CandleTimestamp"] != lastcandletime
                ):
                
                
                params["CandleTimestamp"] = lastcandletime

                message = f"{TIMESTAMP} [ENTRY] Sell in future Contract {params['Symbol']} {params['FyersFutLtp']} "
                print(message)
                write_to_order_logs(message)
                
                if params["PlacementType"] == "FUTURETOOPTION":
                    if params["Flexiblity"]=="PREMIUMBUY":
                        if params["Trade"] == "BUY":
                            place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            message = f"{TIMESTAMP} [EXIT] Sell {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)
                        
                        strike = normalize_to_step(params["FyersFutLtp"], params["StrikeStep"])
                        opt_series = 'OPTIDX' if params['Symbol'] in ('NIFTY', 'BANKNIFTY') else 'OPTSTK'
                        fut_response = xts_marketdata.get_option_symbol(
                        exchangeSegment=2,  # NSEFO
                        series=opt_series,
                        symbol=params['Symbol'],
                        expiryDate=params["OptionExpiery"],
                        optionType='PE',
                        strikePrice=strike
                        )
                        if fut_response['type'] == 'success' and 'result' in fut_response:
                            result_item = fut_response['result'][0]
                            params['Pe_Contract_Token'] = int(result_item['ExchangeInstrumentID'])
                            tick_size = float(result_item.get('TickSize', 0.0))  # Extract tick size from future response
                            params['option_tick_size']=tick_size
                        else:
                            print(f"[ERROR] Could not get FUTSTK instrument ID for {params['Symbol']} {params['expiryDate']}")
                        
                        params['pe_contract'] = f"NSE:{symbol_name}-{strike}-PE"

                        pe_ask = get_ask(params["Pe_Contract_Token"])
                        params["StoplossValue"] = pe_ask - params["Stoploss"]
                        params["TargetValue"] = pe_ask + params["Target"]
                        
                        place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        message = f"{TIMESTAMP} [ENTRY] Buy {params['pe_contract']} {params['Quantity']} @ {pe_ask} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                    
                    if params["Flexiblity"]=="PREMIUMSELL":
                        if params["Trade"] == "BUY":
                            place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            message = f"{TIMESTAMP} [EXIT] Buy {params['pe_contract']} {params['Quantity']} @ {get_ask(params["Pe_Contract_Token"])} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)
                        
                        strike = normalize_to_step(params["FyersFutLtp"], params["StrikeStep"])
                        opt_series = 'OPTIDX' if params['Symbol'] in ('NIFTY', 'BANKNIFTY') else 'OPTSTK'
                        fut_response = xts_marketdata.get_option_symbol(
                        exchangeSegment=2,  # NSEFO
                        series=opt_series,
                        symbol=params['Symbol'],
                        expiryDate=params["OptionExpiery"],
                        optionType='CE',
                        strikePrice=strike
                        )
                        if fut_response['type'] == 'success' and 'result' in fut_response:
                            result_item = fut_response['result'][0]
                            params['Ce_Contract_Token'] = int(result_item['ExchangeInstrumentID'])
                            tick_size = float(result_item.get('TickSize', 0.0))  # Extract tick size from future response
                            params["option_tick_size"]=tick_size
                        else:
                            print(f"[ERROR] Could not get FUTSTK instrument ID for {params['Symbol']} {params['expiryDate']}")
                        
                        params['ce_contract'] = f"NSE:{symbol_name}-{strike}-CE"
                        ce_bid = get_bid(params["Ce_Contract_Token"])
                        params["StoplossValue"] = ce_bid + params["Stoploss"]
                        params["TargetValue"] = ce_bid - params["Target"]
                        
                        place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        message = f"{TIMESTAMP} [ENTRY] Sell {params['ce_contract']} {params['Quantity']} @ {ce_bid} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                    
                    if params["Flexiblity"]=="SYNTHETIC":
                        if params["Trade"] == "BUY":
                            place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                            message = f"{TIMESTAMP} [EXIT] Sell {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)
                            message = f"{TIMESTAMP} [EXIT] Buy {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                            print(message)
                            write_to_order_logs(message)

                        strike = normalize_to_step(params["FyersFutLtp"], params["StrikeStep"])
                        opt_series = 'OPTIDX' if params['Symbol'] in ('NIFTY', 'BANKNIFTY') else 'OPTSTK'
                        fut_response = xts_marketdata.get_option_symbol(
                        exchangeSegment=2,  # NSEFO
                        series=opt_series,
                        symbol=params['Symbol'],
                        expiryDate=params["OptionExpiery"],
                        optionType='PE',
                        strikePrice=strike
                        )
                        if fut_response['type'] == 'success' and 'result' in fut_response:
                            result_item = fut_response['result'][0]
                            params['Pe_Contract_Token'] = int(result_item['ExchangeInstrumentID'])
                            tick_size = float(result_item.get('TickSize', 0.0))  # Extract tick size from future response
                            params["option_tick_size"]=tick_size

                        else:
                            print(f"[ERROR] Could not get FUTSTK instrument ID for {params['Symbol']} {params['expiryDate']}")
                        
                        opt_series = 'OPTIDX' if params['Symbol'] in ('NIFTY', 'BANKNIFTY') else 'OPTSTK'
                        fut_response = xts_marketdata.get_option_symbol(
                        exchangeSegment=2,  # NSEFO
                        series=opt_series,
                        symbol=params['Symbol'],
                        expiryDate=params["OptionExpiery"],
                        optionType='CE',
                        strikePrice=strike
                        )
                        if fut_response['type'] == 'success' and 'result' in fut_response:
                            result_item = fut_response['result'][0]
                            params['Ce_Contract_Token'] = int(result_item['ExchangeInstrumentID'])
                            tick_size = float(result_item.get('TickSize', 0.0))  # Extract tick size from future response
                            params['option_tick_size']=tick_size
                            params["option_tick_size"]=tick_size
                        else:
                            print(f"[ERROR] Could not get FUTSTK instrument ID for {params['Symbol']} {params['expiryDate']}")
                        
                        params['ce_contract'] = f"NSE:{symbol_name}-{strike}-CE"
                        params['pe_contract'] = f"NSE:{symbol_name}-{strike}-PE"
                        place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        
                        params["StoplossValue"] = params["FyersFutLtp"] + params["Stoploss"]
                        params["TargetValue"] = params["FyersFutLtp"] - params["Target"]
                        message = f"{TIMESTAMP} [ENTRY] Sell {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                        message = f"{TIMESTAMP} [ENTRY] Buy {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)

                    

                if params["PlacementType"] == "FUTURETOFUTURE":
                    if params["Trade"] == "BUY":
                        place_order(nfo_ins_id=params["passfuturecontract_token"],symbol=params["passfuturecontract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["passfuturecontract_token"]),unique_key=None,ticksize=params["TickSize"])
                        message = f"{TIMESTAMP} [EXIT] Sell {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)

                    passfuturecontract=params["FyersFutSymbol"]
                    params["passfuturecontract"]=params["FyersFutSymbol"]
                    params["StoplossValue"] = params["FyersFutLtp"] + params["Stoploss"]
                    params["TargetValue"] = params["FyersFutLtp"] - params["Target"]
                    place_order(nfo_ins_id=params["passfuturecontract_token"],symbol=params["passfuturecontract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["passfuturecontract_token"]),unique_key=None,ticksize=params["TickSize"])
                    message = f"{TIMESTAMP} Sell {params['Quantity']} @ {params['FyersFutLtp']} {passfuturecontract} , Stoploss: {params['StoplossValue']} , Target: {params['TargetValue']}"
                    print(message)
                    write_to_order_logs(message)
                
                params["Trade"]="SELL"
            
            # stoploss and target reached buy 

            if params["Trade"] == "BUY":
                if params["PlacementType"] == "FUTURETOOPTION" and params["Flexiblity"]=="PREMIUMBUY":
                    ce_bid = get_bid(params["Ce_Contract_Token"])
                    if ce_bid is not None and params['TargetValue'] is not None and ce_bid >= params['TargetValue'] :
                        place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        message = f"{TIMESTAMP} [EXIT] Target reached {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                        params['TargetValue']=None
                    
                    if ce_bid is not None and params['StoplossValue'] is not None and ce_bid <= params['StoplossValue'] :
                        place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        message = f"{TIMESTAMP} [EXIT] Stoploss reached {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                        params['StoplossValue']=None
                
                if params["PlacementType"] == "FUTURETOOPTION" and params["Flexiblity"]=="PREMIUMSELL":
                    pe_ask = get_ask(params["Pe_Contract_Token"])
                    if pe_ask is not None and params['TargetValue'] is not None and pe_ask <= params['TargetValue'] :
                        place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        message = f"{TIMESTAMP} [EXIT] Target reached {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                        params['TargetValue']=None

                    if pe_ask is not None and params['StoplossValue'] is not None and pe_ask >= params['StoplossValue'] :
                        place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        message = f"{TIMESTAMP} [EXIT] Stoploss reached {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                        params['StoplossValue']=None
                
                if params["PlacementType"] == "FUTURETOOPTION" and params["Flexiblity"]=="SYNTHETIC" and params["FyersFutLtp"] >=params["TargetValue"]:
                    place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                    place_order(nfo_ins_id=params["Pe_Contract_Token"] ,symbol=params["pe_contract"] ,order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                    message = f"{TIMESTAMP} [EXIT] Target reached {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                    print(message)
                    write_to_order_logs(message)
                    message = f"{TIMESTAMP} [EXIT] Target reached {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                    print(message)
                    write_to_order_logs(message)
                    params['TargetValue']=None
                
                if params["PlacementType"] == "FUTURETOOPTION" and params["Flexiblity"]=="SYNTHETIC" and params["FyersFutLtp"] <=params["StoplossValue"]:
                    place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                    place_order(nfo_ins_id=params["Pe_Contract_Token"] ,symbol=params["pe_contract"] ,order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                    message = f"{TIMESTAMP} [EXIT] Stoploss reached {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                    print(message)
                    write_to_order_logs(message)
                    message = f"{TIMESTAMP} [EXIT] Stoploss reached {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                    print(message)
                    write_to_order_logs(message)
                    params['StoplossValue']=None
                
                if params["PlacementType"] == "FUTURETOFUTURE" and params["FyersFutLtp"] >=params["TargetValue"]:
                    place_order(nfo_ins_id=params["passfuturecontract_token"],symbol=params["passfuturecontract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["passfuturecontract_token"]),unique_key=None,ticksize=params["TickSize"])
                    message = f"{TIMESTAMP} {params['Symbol']} Target reached. SELL {params['Quantity']} @ {params['FyersFutLtp']}"
                    print(message)
                    write_to_order_logs(message)
                    params['TargetValue']=None
                
                if params["PlacementType"] == "FUTURETOFUTURE" and params["FyersFutLtp"] <=params["StoplossValue"]:
                    place_order(nfo_ins_id=params["passfuturecontract_token"],symbol=params["passfuturecontract"],order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["passfuturecontract_token"]),unique_key=None,ticksize=params["TickSize"])
                    message = f"{TIMESTAMP} {params['Symbol']} Stoploss reached. Sell {params['Quantity']} @ {params['FyersFutLtp']}"
                    print(message)
                    write_to_order_logs(message)
                    params['StoplossValue']=None
                
            # stoploss and target reached sell 

            if params["Trade"] == "SELL":
                if params["PlacementType"] == "FUTURETOOPTION" and params["Flexiblity"]=="PREMIUMBUY":
                    pe_bid = get_bid(params["Pe_Contract_Token"])
                    if pe_bid is not None and params['TargetValue'] is not None and pe_bid >= params['TargetValue'] :
                        place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_bid(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        message = f"{TIMESTAMP} [EXIT] Target reached {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                        params['TargetValue']=None
                    
                    if pe_bid is not None and params['StoplossValue'] is not None and pe_bid <= params['StoplossValue'] :
                        place_order(nfo_ins_id=params["Pe_Contract_Token"],symbol=params["pe_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_bid(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        message = f"{TIMESTAMP} [EXIT] Stoploss reached {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                        params['StoplossValue']=None
                
                if params["PlacementType"] == "FUTURETOOPTION" and params["Flexiblity"]=="PREMIUMSELL":
                    ce_ask = get_ask(params["Ce_Contract_Token"])
                    if ce_ask is not None and params['TargetValue'] is not None and ce_ask <= params['TargetValue'] :
                        place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        message = f"{TIMESTAMP} [EXIT] Target reached {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                        params['TargetValue']=None
                        
                    if ce_ask is not None and params['StoplossValue'] is not None and ce_ask >= params['StoplossValue'] :
                        place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                        message = f"{TIMESTAMP} [EXIT] Stoploss reached {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                        print(message)
                        write_to_order_logs(message)
                        params['StoplossValue']=None
                        
                if params["PlacementType"] == "FUTURETOOPTION" and params["Flexiblity"]=="SYNTHETIC" and params["FyersFutLtp"] <=params["TargetValue"]:
                    place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                    place_order(nfo_ins_id=params["Pe_Contract_Token"] ,symbol=params["pe_contract"] ,order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                    message = f"{TIMESTAMP} [EXIT] Target reached {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                    print(message)
                    write_to_order_logs(message)
                    message = f"{TIMESTAMP} [EXIT] Target reached {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                    print(message)
                    write_to_order_logs(message)
                    params['TargetValue']=None
                
                if params["PlacementType"] == "FUTURETOOPTION" and params["Flexiblity"]=="SYNTHETIC" and params["FyersFutLtp"] >=params["StoplossValue"]:
                    place_order(nfo_ins_id=params["Ce_Contract_Token"],symbol=params["ce_contract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["Ce_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                    place_order(nfo_ins_id=params["Pe_Contract_Token"] ,symbol=params["pe_contract"] ,order_quantity=params["Quantity"],order_side="SELL",price=get_bid(params["Pe_Contract_Token"]),unique_key=None,ticksize=params["option_tick_size"])
                    message = f"{TIMESTAMP} [EXIT] Stoploss reached {params['ce_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                    print(message)
                    write_to_order_logs(message)
                    message = f"{TIMESTAMP} [EXIT] Stoploss reached {params['pe_contract']} {params['Quantity']} @ {params['FyersFutLtp']} ,flexiblity= {params['Flexiblity']} "
                    print(message)
                    write_to_order_logs(message)
                    params['StoplossValue']=None
                
                if params["PlacementType"] == "FUTURETOFUTURE" and params["FyersFutLtp"] >=params["TargetValue"]:
                    place_order(nfo_ins_id=params["passfuturecontract_token"],symbol=params["passfuturecontract"],order_quantity=params["Quantity"],order_side="BUY",price=get_bid(params["passfuturecontract_token"]),unique_key=None,ticksize=params["TickSize"])
                    message = f"{TIMESTAMP} {params['Symbol']} Target reached. Buy {params['Quantity']} @ {params['FyersFutLtp']}"
                    print(message)
                    write_to_order_logs(message)
                    params['TargetValue']=None
                
                if params["PlacementType"] == "FUTURETOFUTURE" and params["FyersFutLtp"] <=params["StoplossValue"]:
                    place_order(nfo_ins_id=params["passfuturecontract_token"],symbol=params["passfuturecontract"],order_quantity=params["Quantity"],order_side="BUY",price=get_ask(params["passfuturecontract_token"]),unique_key=None,ticksize=params["TickSize"])
                    message = f"{TIMESTAMP} {params['Symbol']} Stoploss reached. BUY {params['Quantity']} @ {params['FyersFutLtp']}"
                    print(message)
                    write_to_order_logs(message)
                    params['StoplossValue']=None
                


                
    except Exception as e:
        print("Error in main strategy:", str(e))
        traceback.print_exc()


if __name__ == "__main__":
    # # Initialize settings and credentials
    #   # <-- Add this line
    credentials_dict_fyers = get_api_credentials_Fyers()
    redirect_uri = credentials_dict_fyers.get('redirect_uri')
    client_id = credentials_dict_fyers.get('client_id')
    secret_key = credentials_dict_fyers.get('secret_key')
    grant_type = credentials_dict_fyers.get('grant_type')
    response_type = credentials_dict_fyers.get('response_type')
    state = credentials_dict_fyers.get('state')
    TOTP_KEY = credentials_dict_fyers.get('totpkey')
    FY_ID = credentials_dict_fyers.get('FY_ID')
    PIN = credentials_dict_fyers.get('PIN')
    # Automated login and initialization steps
    automated_login(client_id=client_id, redirect_uri=redirect_uri, secret_key=secret_key, FY_ID=FY_ID,
                                     PIN=PIN, TOTP_KEY=TOTP_KEY)
    
    get_api_credentials()
    xts_marketdata = login_marketdata_api()
    print("xts_marketdata: ", xts_marketdata)
    login_interactive_api()
    get_user_settings()
    # fetch_MarketQuote(xts_marketdata)

    
    # Initialize Market Data API
    fyres_websocket(FyerSymbolList)
    time.sleep(5)
    
    while True:
        now =   datetime.now()   
        print(f"\nStarting main strategy at {datetime.now()}")
        main_strategy()
        time.sleep(1)
