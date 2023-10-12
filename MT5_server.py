#!/usr/bin/env python
# coding:utf-8

#FIXME add caffeine for windows (caffeine MT5? or windows ?)
#caffeinMe(pid=osGetpid())

from time import sleep
from datetime import datetime
from os.path import join as osPathJoin
from uuid import uuid4
from asyncio import get_running_loop as asyncioGet_running_loop, sleep as asyncioSleep, \
                    start_server as asyncioStart_server, run as asyncioRun, Lock as asyncioLock
import MetaTrader5 as mt5


# relative import
from sys import path;path.extend("..")
from common.config import Config
from common.MyLogger.my_logger import MyLogger
from common.Database.database import Database
from common.ThreadQs.routing_rules import LostQmsg
from common.Helpers.helpers import load_config_files, caffeinMe
from common.Helpers.os_helpers import is_process_running, start_independant_process
from common.Helpers.network_helpers import SafeAsyncSocket


##############################################################################################################################################################################################
# MT5 windows function : https://www.mql5.com/en/docs/python_metatrader5

# initialize : Establish a connection with the MetaTrader 5 terminal
# login : Connect to a trading account using specified parameters
# shutdown : Close the previously established connection to the MetaTrader 5 terminal
# version : Return the MetaTrader 5 terminal version
# last_error : Return data on the last error

# account_info : Get info on the current trading account
# terminal_Info : Get status and parameters of the connected MetaTrader 5 terminal

# symbols_total : Get the number of all financial instruments in the MetaTrader 5 terminal
# symbols_get : Get all financial instruments from the MetaTrader 5 terminal
# symbol_info : Get data on the specified financial instrument
# symbol_info_tick : Get the last tick for the specified financial instrument
# symbol_select : Select a symbol in the MarketWatch window or remove a symbol from the window

# market_book_add : Subscribes the MetaTrader 5 terminal to the Market Depth change events for a specified symbol
# market_book_get : Returns a tuple from BookInfo featuring Market Depth entries for the specified symbol
# market_book_release : Cancels subscription of the MetaTrader 5 terminal to the Market Depth change events for a specified symbol

# copy_rates_from : Get bars from the MetaTrader 5 terminal starting from the specified date
# copy_rates_from_pos : Get bars from the MetaTrader 5 terminal starting from the specified index
# copy_rates_range : Get bars in the specified date range from the MetaTrader 5 terminal
# copy_ticks_from : Get ticks from the MetaTrader 5 terminal starting from the specified date
# copy_ticks_range : Get ticks for the specified date range from the MetaTrader 5 terminal

# orders_total : Get the number of active orders.
# orders_get : Get active orders with the ability to filter by symbol or ticket
# order_calc_margin : Return margin in the account currency to perform a specified trading operation
# order_calc_profit : Return profit in the account currency for a specified trading operation
# order_check : Check funds sufficiency for performing a required trading operation
# order_send : Send a request to perform a trading operation.

# positions_total : Get the number of open positions
# positions_get : Get open positions with the ability to filter by symbol or ticket
# history_orders_total : Get the number of orders in trading history within the specified interval
# history_orders_get : Get orders from trading history with the ability to filter by ticket or position
# history_deals_total : Get the number of deals in trading history within the specified interval
# history_deals_get : Get deals from trading history with the ability to filter by ticket or position


# MT5 only works with windows
MT5_SERVER_NAME = "MT5_server"
MT5_PROCESS_NAME = "terminal64.exe"
MT5_PROCESS_EXE = osPathJoin("C:\\", "Program Files", "MetaTrader 5" , MT5_PROCESS_NAME)

asyncLoop = None ; asyncSleep = 0.1

lockTaskList = asyncioLock() ; taskList = {}
lockQuoteCache = asyncioLock() ; quoteCache = {}

accounts_infos = {}
async_tcp_server = None


class Qmsg:
    def __init__(self, msg, frome, too, ackw=None, priority=False):
        self.id = uuid4()
        self.msg = msg
        self.frome = frome
        self.too = too
        self.ackw = ackw
        self.priority = priority

##############################################################################################################################################################################################
# MT5 commands
async def stream_cache(symbol, quote):
    global lockQuoteCache ; global quoteCache
    async with lockQuoteCache:
        quoteCache[symbol] = quote

async def stream_task(task, task_name, save:bool=False, delete:bool=False):
    global lockTaskList ; global taskList ; global logger
    # save == delete have to be different
    async with lockTaskList:
        if save: 
            try:
                taskList[task_name] = task
            except Exception as e:
                task.cancel()
                await logger.asyncError("{0} : stream_task error while trying to register task '{1}' : {2}".format(MT5_SERVER_NAME, task_name, e))
        else:
            try:
                task = taskList.pop(task_name)
                task.cancel()
            except Exception as e:
                await logger.asyncError("{0} : stream_task error while trying to cancel task '{1}' : {2}".format(MT5_SERVER_NAME, task_name, e))

async def async_stream(asyncSock, symbol):
    cptErr = 0
    while True:
        try:
            tick_data = mt5.symbol_info_tick(symbol)._asdict()
            await asyncSock.send_data(tick_data)
            await stream_cache(symbol=symbol, quote=tick_data)
            #await logger.asyncInfo("{0} : async_stream received data for '{1}' : {2}".format(MT5_SERVER_NAME, symbol, tick_data))
            cptErr = 0
        except Exception as e:
            await logger.asyncError("{0} : async_stream error while trying to get or send quote '{1}' : {2}".format(MT5_SERVER_NAME, symbol, e))
            cptErr+=1 
            if cptErr > 3000: break # more than 5 minutes
            continue
        await asyncioSleep(asyncSleep)

def get_info(symbol):
    pass

async def buy(orderID, symbol, volume, price=None, sl=None, tp=None, deviation=None):
    global lockQuoteCache
    return await mt5.order_send(
        { 
            "action": mt5.TRADE_ACTION_DEAL,                    # immediat buy
            "symbol": symbol, 
            "volume": volume,                                   # lot size currency 100 000 so 1 000 = 0.01
            "type": mt5.ORDER_TYPE_BUY, 
            "price": price if not price is None 
                     else lockQuoteCache[symbol].ask ,          # buy price, if not provided get last ASK quote
            "sl": sl,                                           # stop loss    doc example => price + 100 * pip,
            "tp": tp,                                           # take profit   doc example => price + 100 * pip,
            "deviation": deviation,                             # Fill order if price has changed between deviation..               
            "magic": orderID,                                   # UUID in QMsg 
            "comment": "python script open",            
            "type_time": mt5.ORDER_TIME_GTC,                    # MT5 time
            "type_filling": mt5.ORDER_FILLING_IOC               # Fill or Kill
        }
    ) 

async def sell(orderID, symbol, volume, price=None, sl=None, tp=None, deviation=None):
    global lockQuoteCache
    return await mt5.order_send(
        { 
            "action": mt5.TRADE_ACTION_DEAL,                    # immediat sell
            "symbol": symbol, 
            "volume": volume,                                   # lot size currency 100 000 so 1 000 = 0.01
            "type": mt5.ORDER_TYPE_SELL, 
            "price": price if not price is None 
                     else lockQuoteCache[symbol].bid ,          # sell price, if not provided get last BID quote
            "sl": sl,                                           # stop loss    doc example => price + 100 * pip,
            "tp": tp,                                           # take profit   doc example => price + 100 * pip,
            "deviation": deviation,                             # Fill order if price has changed between deviation..               
            "magic": orderID,                                   # UUID in QMsg 
            "comment": "python script open",            
            "type_time": mt5.ORDER_TIME_GTC,                    # MT5 time
            "type_filling": mt5.ORDER_FILLING_IOC               # Fill or Kill
        }
    ) 


async def aMT5_funcs(asyncSock, deserialized_data:Qmsg, clientName="MT5 receiver"):
    global asyncLoop ; global MT5_SERVER_NAME ; global config ; global logger
    try:    
        MT5_command = deserialized_data.msg.split('|')
        if MT5_command[0] == "QUOTE":
            await stream_task(task=asyncLoop.create_task(async_stream(asyncSock=asyncSock, symbol=MT5_command[1])), task_name="{0}-{1}".format(MT5_command[0], MT5_command[1]), save=True)
            return "coucou"
        if MT5_command[0] == "BUY":
            return await sell()
        if MT5_command[0] == "SELL":
            return await buy()
        else:
            LostQmsg(deserialized_data)
            await logger.asyncError("{0} : error unknown message received : '{1}'".format(clientName, MT5_command))
    except Exception as e:
        await logger.asyncCritical("{0} : aMT5_funcs error while trying to route Qmsg '{1}' : {2}".format(clientName, deserialized_data, e))
        return None

##############################################################################################################################################################################################
# Async TCP gateway
async def handle_TCP_client(reader, writer):
    global MT5_SERVER_NAME ; global config ; global logger
    asyncSock = SafeAsyncSocket(reader=reader, writer=writer)
    data = await asyncSock.receive_data()
    if not data:
        asyncSock.writer.close()
        await asyncSock.writer.wait_closed()
        return
    clientName, host, port = data.split(':') ; port = int(port)      
    await logger.asyncInfo("{0} : '{1}' has established connection without encryption from '{2}' destport '{3}'".format(MT5_SERVER_NAME, clientName, host, port))
    while True:
        data = await asyncSock.receive_data()
        if not data:
            break
        response = await aMT5_funcs(asyncSock=asyncSock, deserialized_data=data)
        if not response is None:
            await asyncSock.send_data(response)
            await logger.asyncInfo("{0} : response '{1}' send to '{2}'".format(MT5_SERVER_NAME, response, clientName))
    asyncSock.writer.close()
    await asyncSock.writer.wait_closed()

########################################################################################################################################
# init MT5 app module
# log accounts
def auto_login():
    global MT5_SERVER_NAME ; global config ; global logger
    for broker, conf in config.parser.items():
        if broker not in ("COMMON", "MT5", "DEFAULT"):
            account=False; pwd=None
            for name_sec, conf_sec in conf.items(): 
                if name_sec == "ACCOUNT": account = conf_sec
                if name_sec == "PASSWORD": pwd = conf_sec
            try:
                login(account=account, pwd=pwd)
            except Exception as e:
                logger.error("{0} : error while trying to log account : '{1}-{2}' : {3}".format(MT5_SERVER_NAME, broker, account, e))
                continue

def login(account, pwd=None):
    global MT5_SERVER_NAME ; global config ; global logger
    try:
        if pwd == None:
            if mt5.login(account):
                logger.info("{0} : account '{1}' connected ! account information => {2}".format(MT5_SERVER_NAME, mt5.account_info()))
        else:
            if mt5.login(account, password=pwd):
                logger.info("{0} : account '{1}' connected ! account information => {2}".format(MT5_SERVER_NAME, mt5.account_info()))
        accounts_infos[account] = mt5.account_info()._asdict()
    except:
        logger.error("{0} : response '{1}' send to '{2}'".format(MT5_SERVER_NAME, mt5.last_error()))

def init_MT5():
    global async_tcp_server ; global config ; global logger
    # launch MT5 app, if not already running
    if not is_process_running(cmdlinePatt=MT5_PROCESS_EXE, processName=MT5_PROCESS_NAME):
        logger.info("{0} : MetaTrader5 app is starting.. .  .".format(MT5_SERVER_NAME, datetime.utcnow()))
        start_independant_process(MT5_PROCESS_EXE)
        logger.info("{0} : MetaTrader5 app has been started at '{1}'".format(MT5_SERVER_NAME, datetime.utcnow()))
    else:         
        logger.info("{0} : MetaTrader5 app is already runnnig...".format(MT5_SERVER_NAME, datetime.utcnow()))
    # initialize MT5 module    
    nbTry = 0
    while True:
        sleep(1)
        try:
            mt5.initialize()
            break
        except Exception as e:
            nbTry+=1
            if nbTry < 10: 
                continue
            else:
                logger.error("{0} : failed to initialize MT5 connection ({1} times) : {2}".format(MT5_SERVER_NAME, nbTry, e))
                exit(1)
    # connect accounts    
    try:
        auto_login()
    except Exception as e:
        logger.error("{0} : error while trying to initialize MT5 connection : {1}".format(MT5_SERVER_NAME, e))
        #logger.error("{0} : last_error : '{1}'".format(MT5_SERVER_NAME, mt5.last_error()))
        try: mt5.shutdown()
        except: pass
        exit(1)
    else:
        logger.info("{0} : connected to MT5 application version '{1}'".format(MT5_SERVER_NAME, mt5.version()))
        #logger.info("{0} : MT5 terminal infos : {1}".format(MT5_SERVER_NAME, mt5.terminal_info()))

########################################################################################################################################
# main async server
async def run_async_tcp_server():

    init_MT5()

    global async_tcp_server ; global asyncLoop ; global config ; global logger
    asyncLoop = asyncioGet_running_loop()

    try:
        async_TCP_IP = '127.0.0.1' ; async_TCP_port = int(config.parser["MT5"]["MT5_TCP_PORT"])
        async_tcp_server = await asyncioStart_server(handle_TCP_client, async_TCP_IP, async_TCP_port)
        await logger.asyncInfo("{0} : socket async TCP handler is open : {1}, srcport {2}".format(MT5_SERVER_NAME, async_TCP_IP, async_TCP_port))
        async with async_tcp_server:
            await async_tcp_server.serve_forever()
    except KeyboardInterrupt:
        await logger.asyncInfo("{0} : has been stopped manually at '{1}'".format(MT5_SERVER_NAME, datetime.utcnow()))
        try: mt5.shutdown()
        except: pass
    except Exception as e:
        await logger.asyncError("{0} : error while trying to start TCP server : '{1}'".format(MT5_SERVER_NAME, e))
        try: mt5.shutdown()
        except: pass
        exit(1)

def start_MT5_server(name=None):
    if not name is None:
        global MT5_SERVER_NAME
        MT5_SERVER_NAME = name
    asyncioRun(run_async_tcp_server())


#================================================================
if __name__ == "__main__":
    from sys import argv
    configStr = "MT5"

    if len(argv) > 1:
        MT5_SERVER_NAME = argv[1]
        if len(argv) >= 3:
            configStr = argv[2]  
        if len(argv) > 3:
            config = Config(name=MT5_SERVER_NAME, config_file_path=load_config_files()[configStr], ignore_config_server=True)
            logger = MyLogger(name=MT5_SERVER_NAME, config=config, enable_notifications=False, notify_from_log_server=False, start_telecommand=False, log_server=False) 
            dblog = Database(logger, config)
            # late binding
            logger.DBlog = dblog
            logger.error("{0} : error while trying to start TCP server, too much parameters provided {1} expected max expected 2 : {2}".format(MT5_SERVER_NAME, len(argv), argv))
            exit(1)
        

    config = Config(name=MT5_SERVER_NAME, config_file_path=load_config_files()[configStr], ignore_config_server=True)
    logger = MyLogger(name=MT5_SERVER_NAME, config=config, enable_notifications=False, notify_from_log_server=False, start_telecommand=False, log_server=False) 
    dblog = Database(logger, config)
    # late binding
    logger.DBlog = dblog
    asyncioRun(run_async_tcp_server())




