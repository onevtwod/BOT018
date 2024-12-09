from datetime import UTC, datetime, timedelta, timezone
from cybotrade.strategy import Strategy as BaseStrategy
from cybotrade.models import (
    Exchange,
    OrderSide,
    RuntimeConfig,
    RuntimeMode,
    Position,
    PositionData,
    Symbol,
)
import numpy as np
import asyncio
import logging
import colorlog
from logging.handlers import TimedRotatingFileHandler
from cybotrade.permutation import Permutation
import pytz
import util
from dotenv import dotenv_values
import sys

config = dotenv_values(".env")
runtime_mode = RuntimeMode.Backtest
init_position_from_db = False # Changed to False for testing
place_order_exchange = Exchange.BybitLinear

TELEGRAM_CHAT_ID = config["TELEGRAM_CHAT_ID"]
TELEGRAM_TOKEN = config["TELEGRAM_TOKEN"]

import json
from pybit.unified_trading import HTTP

pybit_mode = False
if runtime_mode == RuntimeMode.LiveTestnet:
    pybit_mode = True
with open("./credentials.json") as file:
    credentials = json.load(file)
api_key = credentials[0]["api_key"]
api_secret = credentials[0]["api_secret"]
bybit_client = HTTP(
    testnet=pybit_mode,
    api_key=api_key,
    api_secret=api_secret,
)


class Strategy(BaseStrategy):
    stop_loss_percentage = 0.4
    # Indicator params
    bot_sr = 2.3
    leverage = 1.5
    bot_id = "btc_coinbase_premium_index_gap_threshold_1h"
    parameter_json_id = "sr_table"
    multiplier = 0.14
    rolling_window = 330
    db_params = {
        "database": config["DB_NAME"],
        "user": config["DB_USER_NAME"],
        "password": config["DB_PASSWORD"],
        "host": config["DB_HOST"],
        "port": config["DB_PORT"],
        "sslmode": "allow",
    }
    renew_sr_table_time = datetime.now(pytz.timezone("UTC"))
    qty_precision = 3
    price_precision = 1
    min_qty = 0.001
    pair = Symbol(base="BTC", quote="USDT")
    long_short_data = PositionData(quantity=0.0, avg_price=0.0)
    position = Position(pair, long_short_data, long_short_data, updated_time=0)
    total_pnl = 0.0
    entry_time = datetime.now(pytz.timezone("UTC"))
    renew_time_gap_in_mins = 60
    need_to_cut_loss = False
    is_continue = True

    def __init__(self):
        handler = colorlog.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(f"%(log_color)s{Strategy.LOG_FORMAT}")
        )
        file_handler = TimedRotatingFileHandler(
            "y_coinbase_pi_gap_1h_threshold-livetestnet.log", when="h", backupCount=30
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(Strategy.LOG_FORMAT))
        super().__init__(log_level=logging.INFO, handlers=[handler, file_handler])
        # if (
        #     init_position_from_db
        #     and runtime_mode != RuntimeMode.Backtest
        #     and runtime_mode != RuntimeMode.LiveTestnet
        # ):
        #     position_data = util.get_position_from_db(
        #         self.db_params, self.bot_id, self.pair
        #     )
        #     self.position = position_data[0]
        #     self.entry_time = position_data[1]
        # self.leverage, self.renew_sr_table_time = util.get_leverage_from_db(
        #     self.db_params, self.bot_id, self.renew_time_gap_in_mins
        # )
        logging.info(
            f"Position after init: {util.get_position_info(self.position, self.entry_time)}"
        )
        symbol_info = util.get_bybit_symbol_info(
            symbol=self.pair.base + self.pair.quote
        )
        self.qty_precision = symbol_info.qty_precision
        self.price_precision = symbol_info.price_precision
        self.min_qty = symbol_info.min_qty
        logging.info(f"symbol_info: {symbol_info}")
        mess = "Started bot: " + self.bot_id
        util.send_notification(
            message=mess,
            chat_id=TELEGRAM_CHAT_ID,
            token=TELEGRAM_TOKEN,
        )

    async def set_param(self, identifier, value):
        logging.info(f"Setting {identifier} to {value}")
        if identifier == "rolling_window":
            self.rolling_window = int(value)
        elif identifier == "multiplier":
            self.multiplier = float(value)
        else:
            logging.error(f"Could not set {identifier}, not found")

    async def on_order_update(self, strategy, update):
        update = update

    async def on_active_order_interval(self, strategy, active_orders):
        if self.need_to_cut_loss == True:
            wallet_info = util.get_wallet_balance(client=bybit_client)
            wallet_balance = wallet_info.total_wallet_balance
            available_balance = wallet_info.available_balance
            if available_balance / wallet_balance <= self.stop_loss_percentage:
                logging.info(
                    f"Drawdown more than {self.stop_loss_percentage}, stop the BOT!!!"
                )
                self.is_continue = False
                if self.position.long.quantity != 0.0:
                    await util.open_short_position_market(
                        strategy=strategy,
                        place_order_exchange=place_order_exchange,
                        symbol=self.pair,
                        qty=self.position.long.quantity,
                        current_price=self.position.long.avg_price,
                        start_time=datetime.now(pytz.timezone("UTC")).timestamp()
                        * 1000,
                    )
                elif self.position.short.quantity != 0.0:
                    await util.open_long_position_market(
                        strategy=strategy,
                        place_order_exchange=place_order_exchange,
                        symbol=self.pair,
                        qty=self.position.short.quantity,
                        current_price=self.position.short.avg_price,
                        start_time=datetime.now(pytz.timezone("UTC")).timestamp()
                        * 1000,
                    )
                sys.exit()
        # if (
        #     datetime.now(pytz.timezone("UTC")) >= self.renew_sr_table_time
        #     and runtime_mode != RuntimeMode.Backtest
        #     and runtime_mode != RuntimeMode.LiveTestnet
        # ):
        #     self.leverage, self.renew_sr_table_time = util.get_leverage_from_db(
        #         self.db_params, self.bot_id, self.renew_time_gap_in_mins
        #     )

    async def on_datasource_interval(self, strategy, topic, data_list):
        coinbase_pi_data = self.data_map[topic]
        start_time = np.array(
            list(map(lambda c: float(c["start_time"]), coinbase_pi_data))
        )
        coinbase_pi_gap = np.array(
            list(map(lambda c: float(c["coinbase_premium_gap"]), coinbase_pi_data))
        )
        now_utc = datetime.now(pytz.timezone("UTC"))
        closed_hour_time = now_utc - timedelta(hours=1)
        closed_hour_time = closed_hour_time.replace(minute=0, second=0, microsecond=0)
        closed_hour_time_ts = closed_hour_time.timestamp() * 1000
        if (
            closed_hour_time_ts != start_time[-1]
            and runtime_mode != RuntimeMode.Backtest
        ):
            logging.info(
                f"Latest hour should get: {closed_hour_time}, timestamp: {closed_hour_time_ts}, datasource last data start_time: {start_time[-1]}, {util.convert_ms_to_datetime(start_time[-1])}"
            )
            return
        logging.info(
            f"Latest hour should get: {closed_hour_time}, timestamp: {closed_hour_time_ts}, coinbase_pi_gap: {coinbase_pi_gap[-1]}, first_start_time: {util.convert_ms_to_datetime(start_time[0])}, prev_start_time: {util.convert_ms_to_datetime(start_time[-2])} at {util.convert_ms_to_datetime(start_time[-1])}"
        )
        coinbase_pi_gap = coinbase_pi_gap[-self.rolling_window :]
        sma = np.mean(coinbase_pi_gap)
        if sma > 0.0:
            upper_sma_threshold = sma * (1.0 + self.multiplier)
            lower_sma_threshold = sma * (1.0 - self.multiplier)
        else:
            upper_sma_threshold = sma * (1.0 - self.multiplier)
            lower_sma_threshold = sma * (1.0 + self.multiplier)
        latest_pi_gap = coinbase_pi_gap[-1]
        current_price = await strategy.get_current_price(
            symbol=self.pair, exchange=place_order_exchange
        )
        logging.info(
            f"current total_pnl: {self.total_pnl}, current position: {util.get_position_info(self.position, self.entry_time)}, current_price: {current_price}, coinbase_premium_gap: {latest_pi_gap}, sma: {sma}, sma_with_multiplier_up: {upper_sma_threshold}, sma_with_multiplier_down: {lower_sma_threshold} at {util.convert_ms_to_datetime(start_time[-1])}"
        )
        if self.is_continue == False:
            return
        # try:
        #     wallet_balance = await strategy.get_current_available_balance(
        #         exchange=place_order_exchange, symbol=self.pair
        #     )
        # except Exception as e:
        #     logging.error(f"Failed to fetch wallet balance: {e}")
        wallet_info = util.get_wallet_balance(client=bybit_client)
        wallet_balance = wallet_info.total_wallet_balance
        ori_qty = util.get_qty(
            current_price,
            self.qty_precision,
            self.min_qty,
            wallet_balance,
            self.leverage,
        )
        if (
            self.position.long.quantity == 0.0
            and coinbase_pi_gap[-1] > upper_sma_threshold
        ):
            is_close_short = False
            if self.position.short.quantity != 0.0:
                is_close_short = True
                pnl = (self.position.short.avg_price - current_price) * abs(
                    self.position.short.quantity
                )
                self.total_pnl += pnl
                logging.info(
                    f"Closed a sell position with qty {abs(self.position.short.quantity)}, pnl: {pnl}, total_pnl: {self.total_pnl} when current_price: {current_price}, coinbase_premium_gap: {latest_pi_gap}, sma: {sma}, sma_with_multiplier_up: {upper_sma_threshold}, sma_with_multiplier_down: {lower_sma_threshold} at {util.convert_ms_to_datetime(start_time[-1])}"
                )
                qty = round(self.position.short.quantity + ori_qty, self.qty_precision)
            else:
                qty = ori_qty
            await util.open_long_position_market(
                strategy=strategy,
                place_order_exchange=place_order_exchange,
                symbol=self.pair,
                qty=qty,
                current_price=current_price,
                start_time=start_time[-1],
            )
            self.position = Position(
                self.pair,
                PositionData(quantity=ori_qty, avg_price=current_price),
                PositionData(quantity=0.0, avg_price=0.0),
            )
            self.entry_time = util.convert_ms_to_datetime(start_time[-1])
            if is_close_short == True:
                msg_type = "close_short_and_open_long"
            else:
                msg_type = "open_long"
            util.send_telegram_msg(
                telegram_chat_id=TELEGRAM_CHAT_ID,
                telegram_token=TELEGRAM_TOKEN,
                msg_type=msg_type,
                bot_id=self.bot_id,
                qty=qty,
                price=current_price,
                pair=self.pair,
                total_pnl=self.total_pnl,
                position=self.position,
                entry_time=self.entry_time,
            )
        elif (
            self.position.short.quantity == 0.0
            and coinbase_pi_gap[-1] < lower_sma_threshold
        ):
            is_close_long = False
            if self.position.long.quantity != 0.0:
                is_close_long = True
                pnl = (current_price - self.position.long.avg_price) * abs(
                    self.position.long.quantity
                )
                self.total_pnl += pnl
                logging.info(
                    f"Closed a buy position with qty {self.position.long.quantity}, pnl: {pnl}, total_pnl: {self.total_pnl} when current_price: {current_price}, coinbase_premium_gap: {latest_pi_gap}, sma: {sma}, sma_with_multiplier_up: {upper_sma_threshold}, sma_with_multiplier_down: {lower_sma_threshold} at {util.convert_ms_to_datetime(start_time[-1])}"
                )
                qty = round(self.position.long.quantity + ori_qty, self.qty_precision)
            else:
                qty = ori_qty
            await util.open_short_position_market(
                strategy=strategy,
                place_order_exchange=place_order_exchange,
                symbol=self.pair,
                qty=qty,
                current_price=current_price,
                start_time=start_time[-1],
            )
            self.position = Position(
                self.pair,
                PositionData(quantity=0.0, avg_price=0.0),
                PositionData(quantity=ori_qty, avg_price=current_price),
            )
            self.entry_time = util.convert_ms_to_datetime(start_time[-1])
            if is_close_long == True:
                msg_type = "close_long_and_open_short"
            else:
                msg_type = "open_short"
            util.send_telegram_msg(
                telegram_chat_id=TELEGRAM_CHAT_ID,
                telegram_token=TELEGRAM_TOKEN,
                msg_type=msg_type,
                bot_id=self.bot_id,
                qty=qty,
                price=current_price,
                pair=self.pair,
                total_pnl=self.total_pnl,
                position=self.position,
                entry_time=self.entry_time,
            )

    def on_shutdown(self):
        logging.info(f"Trigger shutdown function")
        # if (
        #     (self.position.long.quantity > 0 or self.position.short.quantity > 0)
        #     and runtime_mode != RuntimeMode.Backtest
        #     and runtime_mode != RuntimeMode.LiveTestnet
        # ):
        #     logging.info(
        #         f"Bot get shutdown, insert the position: {util.get_position_info(self.position, self.entry_time)} to db"
        #     )
        #     util.store_position_when_shutdown(
        #         self.db_params, self.bot_id, self.pair, self.entry_time, self.position
        #     )


config = RuntimeConfig(
    mode=runtime_mode,
    datasource_topics=[
        "cryptoquant|1h|btc/market-data/coinbase-premium-index?window=hour"
    ],
    active_order_interval=60,
    initial_capital=1000000.0,
    candle_topics=["candles-1h-BTC/USDT-bybit"],
    start_time=datetime(2023, 6, 1, 0, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
    api_key=config["CYBOTRADE_API_KEY"],
    api_secret=config["CYBOTRADE_SECRET_KEY"],
    data_count=500,
    exchange_keys="./credentials.json",
)

permutation = Permutation(config)
hyper_parameters = {}
hyper_parameters["rolling_window"] = [310]
hyper_parameters["multiplier"] = [0.24]
# hyper_parameters["rolling_window"] = [10]
# hyper_parameters["multiplier"] = [0.0]


async def start_backtest():
    await permutation.run(hyper_parameters, Strategy)


asyncio.run(start_backtest())
