import logging
import math
import time
import numpy as np
from cybotrade.models import (
    OrderSide,
    Symbol,
    Position,
    PositionData,
    OrderUpdate,
    OrderParams,
    RuntimeMode,
)
from datetime import UTC, datetime, timedelta, timezone
import pytz
import requests
from decimal import Decimal
import pandas as pd
import json
import psycopg2
from functools import reduce
from dataclasses import dataclass
from pybit.unified_trading import HTTP

import warnings

# Suppress the specific FutureWarning
warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    message="'T' is deprecated and will be removed in a future version, please use 'min' instead.",
)

LONG = "Long"
SHORT = "Short"
NOTHING = "Nothing"


@dataclass
class SymbolInfo:
    qty_precision: int
    price_precision: int
    symbol: str
    min_qty: float


@dataclass
class WalletBalanceInfo:
    available_balance: float
    total_wallet_balance: float


def get_wallet_balance(client):
    current_wallet_balance_info = WalletBalanceInfo(
        available_balance=0,
        total_wallet_balance=0,
    )
    try:
        wallet_balance = client.get_wallet_balance(
            accountType="UNIFIED",
            coin="USDT",
        )
        data = wallet_balance["result"]["list"][0]
        available_balance = data["totalAvailableBalance"]
        total_wallet_balance = data["totalWalletBalance"]
        current_wallet_balance_info = WalletBalanceInfo(
            available_balance=float(available_balance),
            total_wallet_balance=float(total_wallet_balance),
        )
        return current_wallet_balance_info
    except Exception as e:
        logging.error(
            f"[WALLET_BALANCE] Failed to fetch wallet balance from bybit: {e}"
        )
        return current_wallet_balance_info


def convert_ms_to_datetime(milliseconds):
    seconds = milliseconds / 1000.0
    return datetime.fromtimestamp(seconds, tz=UTC)


def get_position_from_db(db_params, bot_id, pair):
    # Create a connection
    conn = psycopg2.connect(**db_params)
    # Create a cursor object
    cur = conn.cursor()
    # Execute SQL query to fetch data
    cur.execute(f"SELECT * FROM position WHERE bot_id = '{bot_id}';")
    # Fetch all rows from the last executed statement
    rows = cur.fetchall()
    position = Position(
        pair,
        PositionData(quantity=0.0, avg_price=0.0),
        PositionData(quantity=0.0, avg_price=0.0),
    )
    entry_time = datetime.now(pytz.timezone("UTC"))
    for row in rows:
        if row[0] == bot_id:
            logging.info(f"Position in db: {row}")
            if row[4] == LONG:
                position = Position(
                    pair,
                    PositionData(quantity=row[3], avg_price=row[2]),
                    PositionData(quantity=0.0, avg_price=0.0),
                )
            elif row[4] == SHORT:
                position = Position(
                    pair,
                    PositionData(quantity=0.0, avg_price=0.0),
                    PositionData(quantity=row[3], avg_price=row[2]),
                )
            entry_time = convert_ms_to_datetime(row[5])
    # Close the connection
    conn.close()
    return [position, entry_time]


def get_leverage_from_db(db_params, bot_id, renew_time_gap_in_mins):
    # Create a connection
    conn = psycopg2.connect(**db_params)
    # Create a cursor object
    cur = conn.cursor()
    # Execute SQL query to fetch data
    cur.execute(f"SELECT * FROM leverage WHERE bot_id = '{bot_id}';")
    # Fetch all rows from the last executed statement
    rows = cur.fetchall()
    leverage = 1.0
    for row in rows:
        if row[0] == bot_id:
            logging.info(f"Leverage in db: {row}")
            leverage = row[1]
    # Close the connection
    conn.close()
    renew_sr_table_time = datetime.now(pytz.timezone("UTC")) + timedelta(
        minutes=renew_time_gap_in_mins
    )
    logging.info(f"latest {bot_id} leverage: {leverage}")
    return leverage, renew_sr_table_time


def get_sr_table_from_db(
    current_sr_balance_percentage,
    current_sr_no_of_strategy,
    sr_table_db_params,
    parameter_json_id,
    bot_sr,
    renew_time_gap_in_mins=60,
    is_first_time=False,
):
    is_connected = False
    try:
        while is_connected == False:
            try:
                conn = psycopg2.connect(**sr_table_db_params)
                is_connected = True
            except Exception as e:
                if is_first_time == False:
                    is_connected = True
                    renew_sr_table_time = datetime.now(
                        pytz.timezone("UTC")
                    ) + timedelta(minutes=renew_time_gap_in_mins)
                    return [
                        current_sr_balance_percentage,
                        current_sr_no_of_strategy,
                        renew_sr_table_time,
                    ]
                else:
                    time.sleep(1)
                    logging.error(f"Failed to fetch sr table: {e}")
        # Create a cursor object
        cur = conn.cursor()
        # Execute SQL query to fetch data
        cur.execute(
            f"SELECT * FROM parameter WHERE parameter_json_id = '{parameter_json_id}';"
        )
        # Fetch all rows from the last executed statement
        rows = cur.fetchall()
        sr_table_info = get_sr_table_info(bot_sr, rows[0])
        logging.info(f"Latest sr table info: {sr_table_info}")
        current_sr_balance_percentage = sr_table_info[0]
        current_sr_no_of_strategy = sr_table_info[1]
        renew_sr_table_time = datetime.now(pytz.timezone("UTC")) + timedelta(
            minutes=renew_time_gap_in_mins
        )
        # Close the connection
        conn.close()

        return [
            current_sr_balance_percentage,
            current_sr_no_of_strategy,
            renew_sr_table_time,
        ]
    except Exception as e:
        logging.error(f"Failed to fetch sr table: {e}")
        renew_sr_table_time = datetime.now(pytz.timezone("UTC")) + timedelta(
            minutes=renew_time_gap_in_mins
        )
        return [
            current_sr_balance_percentage,
            current_sr_no_of_strategy,
            renew_sr_table_time,
        ]


async def get_order_book(strategy, exchange, pair):
    try:
        ob = await strategy.get_order_book(exchange=exchange, symbol=pair)
        best_bid = ob.bids[0].price
        best_ask = ob.asks[0].price
        logging.info(f"best_bid: {best_bid}, best_ask: {best_ask}")
        return [best_bid, best_ask]
    except Exception as e:
        logging.error(f"Failed to fetch order book: {e}")
        best_bid = 0.0
        best_ask = 0.0
        for i in range(0, 5):
            try:
                ob = await strategy.get_order_book(exchange=exchange, symbol=pair)
                best_bid = ob.bids[0].price
                best_ask = ob.asks[0].price
                logging.info(f"best_bid: {best_bid}, best_ask: {best_ask}")
                break
            except Exception as e:
                time.sleep(0.0001)
                logging.error(f"Failed to fetch order book: {e}")
        return [best_bid, best_ask]


async def check_open_order(strategy, exchange, pair, order_pool):
    if len(order_pool) != 0:
        logging.info(
            f"Checking order_pool {order_pool} order by calling get_open_order from exchange"
        )
        try:
            open_order = await strategy.get_open_orders(exchange, pair)
            for limit in order_pool:
                have_order = False
                for open_od in open_order:
                    if limit[0] == open_od.client_order_id:
                        have_order = True
                        limit[4] = True
                if have_order == False and datetime.now(timezone.utc) >= limit[
                    1
                ] + timedelta(minutes=5):
                    order_pool.remove(limit)
                    logging.info(
                        f"Removed {limit} from order_pool due to order is not in open_order from exchange and more than 5mins, current order_pool: {order_pool}"
                    )
            return order_pool
        except Exception as e:
            logging.error(f"Failed to fetch all open orders: {e}")
            return order_pool
    else:
        return order_pool


def get_mean(array):
    total = np.sum(array)
    return total / len(array)


def get_stddev(array):
    return np.std(array, ddof=1)


def get_rolling_mean(array, rolling_window):
    cumsum = np.zeros(len(array) + rolling_window)
    cumsum[rolling_window:] = np.cumsum(array)
    rolling_sum = cumsum[rolling_window:] - cumsum[:-rolling_window]
    rolling_mean = rolling_sum / rolling_window
    rolling_mean[: rolling_window - 1] = 0
    return np.nan_to_num(rolling_mean)


def get_rolling_std(array, rolling_window):
    array = np.array(array)
    cumsum = np.zeros(len(array) + rolling_window)
    cumsum_sq = np.zeros(len(array) + rolling_window)
    cumsum[rolling_window:] = np.cumsum(array)
    cumsum_sq[rolling_window:] = np.cumsum(array**2)
    rolling_sum = cumsum[rolling_window:] - cumsum[:-rolling_window]
    rolling_sum_sq = cumsum_sq[rolling_window:] - cumsum_sq[:-rolling_window]
    mean = rolling_sum / rolling_window
    variance = (rolling_sum_sq - rolling_window * mean**2) / (rolling_window - 1)
    rolling_std = np.sqrt(variance)
    rolling_std[: rolling_window - 1] = 0
    return np.nan_to_num(rolling_std, neginf=0, posinf=0, nan=0)


def get_rolling_zscore(array, mean, stdev):
    rolling_zscore = (array - mean) / stdev
    remove_nan_rolling_zscore = np.nan_to_num(rolling_zscore, neginf=0, posinf=0, nan=0)
    return remove_nan_rolling_zscore


def get_rolling_sum(array, rolling_window):
    cumsum = np.zeros(len(array) + rolling_window)
    cumsum[rolling_window:] = np.cumsum(array)
    rolling_sum = cumsum[rolling_window:] - cumsum[:-rolling_window]
    rolling_sum[: rolling_window - 1] = 0
    return rolling_sum


def get_rolling_historical_volatility(array, rolling_window, sr_multiplier):
    cumulative_returns = array[1:] / array[:-1] - 1.0
    cumulative_returns = np.insert(cumulative_returns, 0, 0)
    cumsum = np.zeros(len(array) + rolling_window)
    cumsum_sq = np.zeros(len(array) + rolling_window)
    cumsum[rolling_window:] = np.cumsum(cumulative_returns)
    cumsum_sq[rolling_window:] = np.cumsum(cumulative_returns**2)
    rolling_sum = cumsum[rolling_window:] - cumsum[:-rolling_window]
    rolling_sum_sq = cumsum_sq[rolling_window:] - cumsum_sq[:-rolling_window]
    mean = rolling_sum / rolling_window
    variance = (rolling_sum_sq - rolling_window * mean**2) / (rolling_window - 1)
    rolling_hv = np.sqrt(variance) * np.sqrt(365 * sr_multiplier)
    rolling_hv[: rolling_window - 1] = 0
    return np.nan_to_num(rolling_hv, neginf=0, posinf=0, nan=0)


def calculate_ema(data, window):
    multiplier = 2 / (window + 1)  # EMA multiplier
    # Calculate the initial SMA (Simple Moving Average)
    ema_values = [0.0] * (window - 1)
    sma = np.mean(data[:window])
    ema_values.append(sma)

    # Calculate the EMA for the remaining data points
    for i in range(window, len(data)):
        ema = (data[i] - ema_values[-1]) * multiplier + ema_values[-1]
        ema_values.append(ema)

    return np.array(ema_values)


def get_rolling_vwap(high, low, close, volume, rolling_window):
    # Calculate HLC3 (High + Low + Close) multiplied by volume
    hlc3_vol = (high + low + close) / 3 * volume
    cumsum_hlc3_vol = np.zeros(len(close) + rolling_window)
    cumsum_vol = np.zeros(len(close) + rolling_window)
    cumsum_hlc3_vol[rolling_window:] = np.cumsum(hlc3_vol)
    cumsum_vol[rolling_window:] = np.cumsum(volume)
    rolling_sum_hlc3_vol = (
        cumsum_hlc3_vol[rolling_window:] - cumsum_hlc3_vol[:-rolling_window]
    )
    rolling_sum_vol = cumsum_vol[rolling_window:] - cumsum_vol[:-rolling_window]
    vwap = rolling_sum_hlc3_vol / rolling_sum_vol

    return vwap


def get_position_info(position: Position, entry_time):
    if position.short.quantity != 0.0:
        return {
            "side": "Short",
            "qty": position.short.quantity,
            "entry_price": position.short.avg_price,
            "entry_time": entry_time.strftime("%Y-%m-%d %H:%M:%S"),
        }
    elif position.long.quantity != 0.0:
        return {
            "side": "Long",
            "qty": position.long.quantity,
            "entry_price": position.long.avg_price,
            "entry_time": entry_time.strftime("%Y-%m-%d %H:%M:%S"),
        }
    else:
        return {"side": "Nothing", "qty": 0.0, "entry_price": 0.0, "entry_time": 0}


def get_sr_table_info(sr, parameter_json):
    if sr >= 1.0 and sr < 2.0:
        allocate_percentage = parameter_json[1]
        no_of_strategy = parameter_json[7]
    elif sr >= 2.0 and sr < 3.0:
        allocate_percentage = parameter_json[2]
        no_of_strategy = parameter_json[8]
    elif sr >= 3.0 and sr < 4.0:
        allocate_percentage = parameter_json[3]
        no_of_strategy = parameter_json[9]
    elif sr >= 4.0 and sr < 5.0:
        allocate_percentage = parameter_json[4]
        no_of_strategy = parameter_json[10]
    elif sr >= 5.0 and sr < 6.0:
        allocate_percentage = parameter_json[5]
        no_of_strategy = parameter_json[11]
    elif sr >= 6.0:
        allocate_percentage = parameter_json[6]
        no_of_strategy = parameter_json[12]
    else:
        allocate_percentage = 0.0
        no_of_strategy = 0.0

    return [allocate_percentage, no_of_strategy]


def round_with_precision(number: float, decimal_places: int) -> float:
    """
    Truncate a float to the specified number of decimal places.

    Args:
        number (float): The number to truncate.
        decimal_places (int): The number of decimal places to keep.

    Returns:
        float: The truncated number.
    """
    factor = 10.0**decimal_places
    return math.trunc(factor * number) / factor


def get_qty(price, precision, min_qty, wallet_balance, leverage):
    qty = round_with_precision(wallet_balance * leverage / price, precision)
    if qty < min_qty:
        qty = min_qty

    return qty


def get_qty_with_percentage(
    price, precision, wallet_percentage, min_qty, wallet_balance
):
    qty = round_with_precision(wallet_balance * wallet_percentage / price, precision)
    if qty < min_qty:
        qty = min_qty

    return qty


def decimals_sum(num_one, num_two):
    num_one = Decimal(str(num_one))
    num_two = Decimal(str(num_two))
    final_num = num_one + num_two
    return float(final_num)


def decimals_minus(num_one, num_two):
    num_one = Decimal(str(num_one))
    num_two = Decimal(str(num_two))
    final_num = num_one - num_two
    return float(final_num)


def update_position(
    update: OrderUpdate,
    position: Position,
    price_precision: int,
    qty_precision: int,
    entry_time: datetime,
    pair: Symbol,
    total_pnl: float,
):
    if update.side == OrderSide.Buy:
        if position.long.quantity == 0.0 and position.short.quantity == 0.0:
            position = Position(
                pair,
                PositionData(
                    quantity=update.filled_size,
                    avg_price=update.price,
                ),
                PositionData(quantity=0.0, avg_price=0.0),
            )
            entry_time = datetime.now(pytz.timezone("UTC"))
            logging.info(
                f"Open long position: {get_position_info(position,entry_time)}, current order update: {update}"
            )
            logging.info(f"Latest position: {get_position_info(position,entry_time)}")
        elif position.long.quantity == 0.0 and position.short.quantity != 0.0:
            remain_qty = round_with_precision(
                decimals_minus(position.short.quantity, update.filled_size),
                qty_precision,
            )

            if remain_qty > 0.0:
                pnl = (position.short.avg_price - update.price) * update.filled_size
                total_pnl += pnl
                position = Position(
                    pair,
                    PositionData(quantity=0.0, avg_price=0.0),
                    PositionData(
                        quantity=remain_qty,
                        avg_price=position.short.avg_price,
                    ),
                )
            elif remain_qty == 0.0:
                pnl = (position.short.avg_price - update.price) * update.filled_size
                total_pnl += pnl
                position = Position(
                    pair,
                    PositionData(quantity=0.0, avg_price=0.0),
                    PositionData(quantity=0.0, avg_price=0.0),
                )
            else:
                pnl = (
                    position.short.avg_price - update.price
                ) * position.short.quantity
                total_pnl += pnl
                position = Position(
                    pair,
                    PositionData(
                        quantity=abs(remain_qty),
                        avg_price=update.price,
                    ),
                    PositionData(quantity=0.0, avg_price=0.0),
                )
                entry_time = datetime.now(pytz.timezone("UTC"))
            logging.info(
                f"Close short position: {get_position_info(position,entry_time)}, current close order qty: {update.filled_size}, price: {update.price}, short position info: {position.short}, pnl: {pnl}, total_pnl: {total_pnl}"
            )
            logging.info(f"Latest position: {get_position_info(position,entry_time)}")
        elif position.long.quantity != 0.0:
            latest_qty = round_with_precision(
                decimals_sum(position.long.quantity, update.filled_size),
                qty_precision,
            )
            latest_price = round_with_precision(
                (
                    position.long.quantity * position.long.avg_price
                    + update.filled_size * update.price
                )
                / latest_qty,
                price_precision,
            )
            position = Position(
                pair,
                PositionData(
                    quantity=latest_qty,
                    avg_price=latest_price,
                ),
                PositionData(quantity=0.0, avg_price=0.0),
            )
            entry_time = datetime.now(pytz.timezone("UTC"))
            logging.info(f"Latest position: {get_position_info(position,entry_time)}")
    else:
        if position.long.quantity == 0.0 and position.short.quantity == 0.0:
            position = Position(
                pair,
                PositionData(quantity=0.0, avg_price=0.0),
                PositionData(
                    quantity=update.filled_size,
                    avg_price=update.price,
                ),
            )
            entry_time = datetime.now(pytz.timezone("UTC"))
            logging.info(
                f"Open short position: {get_position_info(position,entry_time)}, current order update: {update}"
            )
            logging.info(f"Latest position: {get_position_info(position,entry_time)}")
        elif position.long.quantity != 0.0 and position.short.quantity == 0.0:
            remain_qty = round_with_precision(
                decimals_minus(position.long.quantity, update.filled_size),
                qty_precision,
            )
            if remain_qty > 0.0:
                pnl = (update.price - position.long.avg_price) * update.filled_size
                total_pnl += pnl
                position = Position(
                    pair,
                    PositionData(
                        quantity=remain_qty,
                        avg_price=position.long.avg_price,
                    ),
                    PositionData(quantity=0.0, avg_price=0.0),
                )
            elif remain_qty == 0.0:
                pnl = (update.price - position.long.avg_price) * update.filled_size
                total_pnl += pnl
                position = Position(
                    pair,
                    PositionData(quantity=0.0, avg_price=0.0),
                    PositionData(quantity=0.0, avg_price=0.0),
                )
            else:
                pnl = (update.price - position.long.avg_price) * position.long.quantity
                total_pnl += pnl
                position = Position(
                    pair,
                    PositionData(quantity=0.0, avg_price=0.0),
                    PositionData(
                        quantity=abs(remain_qty),
                        avg_price=update.price,
                    ),
                )
                entry_time = datetime.now(pytz.timezone("UTC"))
            logging.info(
                f"Close long position: {get_position_info(position,entry_time)}, current close order qty: {update.filled_size}, price: {update.price}, long position info: {position.long}, pnl: {pnl}, total_pnl: {total_pnl}"
            )
            logging.info(f"Latest position: {get_position_info(position,entry_time)}")
        elif position.short.quantity != 0.0:
            latest_qty = round_with_precision(
                decimals_sum(position.short.quantity, update.filled_size),
                qty_precision,
            )
            latest_price = round_with_precision(
                (
                    position.short.quantity * position.short.avg_price
                    + update.filled_size * update.price
                )
                / latest_qty,
                price_precision,
            )
            position = Position(
                pair,
                PositionData(quantity=0.0, avg_price=0.0),
                PositionData(
                    quantity=latest_qty,
                    avg_price=latest_price,
                ),
            )
            entry_time = datetime.now(pytz.timezone("UTC"))
            logging.info(f"Latest position: {get_position_info(position,entry_time)}")
    return [position, total_pnl, entry_time]


def send_notification(message: str, chat_id: str, token: str):
    try:
        url = f"https://api.telegram.org/{token}/sendMessage?chat_id={chat_id}&text={message}"
        requests.get(url, timeout=5)
    except Exception as e:
        logging.error(f"Failed to send message to telegram: {e}")


def fetch_binance_spot_candles(start_time, end_time, interval, symbol, gap):
    pi_ticker = symbol
    pi_time_interval = interval
    pi_start_time = start_time
    pi_end_time = end_time
    spot_price_data = []
    while pi_start_time < pi_end_time:
        start_time_2 = int(pi_start_time.timestamp() * 1000)
        spot_kline_url = (
            "https://api.binance.com/api/v3/klines?symbol="
            + str(pi_ticker)
            + "&interval="
            + str(pi_time_interval)
            + "&limit=1000&startTime="
            + str(start_time_2)
        )
        spot_kline_resp = requests.get(spot_kline_url)
        spot_kline_resp = json.loads(spot_kline_resp.content.decode())
        spot_price_data.extend(spot_kline_resp)
        pi_start_time = pi_start_time + timedelta(minutes=gap * 1000)
        current_last_time = spot_price_data[-1][0]
        if current_last_time == end_time and pi_end_time < pi_start_time:
            break
        time.sleep(0.0001)

    spot_price_data = pd.DataFrame(spot_price_data)
    spot_price_data.columns = [
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "Close Time",
        "Ignore",
        "Ignore",
        "Ignore",
        "Ignore",
        "Ignore",
    ]
    # spot_price_data = spot_price_data.set_index('time')
    spot_price_data = spot_price_data[
        ["time", "open", "high", "low", "close", "volume"]
    ]
    spot_price_data["time"] = spot_price_data["time"].astype(float)
    spot_price_data["open"] = spot_price_data["open"].astype(float)
    spot_price_data["high"] = spot_price_data["high"].astype(float)
    spot_price_data["low"] = spot_price_data["low"].astype(float)
    spot_price_data["close"] = spot_price_data["close"].astype(float)
    spot_price_data["volume"] = spot_price_data["volume"].astype(float)
    spot_price_data = spot_price_data[
        spot_price_data["time"] >= start_time.timestamp() * 1000
    ]
    spot_price_data = spot_price_data[
        spot_price_data["time"] <= end_time.timestamp() * 1000
    ]
    spot_price_data = spot_price_data.drop_duplicates(subset=["time"])
    spot_price_data["time"] = pd.to_datetime(spot_price_data["time"], unit="ms")
    logging.info(
        f"Done fetching binance spot candles from {spot_price_data['time'].iloc[0]} to {spot_price_data['time'].iloc[-1]}"
    )
    return spot_price_data


def fetch_bybit_candle(
    session, start_time, end_time, interval, symbol, gap_in_seconds, category
):
    pi_start_time = start_time.timestamp() * 1000
    pi_end_time = end_time.timestamp() * 1000
    price_data = []
    while pi_end_time > pi_start_time:
        candles = session.get_kline(
            category=category,
            symbol=symbol,
            interval=interval,
            limit=1000,
            end=pi_end_time,
        )
        price_data.extend(candles["result"]["list"])
        pi_end_time = pi_end_time - gap_in_seconds * 1000 * 1000

    price_data = pd.DataFrame(price_data)
    price_data.columns = ["time", "open", "high", "low", "close", "volume", "turnover"]
    price_data["time"] = price_data["time"].astype(float)
    price_data["open"] = price_data["open"].astype(float)
    price_data["high"] = price_data["high"].astype(float)
    price_data["low"] = price_data["low"].astype(float)
    price_data["close"] = price_data["close"].astype(float)
    price_data["volume"] = price_data["volume"].astype(float)
    price_data = price_data[price_data["time"] >= start_time.timestamp() * 1000]
    price_data = price_data.drop_duplicates(subset=["time"])
    price_data["time"] = pd.to_datetime(price_data["time"], unit="ms")
    # price_data = price_data.set_index("time")
    price_df = price_data[["time", "open", "high", "low", "close", "volume"]]
    data_reversed = price_df.reindex(index=price_df.index[::-1])
    logging.info(
        f"Done fetching bybit spot candles from {data_reversed['time'].iloc[0]} to {data_reversed['time'].iloc[-1]}"
    )
    return data_reversed


def fetch_bybit_premium_index(
    session, start_time, end_time, interval, symbol, gap_in_seconds, category
):
    pi_start_time = start_time.timestamp() * 1000
    pi_end_time = end_time.timestamp() * 1000
    price_data = []
    while pi_end_time > pi_start_time:
        # print(convert_ms_to_datetime(pi_end_time))
        candles = session.get_premium_index_price_kline(
            category=category,
            symbol=symbol,
            interval=interval,
            limit=1000,
            end=pi_end_time,
        )
        price_data.extend(candles["result"]["list"])
        pi_end_time = pi_end_time - gap_in_seconds * 1000 * 1000
        time.sleep(0.0001)

    price_data = pd.DataFrame(price_data)
    price_data.columns = ["time", "pi_open", "pi_high", "pi_low", "pi_close"]
    price_data["time"] = price_data["time"].astype(float)
    price_data["pi_open"] = price_data["pi_open"].astype(float)
    price_data["pi_high"] = price_data["pi_high"].astype(float)
    price_data["pi_low"] = price_data["pi_low"].astype(float)
    price_data["pi_close"] = price_data["pi_close"].astype(float)
    price_data = price_data[price_data["time"] >= start_time.timestamp() * 1000]
    price_data = price_data[price_data["time"] <= end_time.timestamp() * 1000]
    price_data = price_data.drop_duplicates(subset=["time"])
    price_data["time"] = pd.to_datetime(price_data["time"], unit="ms")
    # price_data = price_data.set_index("time")
    price_df = price_data[["time", "pi_open", "pi_high", "pi_low", "pi_close"]]
    data_reversed = price_df.reindex(index=price_df.index[::-1])
    logging.info(
        f"Done fetching bybit premium index from {data_reversed['time'].iloc[0]} to {data_reversed['time'].iloc[-1]}"
    )
    return data_reversed


def fetch_binance_premium_index(
    fetch_start_date=datetime(2024, 1, 28, 0, 0, 0, tzinfo=timezone.utc),
    fetch_end_time=datetime(2024, 1, 30, 0, 0, 0, tzinfo=timezone.utc),
    timedelta_min=1500,
    symbol="BTCUSDT",
    interval="15m",
):
    very_first_start_time = fetch_start_date
    pi_data = []
    end_time_2 = int(fetch_end_time.timestamp() * 1000)
    current_last_time = 0
    while fetch_start_date < fetch_end_time or current_last_time != end_time_2:
        logging.debug(fetch_start_date)
        start_time_2 = int(fetch_start_date.timestamp() * 1000)
        url = (
            "https://fapi.binance.com/fapi/v1/premiumIndexKlines?symbol="
            + str(symbol)
            + "&interval="
            + str(interval)
            + "&limit=1500&startTime="
            + str(start_time_2)
            + "&endTime="
            + str(end_time_2)
        )
        resp = requests.get(url)
        resp = json.loads(resp.content.decode())
        pi_data.extend(resp)
        current_last_time = pi_data[-1][0]
        # print(pi_data)
        fetch_start_date = fetch_start_date + timedelta(minutes=timedelta_min * 1500)
    pi_data = pd.DataFrame(pi_data)
    pi_data.columns = [
        "time",
        "pi_open",
        "pi_high",
        "pi_low",
        "pi_close",
        "Volume",
        "Close Time",
        "Ignore",
        "Ignore",
        "Ignore",
        "Ignore",
        "Ignore",
    ]
    pi_data = pi_data[pi_data["time"] <= fetch_end_time.timestamp() * 1000]
    pi_data = pi_data[pi_data["time"] >= very_first_start_time.timestamp() * 1000]
    pi_data = pi_data[["time", "pi_open", "pi_high", "pi_low", "pi_close"]]
    pi_data["time"] = pi_data["time"].astype(float)
    pi_data["pi_open"] = pi_data["pi_open"].astype(float)
    pi_data["pi_high"] = pi_data["pi_high"].astype(float)
    pi_data["pi_low"] = pi_data["pi_low"].astype(float)
    pi_data["pi_close"] = pi_data["pi_close"].astype(float)
    pi_data = pi_data.drop_duplicates(subset=["time"])
    logging.info(
        f"Done fetching binance premium index from {convert_ms_to_datetime(pi_data['time'].iloc[0])} to {convert_ms_to_datetime(pi_data['time'].iloc[-1])}"
    )
    return pi_data


def store_position_when_shutdown(
    position_db_params, bot_id, pair, entry_time, position
):
    is_connected = False
    try:
        while is_connected == False:
            try:
                conn = psycopg2.connect(**position_db_params)
                is_connected = True
            except Exception as e:
                time.sleep(1)
                logging.error(f"Failed to connect db: {e}")
            cur = conn.cursor()
            data = {
                "bot_id": bot_id,
                "symbol": f"{pair.base}{pair.quote}",
                "timestamp": entry_time.timestamp() * 1000,
            }
            if position.long.quantity > 0:
                data["entry_price"] = position.long.avg_price
                data["qty"] = position.long.quantity
                data["side"] = LONG
            else:
                data["entry_price"] = position.short.avg_price
                data["qty"] = position.short.quantity
                data["side"] = SHORT
            # Execute SQL query to delete data if it exists
            cur.execute(
                """
                DELETE FROM position
                WHERE bot_id = %(bot_id)s;
            """,
                data,
            )
            conn.commit()
            # Execute SQL query to insert data
            cur.execute(
                """
                INSERT INTO position (bot_id, symbol, entry_price, qty, side, timestamp)
                VALUES (%(bot_id)s, %(symbol)s, %(entry_price)s, %(qty)s, %(side)s, %(timestamp)s);
            """,
                data,
            )
            logging.info(f"inserted the position: {data} to db")
            conn.commit()
            # Close the connection
            conn.close()
    except Exception as e:
        logging.error(f"Failed to store position into db: {e}")


async def limit_order_handler(
    active_orders,
    order_pool,
    strategy,
    place_order_exchange,
    pair,
    replace_entry_order_count,
    replace_tp_order_count,
    replace_limit_max_time,
    replace_order_interval_in_sec,
    cancel_entry_order_count,
):
    if len(active_orders) != 0 or len(order_pool) != 0:
        for order in active_orders:
            for limit in order_pool:
                if limit[0] == order.client_order_id and datetime.now(
                    timezone.utc
                ) >= limit[1] + timedelta(seconds=replace_order_interval_in_sec):
                    if limit[3] == False and limit[4] == True:
                        if datetime.now(timezone.utc) >= limit[6] and limit[2] == False:
                            try:
                                await strategy.cancel(
                                    exchange=place_order_exchange,
                                    id=order.client_order_id,
                                    symbol=pair,
                                )
                                logging.info(
                                    f"Sent cancel for limit entry order {limit} with entry_time: {limit[1]} at {datetime.now(timezone.utc)} after {replace_limit_max_time} seconds"
                                )
                                cancel_entry_order_count += 1
                                limit[3] = True
                            except Exception as e:
                                logging.error(f"Failed to cancel order: {e}")
                        else:
                            try:
                                best_bid_ask = await get_order_book(
                                    strategy=strategy,
                                    exchange=place_order_exchange,
                                    pair=pair,
                                )
                                if best_bid_ask[0] != 0.0 and best_bid_ask[1] != 0.0:
                                    need_to_replace = True
                                    if order.params.side == OrderSide.Buy:
                                        price = best_bid_ask[0]
                                        if price == limit[5]:
                                            need_to_replace = False
                                    else:
                                        price = best_bid_ask[1]
                                        if price == limit[5]:
                                            need_to_replace = False
                                    if need_to_replace:
                                        await strategy.cancel(
                                            exchange=place_order_exchange,
                                            id=order.client_order_id,
                                            symbol=pair,
                                        )
                                        if limit[2] == True:
                                            order_type = "tp"
                                            is_tp_order = True
                                            replace_tp_order_count += 1
                                        else:
                                            order_type = "entry"
                                            is_tp_order = False
                                            replace_entry_order_count += 1
                                        logging.info(
                                            f"Sent cancel for limit {order_type} order {limit} with entry_time: {limit[1]} at {datetime.now(timezone.utc)}"
                                        )
                                        limit[3] = True
                                        order_resp = await strategy.order(
                                            params=OrderParams(
                                                limit=price,
                                                side=order.params.side,
                                                quantity=order.params.quantity,
                                                symbol=pair,
                                                exchange=place_order_exchange,
                                                is_hedge_mode=False,
                                                is_post_only=True,
                                            )
                                        )
                                        order_pool.append(
                                            [
                                                order_resp.client_order_id,
                                                datetime.now(timezone.utc),
                                                is_tp_order,
                                                False,
                                                False,
                                                price,
                                                limit[6],
                                                limit[7],
                                            ]
                                        )
                                        logging.info(
                                            f"Inserted a replace {order.params.side} limit {order_type} order with client_order_id: {order_resp.client_order_id} into order_pool with replacing {order.client_order_id}"
                                        )
                                        logging.info(
                                            f"Placed a replace {order.params.side} {order_type} order with qty {order.params.quantity} when price: {price} and ori_price: {limit[7]} at time {datetime.now(timezone.utc)}"
                                        )
                            except Exception as e:
                                logging.error(
                                    f"Failed to cancel and replace order: {e}"
                                )
    return [
        order_pool,
        replace_entry_order_count,
        replace_tp_order_count,
        cancel_entry_order_count,
    ]


async def close_long_position_limit(
    strategy,
    order_book,
    position,
    order_pool,
    place_order_exchange,
    symbol,
    replace_limit_max_time_in_min,
    start_time,
):
    price = order_book[1]
    try:
        order_resp = await strategy.order(
            params=OrderParams(
                limit=price,
                side=OrderSide.Sell,
                quantity=abs(position.long.quantity),
                symbol=symbol,
                exchange=place_order_exchange,
                is_hedge_mode=False,
                is_post_only=True,
            )
        )
        order_pool.append(
            [
                order_resp.client_order_id,
                datetime.now(timezone.utc),
                True,
                False,
                False,
                price,
                datetime.now(timezone.utc)
                + timedelta(minutes=replace_limit_max_time_in_min),
                price,
            ]
        )
        logging.info(
            f"Inserted a close long limit order with client_order_id: {order_resp.client_order_id} into order_pool"
        )
        logging.info(
            f"Placed a close long order with qty {position.long.quantity} when price: {price} at time {convert_ms_to_datetime(start_time)}"
        )
    except Exception as e:
        logging.error(f"Failed to close entire position: {e}")

    return order_pool


async def close_short_position_limit(
    strategy,
    order_book,
    position,
    order_pool,
    place_order_exchange,
    symbol,
    replace_limit_max_time_in_min,
    start_time,
):
    price = order_book[0]
    try:
        order_resp = await strategy.order(
            params=OrderParams(
                limit=price,
                side=OrderSide.Buy,
                quantity=abs(position.short.quantity),
                symbol=symbol,
                exchange=place_order_exchange,
                is_hedge_mode=False,
                is_post_only=True,
            )
        )
        order_pool.append(
            [
                order_resp.client_order_id,
                datetime.now(timezone.utc),
                True,
                False,
                False,
                price,
                datetime.now(timezone.utc)
                + timedelta(minutes=replace_limit_max_time_in_min),
                price,
            ]
        )
        logging.info(
            f"Inserted a close short limit order with client_order_id: {order_resp.client_order_id} into order_pool"
        )
        logging.info(
            f"Placed a close short order with qty {position.short.quantity} when price: {price} at time {convert_ms_to_datetime(start_time)}"
        )
    except Exception as e:
        logging.error(f"Failed to close entire position: {e}")

    return order_pool


async def open_short_position_limit(
    strategy,
    order_book,
    order_pool,
    place_order_exchange,
    symbol,
    replace_limit_max_time_in_min,
    start_time,
    qty,
):
    price = order_book[1]
    try:
        order_resp = await strategy.order(
            params=OrderParams(
                limit=price,
                side=OrderSide.Sell,
                quantity=qty,
                symbol=symbol,
                exchange=place_order_exchange,
                is_hedge_mode=False,
                is_post_only=True,
            )
        )
        order_pool.append(
            [
                order_resp.client_order_id,
                datetime.now(timezone.utc),
                False,
                False,
                False,
                price,
                datetime.now(timezone.utc)
                + timedelta(minutes=replace_limit_max_time_in_min),
                price,
            ]
        )
        logging.info(
            f"Inserted a sell limit order with client_order_id: {order_resp.client_order_id} into order_pool"
        )
        logging.info(
            f"Placed a sell order with qty {qty} when price: {price} at time {convert_ms_to_datetime(start_time)}"
        )
    except Exception as e:
        logging.error(f"Failed to place sell limit order: {e}")
    return order_pool


async def open_long_position_limit(
    strategy,
    order_book,
    order_pool,
    place_order_exchange,
    symbol,
    replace_limit_max_time_in_min,
    start_time,
    qty,
):
    price = order_book[0]
    try:
        order_resp = await strategy.order(
            params=OrderParams(
                limit=price,
                side=OrderSide.Buy,
                quantity=qty,
                symbol=symbol,
                exchange=place_order_exchange,
                is_hedge_mode=False,
                is_post_only=True,
            )
        )
        order_pool.append(
            [
                order_resp.client_order_id,
                datetime.now(timezone.utc),
                False,
                False,
                False,
                price,
                datetime.now(timezone.utc)
                + timedelta(minutes=replace_limit_max_time_in_min),
                price,
            ]
        )
        logging.info(
            f"Inserted a buy limit order with client_order_id: {order_resp.client_order_id} into order_pool"
        )
        logging.info(
            f"Placed a buy order with qty {qty} when price: {price} at time {convert_ms_to_datetime(start_time)}"
        )
    except Exception as e:
        logging.error(f"Failed to place buy limit order: {e}")
    return order_pool


async def replace_rejected_limit_order(
    strategy,
    order_pool,
    place_order_exchange,
    pair,
    update,
    replace_limit_max_time_in_min,
):
    try:
        best_bid_ask = await get_order_book(
            strategy=strategy,
            exchange=place_order_exchange,
            pair=pair,
        )
        if best_bid_ask[0] != 0.0 and best_bid_ask[1] != 0.0:
            if update.side == OrderSide.Buy:
                price = best_bid_ask[0]
            else:
                price = best_bid_ask[1]
            order_resp = await strategy.order(
                params=OrderParams(
                    limit=price,
                    side=update.side,
                    quantity=update.remain_size,
                    symbol=pair,
                    exchange=place_order_exchange,
                    is_hedge_mode=False,
                    is_post_only=True,
                )
            )
            order_pool.append(
                [
                    order_resp.client_order_id,
                    datetime.now(timezone.utc),
                    True,
                    False,
                    False,
                    price,
                    datetime.now(timezone.utc)
                    + timedelta(minutes=replace_limit_max_time_in_min),
                    price,
                ]
            )
            logging.info(
                f"Inserted a replace {update.side} limit order with client_order_id: {order_resp.client_order_id} into order_pool with replacing {update.client_order_id}"
            )
            logging.info(
                f"Placed a replace {update.side} order with qty {update.remain_size} when price: {price} due to order get rejected at time {datetime.now(timezone.utc)}"
            )
    except Exception as e:
        logging.error(f"Failed to cancel and replace order: {e}")
    return order_pool


async def replace_rejected_limit_tp_order(
    strategy,
    tp_order_pool,
    place_order_exchange,
    pair,
    update,
):
    try:
        best_bid_ask = await get_order_book(
            strategy=strategy,
            exchange=place_order_exchange,
            pair=pair,
        )
        if best_bid_ask[0] != 0.0 and best_bid_ask[1] != 0.0:
            if update.side == OrderSide.Buy:
                price = best_bid_ask[0]
            else:
                price = best_bid_ask[1]
            order_resp = await strategy.order(
                params=OrderParams(
                    limit=price,
                    side=update.side,
                    quantity=update.remain_size,
                    symbol=pair,
                    exchange=place_order_exchange,
                    is_hedge_mode=False,
                    is_post_only=True,
                )
            )
            tp_order_pool.append(
                [
                    order_resp.client_order_id,
                    datetime.now(timezone.utc),
                    True,
                    False,
                    False,
                ]
            )
            logging.info(
                f"Inserted a replace {update.side} tp limit order with client_order_id: {order_resp.client_order_id} into order_pool"
            )
            logging.info(
                f"Placed a replace {update.side} tp order with qty {update.remain_size} when price: {price} due to order get rejected at time {datetime.now(timezone.utc)}"
            )
    except Exception as e:
        logging.error(f"Failed to cancel and replace order: {e}")
    return tp_order_pool


async def open_long_position_with_tp(
    strategy,
    order_book,
    order_pool,
    place_order_exchange,
    symbol,
    replace_limit_max_time_in_min,
    start_time,
    qty,
    tp_percentage,
    tp_order_pool,
    closed_min_time_ts,
    event_entry_time,
):
    try:
        event_entry_time = closed_min_time_ts
        take_profit = (order_book[1] + order_book[0]) / 2.0 * (1.0 + tp_percentage)
        order_resp = await strategy.order(
            params=OrderParams(
                limit=order_book[0],
                side=OrderSide.Buy,
                quantity=qty,
                symbol=symbol,
                exchange=place_order_exchange,
                is_hedge_mode=False,
                is_post_only=True,
            )
        )
        order_pool.append(
            [
                order_resp.client_order_id,
                datetime.now(timezone.utc),
                False,
                False,
                False,
                order_book[0],
                datetime.now(timezone.utc)
                + timedelta(minutes=replace_limit_max_time_in_min),
                order_book[0],
            ]
        )
        logging.info(
            f"Inserted a buy limit order with client_order_id: {order_resp.client_order_id} into order_pool"
        )
        order_resp1 = await strategy.order(
            params=OrderParams(
                limit=take_profit,
                side=OrderSide.Sell,
                quantity=qty,
                symbol=symbol,
                exchange=place_order_exchange,
                is_hedge_mode=False,
                is_post_only=True,
            )
        )
        tp_order_pool.append(
            [
                order_resp1.client_order_id,
                datetime.now(timezone.utc),
                False,
                False,
                False,
            ]
        )
        logging.info(
            f"Inserted a tp limit order with client_order_id: {order_resp1.client_order_id} into tp_order_pool"
        )
        logging.info(
            f"Placed a buy order with qty {qty} when price: {order_book[0]}, entry_time: {closed_min_time_ts}, take_profit: {take_profit} at time {convert_ms_to_datetime(start_time)}"
        )
    except Exception as e:
        logging.error(f"Failed to place buy limit order: {e}")

    return [order_pool, tp_order_pool, event_entry_time]


async def open_short_position_with_tp(
    strategy,
    order_book,
    order_pool,
    place_order_exchange,
    symbol,
    replace_limit_max_time_in_min,
    start_time,
    qty,
    tp_percentage,
    tp_order_pool,
    closed_min_time_ts,
    event_entry_time,
):
    try:
        event_entry_time = closed_min_time_ts
        take_profit = (order_book[1] + order_book[0]) / 2.0 * (1.0 - tp_percentage)
        order_resp = await strategy.order(
            params=OrderParams(
                limit=order_book[1],
                side=OrderSide.Sell,
                quantity=qty,
                symbol=symbol,
                exchange=place_order_exchange,
                is_hedge_mode=False,
                is_post_only=True,
            )
        )
        order_pool.append(
            [
                order_resp.client_order_id,
                datetime.now(timezone.utc),
                False,
                False,
                False,
                order_book[1],
                datetime.now(timezone.utc)
                + timedelta(minutes=replace_limit_max_time_in_min),
                order_book[1],
            ]
        )
        logging.info(
            f"Inserted a sell limit order with client_order_id: {order_resp.client_order_id} into order_pool"
        )
        order_resp1 = await strategy.order(
            params=OrderParams(
                limit=take_profit,
                side=OrderSide.Buy,
                quantity=qty,
                symbol=symbol,
                exchange=place_order_exchange,
                is_hedge_mode=False,
                is_post_only=True,
            )
        )
        tp_order_pool.append(
            [
                order_resp1.client_order_id,
                datetime.now(timezone.utc),
                False,
                False,
                False,
            ]
        )
        logging.info(
            f"Inserted a tp limit order with client_order_id: {order_resp1.client_order_id} into tp_order_pool"
        )
        logging.info(
            f"Placed a sell order with qty {qty} when price: {order_book[1]}, entry_time: {closed_min_time_ts}, take_profit: {take_profit} at time {convert_ms_to_datetime(start_time)}"
        )
    except Exception as e:
        logging.error(f"Failed to place sell limit order: {e}")
    return [order_pool, tp_order_pool, event_entry_time]


def convert_liquidations_btc_unit_df(
    data,
    ori_df,
    topic,
    runtime_mode,
    closed_min_time_ts,
    closed_min_time,
    exchange,
    is_reached,
    resample_time,
):
    start_time = np.array(list(map(lambda c: float(c["start_time"]), data)))
    if closed_min_time_ts != start_time[-1] and runtime_mode != RuntimeMode.Backtest:
        logging.info(
            f"topic: {topic}, Latest min should get: {closed_min_time}, timestamp: {closed_min_time_ts}, datasource last liquidations data start_time: {start_time[-1]}, {convert_ms_to_datetime(start_time[-1])}"
        )
        return ori_df, is_reached
    long_liquidations = np.array(
        list(map(lambda c: float(c["long_liquidations"]), data))
    )
    short_liquidations = np.array(
        list(map(lambda c: float(c["short_liquidations"]), data))
    )
    col1 = f"long_liquidations_{exchange}"
    col2 = f"short_liquidations_{exchange}"
    liquidation_df = pd.DataFrame(
        {
            "time": start_time,
            col1: long_liquidations,
            col2: short_liquidations,
        }
    )
    liquidation_df["time"] = liquidation_df["time"].astype(float)
    liquidation_df["time"] = pd.to_datetime(liquidation_df["time"], unit="ms")
    liquidation_df.set_index("time", inplace=True)
    df_resampled = liquidation_df[[col1, col2]].resample(resample_time).sum()
    df_resampled.reset_index(inplace=True)
    is_reached = True

    logging.info(
        f"topic: {topic}, is_latest_{exchange}_liquidation: {is_reached}, Latest min should get: {closed_min_time}, timestamp: {closed_min_time_ts}, short_liquidations: {short_liquidations[-1]}, long_liquidations: {long_liquidations[-1]} at {convert_ms_to_datetime(start_time[-1])}"
    )
    return df_resampled, is_reached


def convert_open_interest_df(
    data,
    ori_df,
    topic,
    runtime_mode,
    closed_min_time_ts,
    closed_min_time,
    exchange,
    is_reached,
    resample_time,
):
    start_time = np.array(list(map(lambda c: float(c["start_time"]), data)))
    if closed_min_time_ts != start_time[-1] and runtime_mode != RuntimeMode.Backtest:
        logging.info(
            f"topic: {topic}, Latest min should get: {closed_min_time}, timestamp: {closed_min_time_ts}, datasource last open interest data start_time: {start_time[-1]}, {convert_ms_to_datetime(start_time[-1])}"
        )
        return ori_df, is_reached
    open_interest = np.array(list(map(lambda c: float(c["open_interest"]), data)))

    col1 = f"open_interest_{exchange}"
    oi_df = pd.DataFrame(
        {
            "time": start_time,
            col1: open_interest,
        }
    )
    oi_df["time"] = oi_df["time"].astype(float)
    oi_df["time"] = pd.to_datetime(oi_df["time"], unit="ms")
    oi_df.set_index("time", inplace=True)
    df_resampled = oi_df[[col1]].resample(resample_time).last()
    df_resampled.reset_index(inplace=True)
    is_reached = True
    logging.info(
        f"topic: {topic}, is_latest_{exchange}_oi: {is_reached}, Latest min should get: {closed_min_time}, timestamp: {closed_min_time_ts}, open_interest: {open_interest[-1]} at {convert_ms_to_datetime(start_time[-1])}"
    )
    return df_resampled, is_reached


def convert_marketcap_df(data, topic, resample_time):
    start_time = np.array(list(map(lambda c: float(c["start_time"]), data)))
    market_cap = np.array(list(map(lambda c: float(c["market_cap"]), data)))
    mc_df = pd.DataFrame(
        {
            "time": start_time,
            "market_cap": market_cap,
        }
    )
    mc_df["time"] = mc_df["time"].astype(float)
    mc_df["time"] = pd.to_datetime(mc_df["time"], unit="ms")
    mc_df["time"] = mc_df["time"].dt.floor("1min")
    mc_df = mc_df.drop_duplicates(subset=["time"], keep="last")
    mc_df.set_index("time", inplace=True)
    df_resampled = mc_df[["market_cap"]].resample(resample_time).last()
    df_resampled.reset_index(inplace=True)
    logging.info(
        f"topic: {topic}, market_cap: {market_cap[-1]} at {convert_ms_to_datetime(start_time[-1])}"
    )
    return df_resampled


def merge_all_df(dataframe_arr):
    merged_df = reduce(
        lambda left, right: pd.merge(left, right, on="time", how="left"), dataframe_arr
    )
    merged_df = merged_df.ffill()
    return merged_df


def get_precision_ignore_trailing_zeros(number):
    # Convert the number to a string
    number_str = str(number)

    # Find the decimal point
    if "." in number_str:
        decimal_part = number_str.split(".")[1].rstrip("0")
        return len(decimal_part)
    else:
        return 0


def get_bybit_symbol_info(symbol):
    current_symbol_info = SymbolInfo(
        qty_precision=0,
        price_precision=0,
        symbol=symbol,
        min_qty=0,
    )
    client = HTTP(
        testnet=False,
        api_key="...",
        api_secret="...",
    )
    try:
        symbol_info = client.get_instruments_info(category="linear", symbol=symbol)
        data = symbol_info["result"]["list"][0]
        qty_step = data["lotSizeFilter"]["qtyStep"]
        price_tick_size = data["priceFilter"]["tickSize"]
        current_symbol_info = SymbolInfo(
            qty_precision=get_precision_ignore_trailing_zeros(qty_step),
            price_precision=get_precision_ignore_trailing_zeros(price_tick_size),
            symbol=symbol,
            min_qty=float(data["lotSizeFilter"]["minOrderQty"]),
        )
        return current_symbol_info
    except Exception as e:
        logging.error(f"Failed to fetch orderbook from bybit: {e}")
        return current_symbol_info


def get_ob_total_qty_with_percentage(ob, percentage, side):
    total_qty = 0.0
    if side == "bid":
        first_price = ob.peekitem(-1)[0]
        upper_limit = first_price * (1.0 - percentage)
        logging.info(f"bid first_price: {first_price}, lower_limit: {upper_limit}")
        for price in reversed(ob):
            if price <= upper_limit:
                break
            total_qty += ob[price]
    else:
        first_price = ob.peekitem(0)[0]
        upper_limit = first_price * (1.0 + percentage)
        logging.info(f"ask first_price: {first_price}, upper_limit: {upper_limit}")
        for price in ob:
            if price >= upper_limit:
                break
            total_qty += ob[price]
    return total_qty


async def open_long_position_market(
    strategy, place_order_exchange, symbol, qty, current_price, start_time
):
    try:
        await strategy.open(
            side=OrderSide.Buy,
            quantity=qty,
            take_profit=None,
            stop_loss=None,
            symbol=symbol,
            exchange=place_order_exchange,
            is_hedge_mode=False,
            is_post_only=False,
        )
        logging.info(
            f"Placed a buy order with qty {qty} when current_price: {current_price} at {convert_ms_to_datetime(start_time)}"
        )
    except Exception as e:
        logging.error(f"Failed to place open buy market order: {e}")


async def open_short_position_market(
    strategy, place_order_exchange, symbol, qty, current_price, start_time
):
    try:
        await strategy.open(
            side=OrderSide.Sell,
            quantity=qty,
            take_profit=None,
            stop_loss=None,
            symbol=symbol,
            exchange=place_order_exchange,
            is_hedge_mode=False,
            is_post_only=False,
        )
        logging.info(
            f"Placed a sell order with qty {qty} when current_price: {current_price} at {convert_ms_to_datetime(start_time)}"
        )
    except Exception as e:
        logging.error(f"Failed to place open sell market order: {e}")


def send_telegram_msg(
    telegram_chat_id,
    telegram_token,
    msg_type,
    bot_id,
    qty,
    price,
    pair,
    total_pnl,
    position,
    entry_time,
):
    if msg_type == "open_long":
        msg = "placed a buy order"
    elif msg_type == "open_short":
        msg = "placed a sell order"
    elif msg_type == "close_long":
        msg = "closed long position"
    elif msg_type == "close_short":
        msg = "closed short position"
    elif msg_type == "close_long_and_open_short":
        msg = "closed long position and open short"
    else:
        msg = "closed short position and open long"
    mess = (
        str(bot_id)
        + " "
        + str(msg)
        + " with qty : "
        + str(qty)
        + " at price : "
        + str(price)
        + " with symbol : "
        + str(pair)
        + "\n"
        + "current total_pnl : "
        + str(total_pnl)
        + "\n"
        + "curernt position : "
        + str(get_position_info(position=position, entry_time=entry_time))
    )
    send_notification(
        message=mess,
        chat_id=telegram_chat_id,
        token=telegram_token,
    )
