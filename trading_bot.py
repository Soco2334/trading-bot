import alpaca_trade_api as tradeapi
import pandas as pd
import time
import logging
from alpaca_trade_api.rest import TimeFrame

# Alpaca API Keys
ALPACA_API_KEY = 'PK5KPFU1V91NBZAHC5GO'
ALPACA_SECRET_KEY = 'zmXaucwvaAgHXxFiYfILTYSH3aoObXgqlpfYPrmd'
ALPACA_BASE_URL = 'https://paper-api.alpaca.markets'

# Initialize Alpaca API
api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url=ALPACA_BASE_URL)

# Logging setup
logging.basicConfig(level=logging.DEBUG)

def fetch_latest_price(symbol):
    """Fetch the latest market price for the given symbol."""
    try:
        bars = api.get_bars(symbol, TimeFrame.Minute, limit=1)
        if not bars or len(bars) == 0:
            logging.error(f"No bar data available for {symbol}.")
            return None
        latest_bar = list(bars)[-1]
        return latest_bar.c
    except Exception as e:
        logging.error(f"Error fetching price for {symbol}: {e}")
        return None

def fetch_data(symbol, limit=100):
    """Fetch historical data for the given symbol."""
    try:
        bars = api.get_bars(symbol, TimeFrame.Minute, limit=limit)
        if not bars or len(bars) == 0:
            logging.warning("No market data available.")
            return pd.DataFrame()

        data = pd.DataFrame({
            'timestamp': [bar.t for bar in bars],
            'open': [bar.o for bar in bars],
            'high': [bar.h for bar in bars],
            'low': [bar.l for bar in bars],
            'close': [bar.c for bar in bars],
            'volume': [bar.v for bar in bars]
        })
        return data
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        return pd.DataFrame()

def calculate_sma(data, window):
    """Calculate the Simple Moving Average (SMA)."""
    return data['close'].rolling(window=window).mean()

def calculate_signals(data):
    """Generate buy, sell, or hold signals based on SMA."""
    try:
        data['sma_5'] = calculate_sma(data, 5)
        data['sma_20'] = calculate_sma(data, 20)

        if len(data) < 20:
            logging.warning("Not enough data for SMA calculations.")
            return data

        data['buy_condition'] = (data['sma_5'] > data['sma_20']) & (data['close'] > data['sma_5'])
        data['sell_condition'] = (data['sma_5'] < data['sma_20']) & (data['close'] < data['sma_5'])

        data['signal'] = 0
        data.loc[data['buy_condition'], 'signal'] = 1
        data.loc[data['sell_condition'], 'signal'] = -1

        return data
    except Exception as e:
        logging.error(f"Error calculating signals: {e}")
        return data

def place_order(symbol, qty, side, price=None):
    """Place an order to buy, sell, short, or cover shares."""
    try:
        if side not in ['buy', 'sell', 'short', 'cover']:
            logging.error(f"Invalid side: {side}")
            return
        
        if price is None:
            # Market order
            api.submit_order(
                symbol=symbol,
                qty=qty,
                side='buy' if side in ['buy', 'cover'] else 'sell',
                type='market',
                time_in_force='gtc'  # Good 'til canceled
            )
            logging.info(f"Market order placed: {side} {qty} shares of {symbol}")
        else:
            # Limit order
            api.submit_order(
                symbol=symbol,
                qty=qty,
                side='buy' if side in ['buy', 'cover'] else 'sell',
                type='limit',
                limit_price=price,
                time_in_force='gtc'  # Good 'til canceled
            )
            logging.info(f"Limit order placed: {side} {qty} shares of {symbol} at price {price}")
    except Exception as e:
        logging.error(f"Error placing {side} order for {symbol}: {e}")


def get_open_position(symbol):
    """Check if there is an open position (long or short) for the symbol."""
    try:
        positions = api.list_positions()
        for position in positions:
            if position.symbol == symbol:
                return position  # Position found for the symbol
        return None  # No open position for the symbol
    except Exception as e:
        logging.error(f"Error fetching open position for {symbol}: {e}")
        return None
def run_bot():
    """Main loop to run the trading bot with enhanced logging and debugging."""
    symbol = 'AAPL'
    risk_percentage = 0.01  # Risk 1% of balance per trade
    logging.info("Starting the trading bot...")

    while True:
        try:
            # Fetch data
            logging.debug("Fetching data...")
            data = fetch_data(symbol, limit=100)
            if data.empty:
                logging.warning("No data retrieved. Retrying...")
                time.sleep(60)
                continue

            # Calculate signals
            data = calculate_signals(data)
            latest_signal = data.iloc[-1]['signal']
            latest_price = fetch_latest_price(symbol)

            if latest_price is None:
                logging.warning("Skipping trade due to missing price data.")
                time.sleep(60)
                continue

            # Fetch account balance and calculate trade quantity
            balance = float(api.get_account().cash)
            qty = max(int((balance * risk_percentage) // latest_price), 1)

            # Check open position
            open_position = get_open_position(symbol)
            position_qty = int(open_position.qty) if open_position else 0
            position_side = 'long' if position_qty > 0 else 'short' if position_qty < 0 else None

            logging.info(f"Latest signal: {latest_signal}, Position side: {position_side}, Quantity: {position_qty}")

            if latest_signal == 1:  # Buy signal
                if position_side == 'short':
                    logging.info("Covering short position.")
                    place_order(symbol, abs(position_qty), 'cover', price=latest_price)
                elif position_side is None:
                    logging.info("Placing buy order.")
                    place_order(symbol, qty, 'buy')

            elif latest_signal == -1:  # Sell signal
                if position_side == 'long':
                    logging.info("Selling long position.")
                    place_order(symbol, position_qty, 'sell', price=latest_price)
                elif position_side is None:
                    logging.info("Placing short sell order.")
                    place_order(symbol, qty, 'short')

            else:
                logging.info("Signal: HOLD")

            # Sleep before the next iteration
            time.sleep(60)

        except Exception as e:
            logging.error(f"Error in trading loop: {e}")
            time.sleep(60)
