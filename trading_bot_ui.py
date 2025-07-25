import streamlit as st
import time
import threading
import queue
from datetime import datetime
import requests
import json
from Trading_bot_1stdraft import (
    ImprovedNSEDataFetcher,
    ImprovedZerodhaConnection,
    ImprovedOrderManager,
    ProfessionalTradingEngine,
    SocialMediaStockScanner
)

# Page config
st.set_page_config(
    page_title="Trading Bot Dashboard",
    page_icon="📈",
    layout="wide"
)

# Global variables for state management
bot_running = False
trading_thread = None
message_queue = queue.Queue()
components = {}
connected = False

def get_trending_stocks_from_news():
    """Get trending stocks from news and social media"""
    try:
        # Initialize social media scanner
        scanner = SocialMediaStockScanner()
        
        # Get trending stocks from social media
        social_trending = scanner.get_trending_stocks(min_mentions=3)
        
        # Get stocks from news APIs (example using Alpha Vantage News API)
        news_stocks = []
        try:
            # You can replace this with your preferred news API
            response = requests.get(
                "https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=NSE&apikey=E985JLUWG7YHM9XO"
            )
            if response.status_code == 200:
                news_data = response.json()
                if 'feed' in news_data:
                    for article in news_data['feed']:
                        if 'ticker_sentiment' in article:
                            for ticker in article['ticker_sentiment']:
                                if ticker['ticker_sentiment_score'] > 0.5:  # Positive sentiment
                                    news_stocks.append(ticker['ticker'])
        except Exception as e:
            message_queue.put(f"Error fetching news data: {e}")
        
        # Combine and deduplicate stocks
        all_trending = list(set(social_trending + news_stocks))
        
        # Get real-time data for trending stocks
        valid_trending = []
        for symbol in all_trending:
            try:
                stock_data = components['nse_fetcher'].get_quote(symbol)
                if stock_data and stock_data['price'] > 0:
                    valid_trending.append({
                        'symbol': symbol,
                        'price': stock_data['price'],
                        'change_percent': stock_data['change_percent'],
                        'volume': stock_data['volume']
                    })
            except Exception as e:
                continue
        
        # Sort by volume and change percentage
        valid_trending.sort(key=lambda x: (x['volume'], x['change_percent']), reverse=True)
        
        return [stock['symbol'] for stock in valid_trending[:10]]  # Return top 10 trending stocks
        
    except Exception as e:
        message_queue.put(f"Error getting trending stocks: {e}")
        return []

def run_trading_cycle():
    """Main trading cycle from Trading_bot_1stdraft.py"""
    global bot_running, components
    
    while bot_running:
        try:
            # Add cycle start message
            message_queue.put(f"Starting new trading cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # Check if market is open
            if not components['order_manager'].is_market_open():
                next_open = components['order_manager'].get_next_market_open()
                message_queue.put(f"Market is currently closed.")
                message_queue.put(f"Next market open: {next_open.strftime('%Y-%m-%d %H:%M:%S')}")
                time.sleep(60)  # Check every minute
                continue

            message_queue.put("Market is open. Starting trading operations...")

            # Update positions and analytics
            components['order_manager'].update_positions()
            components['order_manager'].update_portfolio_analytics()
            current_positions = set(components['order_manager'].positions.keys())

            # Get trending stocks from multiple sources
            trending_stocks = get_trending_stocks_from_news()
            message_queue.put(f"Found {len(trending_stocks)} trending stocks: {trending_stocks}")

            # Track daily trades
            daily_trades = []
            daily_profit_target = 0.02  # 2% daily profit target
            daily_profit = 0

            # First, analyze stocks in our portfolio
            message_queue.put("Analyzing portfolio stocks...")
            for symbol in current_positions:
                try:
                    message_queue.put(f"Analyzing portfolio stock {symbol}...")
                    stock_data = components['nse_fetcher'].get_quote(symbol)

                    if stock_data:
                        # Check stop loss first
                        stop_loss_triggered, stop_loss_quantity = components['order_manager'].check_stop_loss(
                            symbol, stock_data['price']
                        )
                        if stop_loss_triggered:
                            message_queue.put(f"⚠️ Stop loss triggered for {symbol} at {stock_data['price']}")
                            result = components['order_manager'].place_and_track_order(
                                symbol=symbol,
                                action='SELL',
                                quantity=stop_loss_quantity
                            )
                            if result:
                                message_queue.put(f"✅ Stop loss sell order placed for {symbol}: {result}")
                                continue

                        # Check profit targets
                        target_hit, target_quantity = components['order_manager'].check_profit_targets(
                            symbol, stock_data['price']
                        )
                        if target_hit:
                            message_queue.put(f"🎯 Profit target hit for {symbol} at {stock_data['price']}")
                            result = components['order_manager'].place_and_track_order(
                                symbol=symbol,
                                action='SELL',
                                quantity=target_quantity
                            )
                            if result:
                                message_queue.put(f"✅ Partial profit taking order placed for {symbol}: {result}")
                                continue

                        # Check scale-in opportunity
                        scale_in, scale_quantity = components['order_manager'].check_scale_in_opportunity(
                            symbol, stock_data['price']
                        )
                        if scale_in:
                            message_queue.put(f"📈 Scale-in opportunity for {symbol} at {stock_data['price']}")
                            result = components['order_manager'].place_and_track_order(
                                symbol=symbol,
                                action='BUY',
                                quantity=scale_quantity
                            )
                            if result:
                                message_queue.put(f"✅ Scale-in order placed for {symbol}: {result}")
                                continue

                        # Quick profit taking for intraday trades
                        entry_price = components['order_manager'].positions[symbol]['average_price']
                        current_price = stock_data['price']
                        profit_percentage = ((current_price - entry_price) / entry_price) * 100

                        # Exit if we have 1% profit or more
                        if profit_percentage >= 1.0:
                            message_queue.put(f"🎯 Quick profit target hit for {symbol} at {profit_percentage:.2f}%")
                            result = components['order_manager'].place_and_track_order(
                                symbol=symbol,
                                action='SELL',
                                quantity=components['order_manager'].positions[symbol]['quantity']
                            )
                            if result:
                                message_queue.put(f"✅ Quick profit exit order placed for {symbol}: {result}")
                                daily_profit += profit_percentage
                                daily_trades.append({
                                    'symbol': symbol,
                                    'action': 'SELL',
                                    'profit': profit_percentage,
                                    'type': 'quick_exit'
                                })
                                continue

                        # If no stop loss, profit target, or scale-in, check AI signal
                        signal = components['ai_engine'].get_professional_signal(symbol, stock_data)
                        message_queue.put(f"{symbol}: {signal['action']} (Confidence: {signal['confidence']:.2f})")

                        # Check for sell signals
                        if signal['confidence'] > 0.7 and signal['action'] == 'SELL':
                            message_queue.put(f"🔴 Strong sell signal for {symbol}")
                            result = components['order_manager'].place_and_track_order(
                                symbol=symbol,
                                action='SELL',
                                quantity=components['order_manager'].positions[symbol]['quantity']
                            )
                            if result:
                                message_queue.put(f"✅ Sell order placed for {symbol}: {result}")

                except Exception as e:
                    message_queue.put(f"❌ Error analyzing portfolio stock {symbol}: {e}")
                    continue

            # Then analyze new trending stocks for buying opportunities
            message_queue.put("Analyzing new trending stocks for buying opportunities...")
            for symbol in trending_stocks:
                if symbol in current_positions:
                    continue

                try:
                    message_queue.put(f"Analyzing new stock {symbol}...")
                    stock_data = components['nse_fetcher'].get_quote(symbol)

                    if stock_data:
                        signal = components['ai_engine'].get_professional_signal(symbol, stock_data)
                        message_queue.put(f"{symbol}: {signal['action']} (Confidence: {signal['confidence']:.2f})")

                        # Lower confidence threshold for intraday trades
                        if signal['confidence'] > 0.6 and signal['action'] == 'BUY':
                            message_queue.put(f"🟢 Buy signal for {symbol}")
                            position_size = components['order_manager'].calculate_position_size(
                                symbol, stock_data['price']
                            )

                            if components['order_manager'].can_trade(symbol):
                                message_queue.put(f"Placing buy order for {symbol} - Quantity: {position_size}")
                                result = components['order_manager'].place_and_track_order(
                                    symbol=symbol,
                                    action='BUY',
                                    quantity=position_size
                                )
                                if result:
                                    message_queue.put(f"✅ Buy order placed for {symbol}: {result}")
                                    daily_trades.append({
                                        'symbol': symbol,
                                        'action': 'BUY',
                                        'price': stock_data['price'],
                                        'type': 'new_position'
                                    })
                                else:
                                    message_queue.put(f"❌ Failed to place buy order for {symbol}")
                            else:
                                message_queue.put(f"⚠️ Cannot trade {symbol} - Trading conditions not met")

                except Exception as e:
                    message_queue.put(f"❌ Error analyzing new stock {symbol}: {e}")
                    continue

            # Print daily trading summary
            message_queue.put("\nDaily Trading Summary:")
            message_queue.put(f"Total Trades Today: {len(daily_trades)}")
            message_queue.put(f"Daily Profit: {daily_profit:.2f}%")
            message_queue.put(f"Daily Profit Target: {daily_profit_target*100:.2f}%")
            
            if daily_trades:
                message_queue.put("\nTrades Executed:")
                for trade in daily_trades:
                    if trade['type'] == 'quick_exit':
                        message_queue.put(f"Quick Exit: {trade['symbol']} - Profit: {trade['profit']:.2f}%")
                    else:
                        message_queue.put(f"New Position: {trade['symbol']} @ {trade['price']:.2f}")

            # Wait before next cycle (reduced from 5 minutes to 2 minutes for more frequent checks)
            message_queue.put("\nWaiting 2 minutes before next cycle...")
            time.sleep(120)  # 2 minutes

        except Exception as e:
            message_queue.put(f"❌ Error in trading cycle: {e}")
            time.sleep(60)  # Wait 1 minute before retrying
            continue

# Sidebar for configuration
with st.sidebar:
    st.title("Configuration")
    
    # API Credentials
    api_key = st.text_input("Zerodha API Key", type="password")
    api_secret = st.text_input("Zerodha API Secret", type="password")
    access_token = st.text_input("Zerodha Access Token", type="password")
    
    if st.button("Connect to Zerodha"):
        if api_key and api_secret and access_token:
            try:
                # Initialize components from your existing code
                nse_fetcher = ImprovedNSEDataFetcher()
                zerodha_conn = ImprovedZerodhaConnection(
                    api_key=api_key,
                    api_secret=api_secret,
                    access_token=access_token
                )
                
                if zerodha_conn.connect():
                    order_manager = ImprovedOrderManager(zerodha_conn)
                    ai_engine = ProfessionalTradingEngine()
                    
                    # Store components in global variable
                    components = {
                        'nse_fetcher': nse_fetcher,
                        'zerodha_conn': zerodha_conn,
                        'order_manager': order_manager,
                        'ai_engine': ai_engine
                    }
                    connected = True
                    st.success("Connected to Zerodha!")
                else:
                    connected = False
                    st.error("Failed to connect to Zerodha")
            except Exception as e:
                connected = False
                st.error(f"Error connecting to Zerodha: {str(e)}")
        else:
            st.warning("Please enter all API credentials")

# Main content
st.title("Trading Bot Dashboard")

# Control buttons
col1, col2 = st.columns(2)
with col1:
    if st.button("Start Bot", disabled=bot_running):
        if not connected:
            st.error("Please connect to Zerodha first")
        else:
            bot_running = True
            # Start trading thread
            if trading_thread is None or not trading_thread.is_alive():
                trading_thread = threading.Thread(target=run_trading_cycle)
                trading_thread.daemon = True
                trading_thread.start()
            st.rerun()

with col2:
    if st.button("Stop Bot", disabled=not bot_running):
        bot_running = False
        st.rerun()

# Status indicator
st.markdown(f"### Status: {'🟢 Running' if bot_running else '🔴 Stopped'}")

# Main dashboard
if bot_running and connected and components.get('order_manager'):
    order_manager = components['order_manager']
    nse_fetcher = components['nse_fetcher']
    
    # Market Status
    market_open = order_manager.is_market_open()
    st.markdown(f"### Market Status: {'🟢 Open' if market_open else '🔴 Closed'}")
    
    if not market_open:
        next_open = order_manager.get_next_market_open()
        st.info(f"Next market open: {next_open.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Trading Analysis Output
    st.markdown("### Trading Analysis")
    analysis_container = st.empty()
    with analysis_container.container():
        # Get all messages from the queue
        while not message_queue.empty():
            message = message_queue.get()
            st.write(message)
    
    # Current Positions
    st.markdown("### Current Positions")
    positions = order_manager.positions
    if positions:
        for symbol, position in positions.items():
            with st.expander(f"{symbol} - {position['quantity']} shares"):
                st.write(f"Average Price: ₹{position['average_price']:.2f}")
                st.write(f"Last Updated: {position['last_updated'].strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        st.info("No current positions")
    
    # Trending Stocks
    st.markdown("### Trending Stocks")
    trending_stocks = nse_fetcher.get_top_gainers(limit=5)
    for symbol in trending_stocks:
        with st.expander(symbol):
            stock_data = nse_fetcher.get_quote(symbol)
            if stock_data:
                st.write(f"Price: ₹{stock_data['price']:.2f}")
                st.write(f"Change: {stock_data['change_percent']:.2f}%")
                st.write(f"Volume: {stock_data['volume']:,}")
    
    # Portfolio Analytics
    st.markdown("### Portfolio Analytics")
    order_manager.update_portfolio_analytics()
    st.write(f"Total Portfolio Value: ₹{order_manager.portfolio_value:,.2f}")
    st.write(f"Available Capital: ₹{order_manager.available_capital:,.2f}")
    
    # Pending Orders
    st.markdown("### Pending Orders")
    pending_orders = order_manager.get_pending_orders()
    if pending_orders:
        for order_id, order_info in pending_orders.items():
            with st.expander(f"Order {order_id}"):
                st.write(f"Symbol: {order_info['symbol']}")
                st.write(f"Action: {order_info['action']}")
                st.write(f"Quantity: {order_info['quantity']}")
                st.write(f"Status: {order_info['status']}")
    else:
        st.info("No pending orders")
    
    # Position Metrics
    st.markdown("### Position Metrics")
    for symbol, metrics in order_manager.position_metrics.items():
        with st.expander(f"{symbol} Metrics"):
            st.write(f"Entry Price: ₹{metrics['entry_price']:.2f}")
            st.write(f"Current Price: ₹{metrics['current_price']:.2f}")
            st.write(f"Unrealized P&L: ₹{metrics['unrealized_pnl']:.2f} ({metrics['unrealized_pnl_percent']:.2f}%)")
            st.write(f"Holding Time: {metrics['holding_time']:.1f} hours")

# Auto-refresh every 30 seconds if bot is running
if bot_running:
    time.sleep(30)
    st.rerun() 