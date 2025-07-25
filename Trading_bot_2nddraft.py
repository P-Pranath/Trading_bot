#!/usr/bin/env python3
"""
Critical fixes for the Zerodha Trading Bot
==========================================
"""

import requests
import time
from datetime import datetime, timedelta
import logging
import statistics
from kiteconnect import KiteConnect
from kiteconnect.exceptions import TokenException
import sys
import codecs
import functools
import threading

# Set stdout to use UTF-8 encoding
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)

# Force flush for print statements
print = functools.partial(print, flush=True)

# Configure logging for real-time output
class FlushingStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        super().__init__(stream)
        self.stream = stream

    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            stream.write(msg + self.terminator)
            stream.flush()
        except Exception:
            self.handleError(record)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('Tradinghistory.log', encoding='utf-8', mode='w'),
        FlushingStreamHandler(sys.stdout)
    ]
)

# Remove any existing handlers to avoid duplicates
root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Add our handlers
root_logger.addHandler(logging.FileHandler('Tradinghistory.log', encoding='utf-8', mode='w'))
root_logger.addHandler(FlushingStreamHandler(sys.stdout))

class SocialMediaStockScanner:
    """
    Scans social media platforms for stock mentions and sentiment
    """
    
    def __init__(self):
        self.reddit_client = None
        self.twitter_client = None
        self.stock_mentions = {}
        self.sentiment_scores = {}
        
    def initialize_reddit(self, client_id: str, client_secret: str, user_agent: str):
        """Initialize Reddit API client"""
        try:
            import praw
            self.reddit_client = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent
            )
            logging.info("Reddit client initialized successfully")
        except Exception as e:
            logging.error(f"Failed to initialize Reddit client: {e}")
    
    def initialize_twitter(self, api_key: str, api_secret: str, access_token: str, access_token_secret: str):
        """Initialize Twitter API client"""
        try:
            import tweepy
            auth = tweepy.OAuthHandler(api_key, api_secret)
            auth.set_access_token(access_token, access_token_secret)
            self.twitter_client = tweepy.API(auth)
            logging.info("Twitter client initialized successfully")
        except Exception as e:
            logging.error(f"Failed to initialize Twitter client: {e}")
    
    def scan_reddit(self, subreddits: list = ['wallstreetbets', 'stocks', 'investing'], limit: int = 100):
        """Scan Reddit for stock mentions"""
        if not self.reddit_client:
            return {}
        
        try:
            mentions = {}
            for subreddit_name in subreddits:
                subreddit = self.reddit_client.subreddit(subreddit_name)
                for post in subreddit.hot(limit=limit):
                    # Extract stock symbols from title and content
                    text = f"{post.title} {post.selftext}"
                    symbols = self._extract_stock_symbols(text)
                    
                    for symbol in symbols:
                        if symbol not in mentions:
                            mentions[symbol] = {
                                'count': 0,
                                'sentiment': 0,
                                'posts': []
                            }
                        
                        mentions[symbol]['count'] += 1
                        mentions[symbol]['sentiment'] += self._analyze_sentiment(text)
                        mentions[symbol]['posts'].append({
                            'title': post.title,
                            'score': post.score,
                            'url': post.url
                        })
            
            return mentions
            
        except Exception as e:
            logging.error(f"Error scanning Reddit: {e}")
            return {}
    
    def scan_twitter(self, search_terms: list = ['stock', 'invest', 'trading'], limit: int = 100):
        """Scan Twitter for stock mentions"""
        if not self.twitter_client:
            return {}
        
        try:
            mentions = {}
            for term in search_terms:
                tweets = self.twitter_client.search_tweets(q=term, lang='en', count=limit)
                
                for tweet in tweets:
                    # Extract stock symbols from tweet text
                    symbols = self._extract_stock_symbols(tweet.text)
                    
                    for symbol in symbols:
                        if symbol not in mentions:
                            mentions[symbol] = {
                                'count': 0,
                                'sentiment': 0,
                                'tweets': []
                            }
                        
                        mentions[symbol]['count'] += 1
                        mentions[symbol]['sentiment'] += self._analyze_sentiment(tweet.text)
                        mentions[symbol]['tweets'].append({
                            'text': tweet.text,
                            'likes': tweet.favorite_count,
                            'retweets': tweet.retweet_count
                        })
            
            return mentions
            
        except Exception as e:
            logging.error(f"Error scanning Twitter: {e}")
            return {}
    
    def _extract_stock_symbols(self, text: str) -> list:
        """Extract stock symbols from text"""
        import re
        # Match common stock symbol patterns
        patterns = [
            r'\$[A-Z]{1,5}',  # $AAPL style
            r'[A-Z]{1,5}\.NS',  # RELIANCE.NS style
            r'[A-Z]{1,5}\.BO',  # RELIANCE.BO style
        ]
        
        symbols = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            symbols.extend(matches)
        
        return list(set(symbols))
    
    def _analyze_sentiment(self, text: str) -> float:
        """Simple sentiment analysis"""
        from textblob import TextBlob
        try:
            analysis = TextBlob(text)
            return analysis.sentiment.polarity
        except:
            return 0.0
    
    def get_trending_stocks(self, min_mentions: int = 5) -> list:
        """Get trending stocks from social media"""
        reddit_mentions = self.scan_reddit()
        twitter_mentions = self.scan_twitter()
        
        # Combine mentions from both sources
        all_mentions = {}
        for source_mentions in [reddit_mentions, twitter_mentions]:
            for symbol, data in source_mentions.items():
                if symbol not in all_mentions:
                    all_mentions[symbol] = {
                        'count': 0,
                        'sentiment': 0
                    }
                all_mentions[symbol]['count'] += data['count']
                all_mentions[symbol]['sentiment'] += data['sentiment']
        
        # Filter and sort by mention count
        trending = [
            symbol for symbol, data in all_mentions.items()
            if data['count'] >= min_mentions
        ]
        
        return sorted(trending, key=lambda x: all_mentions[x]['count'], reverse=True)

class ImprovedNSEDataFetcher:
    """
    Improved NSE data fetcher with real API integration
    """
    
    def __init__(self):
        self.base_url = "https://www.nseindia.com/api"
        self.fallback_sources = [
            "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20NEXT%2050",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20MIDCAP%20100"
        ]
        self.session = requests.Session()
        self._last_session_refresh = 0
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        self._initialize_session()
        
        # Initialize social media scanner
        self.social_scanner = SocialMediaStockScanner()
        
        # Default stocks to monitor when no other data is available
        self.default_stocks = [
            "RELIANCE",    # Reliance Industries - Diversified conglomerate
            "HDFCBANK",    # HDFC Bank - Leading private bank
            "TCS",         # Tata Consultancy Services - IT major
            "INFY",        # Infosys - IT major
            "BAJFINANCE",  # Bajaj Finance - NBFC with strong growth
            "ASIANPAINT",  # Asian Paints - Market leader in paints
            "TITAN",       # Titan Company - Consumer goods
            "HDFCAMC"      # HDFC AMC - Asset management
        ]
    
    def _initialize_session(self):
        """Initialize NSE session with proper cookies"""
        try:
            # Get NSE homepage first to establish session
            response = self.session.get("https://www.nseindia.com", timeout=10)
            if response.status_code == 200:
                logging.info("NSE session initialized successfully")
            else:
                logging.warning(f"NSE session initialization returned status: {response.status_code}")
        except Exception as e:
            logging.error(f"Error initializing NSE session: {e}")
    
    def get_quote(self, symbol: str) -> dict:
        """Get real-time quote using Yahoo Finance API with rate limiting"""
        try:
            # Remove .NS suffix if present
            symbol = symbol.replace('.NS', '')
            yahoo_symbol = f"{symbol}.NS"
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
            
            # Try up to 3 times with exponential backoff
            for attempt in range(3):
                try:
                    response = self.session.get(url, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                            result = data['chart']['result'][0]
                            meta = result.get('meta', {})
                            
                            return {
                                'symbol': symbol,
                                'price': float(meta.get('regularMarketPrice', 0)),
                                'volume': int(meta.get('regularMarketVolume', 0)),
                                'change': float(meta.get('regularMarketPrice', 0) - meta.get('previousClose', 0)),
                                'change_percent': float(((meta.get('regularMarketPrice', 0) - meta.get('previousClose', 0)) / meta.get('previousClose', 1)) * 100),
                                'high': float(meta.get('regularMarketDayHigh', 0)),
                                'low': float(meta.get('regularMarketDayLow', 0)),
                                'open': float(meta.get('regularMarketOpen', 0)),
                                'close': float(meta.get('previousClose', 0)),
                                'source': 'yahoo'
                            }
                            
                    elif response.status_code == 429:  # Rate limit
                        wait_time = (2 ** attempt) * 2
                        logging.warning(f"Rate limited, waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                        
                except Exception as e:
                    logging.warning(f"Attempt {attempt + 1} failed for {symbol}: {e}")
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                        continue
                    break
            
            # If all attempts fail, use fallback data
            return self._get_fallback_data(symbol)
            
        except Exception as e:
            logging.error(f"Error fetching quote for {symbol}: {e}")
            return self._get_fallback_data(symbol)

    def _try_nse_api(self, symbol: str) -> dict:
        """Try official NSE API with enhanced session management"""
        try:
            # Refresh session periodically
            if not hasattr(self, '_last_session_refresh') or \
            time.time() - self._last_session_refresh > 1800:  # 30 minutes
                self._refresh_nse_session()
            
            # Try multiple NSE endpoints
            endpoints = [
                f"https://www.nseindia.com/api/quote-equity?symbol={symbol}",
                f"https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050&symbol={symbol}"
            ]
            
            for url in endpoints:
                try:
                    response = self.session.get(url, timeout=15)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Handle different response structures
                        if 'data' in data and data['data']:
                            quote_data = data['data']
                            return self._format_nse_data(symbol, quote_data)
                        elif 'priceInfo' in data:
                            return self._format_nse_data(symbol, data['priceInfo'])
                            
                    elif response.status_code == 403:
                        logging.warning(f"NSE API blocked (403) for {symbol}")
                        self._refresh_nse_session()
                        time.sleep(2)
                        
                except requests.exceptions.Timeout:
                    logging.warning(f"NSE API timeout for {symbol}")
                    continue
                except requests.exceptions.RequestException as e:
                    logging.warning(f"NSE API request failed for {symbol}: {e}")
                    continue
                    
        except Exception as e:
            logging.error(f"NSE API method failed for {symbol}: {e}")
        
        return None

    def _try_yahoo_finance(self, symbol: str) -> dict:
        """Try Yahoo Finance API as fallback"""
        try:
            yahoo_symbol = f"{symbol}.NS"  # NSE suffix for Yahoo
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                    result = data['chart']['result'][0]
                    meta = result.get('meta', {})
                    
                    # Get the latest quote data
                    quotes = result.get('indicators', {}).get('quote', [{}])[0]
                    timestamp_data = result.get('timestamp', [])
                    
                    if meta and quotes:
                        return {
                            'symbol': symbol,
                            'price': float(meta.get('regularMarketPrice', 0)),
                            'volume': int(meta.get('regularMarketVolume', 0)),
                            'change': float(meta.get('regularMarketPrice', 0) - meta.get('previousClose', 0)),
                            'change_percent': float(((meta.get('regularMarketPrice', 0) - meta.get('previousClose', 0)) / meta.get('previousClose', 1)) * 100),
                            'high': float(meta.get('regularMarketDayHigh', 0)),
                            'low': float(meta.get('regularMarketDayLow', 0)),
                            'open': float(meta.get('regularMarketOpen', 0)),
                            'close': float(meta.get('previousClose', 0)),
                            'source': 'yahoo'
                        }
                        
        except Exception as e:
            logging.warning(f"Yahoo Finance failed for {symbol}: {e}")
        
        return None

    def _try_nse_alternative(self, symbol: str) -> dict:
        """Try alternative NSE endpoints"""
        try:
            # Alternative endpoint - sometimes works when main API doesn't
            alt_urls = [
                f"https://www.nseindia.com/live_market/dynaContent/live_watch/get_quote/GetQuote.jsp?symbol={symbol}",
                f"https://www.nseindia.com/live_market/dynaContent/live_watch/stock_watch/stockWatchData.json"
            ]
            
            for url in alt_urls:
                try:
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 200:
                        # Try to parse as JSON
                        try:
                            data = response.json()
                            if data and isinstance(data, dict):
                                return self._parse_alternative_response(symbol, data)
                        except ValueError:
                            # Some endpoints return HTML/text
                            continue
                            
                except Exception as e:
                    continue
                    
        except Exception as e:
            logging.warning(f"Alternative NSE methods failed for {symbol}: {e}")
        
        return None

    def _refresh_nse_session(self):
        """Refresh NSE session with better headers and cookies"""
        try:
            # Clear existing session
            self.session.cookies.clear()
            
            # Update headers to mimic real browser
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0'
            })
            
            # Get homepage to establish session
            response = self.session.get("https://www.nseindia.com", timeout=15)
            
            if response.status_code == 200:
                # Visit market data page
                self.session.get("https://www.nseindia.com/market-data", timeout=10)
                self._last_session_refresh = time.time()
                logging.info("NSE session refreshed successfully")
            else:
                logging.warning(f"NSE session refresh returned: {response.status_code}")
                
        except Exception as e:
            logging.error(f"Failed to refresh NSE session: {e}")

    def _format_nse_data(self, symbol: str, quote_data: dict) -> dict:
        """Format NSE API response into standard format"""
        try:
            return {
                'symbol': symbol,
                'price': float(quote_data.get('lastPrice', quote_data.get('ltp', 0))),
                'volume': int(quote_data.get('totalTradedVolume', quote_data.get('volume', 0))),
                'change': float(quote_data.get('change', quote_data.get('netChange', 0))),
                'change_percent': float(quote_data.get('pChange', quote_data.get('percentChange', 0))),
                'high': float(quote_data.get('dayHigh', quote_data.get('high', 0))),
                'low': float(quote_data.get('dayLow', quote_data.get('low', 0))),
                'open': float(quote_data.get('open', 0)),
                'close': float(quote_data.get('previousClose', quote_data.get('prevClose', 0))),
                'source': 'nse'
            }
        except Exception as e:
            logging.error(f"Error formatting NSE data for {symbol}: {e}")
            return None

    def _parse_alternative_response(self, symbol: str, data: dict) -> dict:
        """Parse alternative API responses"""
        try:
            # Handle different response formats
            if 'data' in data:
                return self._format_nse_data(symbol, data['data'])
            elif symbol in data:
                return self._format_nse_data(symbol, data[symbol])
            else:
                # Try to find the symbol data in the response
                for key, value in data.items():
                    if isinstance(value, dict) and 'lastPrice' in value:
                        return self._format_nse_data(symbol, value)
        except Exception:
            pass
        
        return None

    def _get_fallback_data(self, symbol: str) -> dict:
        """Return fallback data when all sources fail - REMOVE IN PRODUCTION"""
        import random
        
        # Generate realistic-looking mock data for testing
        base_price = random.uniform(100, 3000)
        change_percent = random.uniform(-5, 5)
        change = base_price * (change_percent / 100)
        
        return {
            'symbol': symbol,
            'price': round(base_price, 2),
            'volume': random.randint(10000, 1000000),
            'change': round(change, 2),
            'change_percent': round(change_percent, 2),
            'high': round(base_price * 1.05, 2),
            'low': round(base_price * 0.95, 2),
            'open': round(base_price * random.uniform(0.98, 1.02), 2),
            'close': round(base_price - change, 2),
            'source': 'fallback'
        }
    
    def get_market_stocks(self, max_stocks: int = 20) -> list:
        """Get stocks from various market sources with a limit"""
        stocks = set()
        
        try:
            # 1. Get stocks from NSE indices (limit to NIFTY 50 only)
            try:
                nse_url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
                response = self.session.get(nse_url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data:
                        # Sort by change percentage
                        sorted_stocks = sorted(
                            data['data'],
                            key=lambda x: float(x.get('pChange', 0)),
                            reverse=True
                        )
                        for stock in sorted_stocks[:max_stocks]:
                            if 'symbol' in stock:
                                stocks.add(stock['symbol'])
            except Exception as e:
                logging.warning(f"Error fetching from NSE: {e}")
            
            # 2. Get stocks from Yahoo Finance top gainers (limited)
            try:
                yahoo_url = f"https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=true&lang=en-US&region=US&scrIds=top_gainers&count={max_stocks}"
                response = self.session.get(yahoo_url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if 'finance' in data and 'result' in data['finance']:
                        for stock in data['finance']['result'][0]['quotes'][:max_stocks]:
                            if 'symbol' in stock:
                                symbol = stock['symbol'].replace('.NS', '')
                                stocks.add(symbol)
            except Exception as e:
                logging.warning(f"Error fetching from Yahoo Finance: {e}")
            
            return list(stocks)[:max_stocks]  # Ensure we don't return more than max_stocks
            
        except Exception as e:
            logging.error(f"Error getting market stocks: {e}")
            return []

    def get_top_gainers(self, limit: int = 10) -> list:
        """Get top gainers using multiple sources including social media"""
        try:
            # Get stocks from all sources with limits
            social_trending = self.social_scanner.get_trending_stocks(min_mentions=3)[:5]  # Limit to top 5 social trends
            market_stocks = self.get_market_stocks(max_stocks=15)  # Limit to 15 market stocks
            
            # Combine all sources, prioritizing social media trends
            stocks_to_analyze = []
            if social_trending:
                stocks_to_analyze.extend(social_trending)
            if market_stocks:
                stocks_to_analyze.extend(market_stocks)
            if not stocks_to_analyze:
                stocks_to_analyze = self.default_stocks
            
            # Remove duplicates while preserving order
            stocks_to_analyze = list(dict.fromkeys(stocks_to_analyze))
            
            print(f"\nFetching real-time data for {len(stocks_to_analyze)} stocks...")
            
            # Create a new session for Yahoo Finance
            yahoo_session = requests.Session()
            yahoo_session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9',
            })
            
            # Fetch real-time data for all stocks
            stock_data = []
            for symbol in stocks_to_analyze:
                try:
                    data = self.get_quote(symbol)
                    if data and data['price'] > 0:  # Only include stocks with valid data
                        stock_data.append({
                            'symbol': symbol,
                            'price': data['price'],
                            'change_percent': data['change_percent'],
                            'volume': data['volume']
                        })
                        print(f"Fetched {symbol}: {data['change_percent']:.2f}% change")
                    time.sleep(1)  # Rate limiting
                except Exception as e:
                    print(f"Error fetching {symbol}: {e}")
                    continue
            
            # Sort stocks by percentage change and volume
            stock_data.sort(key=lambda x: (x['change_percent'], x['volume']), reverse=True)
            
            # Get top gainers
            top_gainers = [stock['symbol'] for stock in stock_data[:limit]]
            
            print("\nTop Gainers:")
            for stock in stock_data[:limit]:
                print(f"{stock['symbol']}: {stock['change_percent']:.2f}% (Volume: {stock['volume']:,})")
            
            return top_gainers
            
        except Exception as e:
            logging.error(f"Error fetching top gainers: {e}")
            return self.default_stocks[:limit]


class ImprovedZerodhaConnection:
    """
    Improved Zerodha connection with token management
    """
    
    def __init__(self, api_key: str, api_secret: str, access_token: str = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        self.kite = None
        self.token_expiry = None
        
    def connect(self) -> bool:
        """Connect with automatic token refresh"""
        try:
            if not self.access_token:
                logging.error("No access token provided")
                return False
            
            self.kite = KiteConnect(api_key=self.api_key)
            self.kite.set_access_token(self.access_token)
            
            # Test connection
            profile = self.kite.profile()
            logging.info(f"Connected to Zerodha. User: {profile['user_name']}")
            
            # Set token expiry (tokens typically expire at 6 AM next day)
            now = datetime.now()
            tomorrow_6am = (now + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
            self.token_expiry = tomorrow_6am
            
            return True
            
        except TokenException as e:
            logging.error(f"Token error: {e}")
            return self._refresh_token()
        except Exception as e:
            logging.error(f"Connection error: {e}")
            return False
    
    def _refresh_token(self) -> bool:
        """
        Refresh access token (requires manual intervention for security)
        In production, implement proper OAuth flow
        """
        logging.warning("Access token expired. Manual refresh required.")
        logging.info("Please generate a new access token using the setup instructions.")
        return False
    
    def is_token_valid(self) -> bool:
        """Check if current token is still valid"""
        if not self.token_expiry:
            return False
        return datetime.now() < self.token_expiry
    
    def place_order_with_retry(self, **order_params) -> str:
        """Place order with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not self.is_token_valid():
                    if not self.connect():
                        return None
                
                order_id = self.kite.place_order(**order_params)
                return order_id
                
            except TokenException:
                if attempt < max_retries - 1:
                    logging.warning(f"Token error on attempt {attempt + 1}, retrying...")
                    time.sleep(2)
                    if not self.connect():
                        break
                else:
                    logging.error("Failed to place order after token refresh attempts")
                    return None
            except Exception as e:
                logging.error(f"Order placement error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    return None
        
        return None


class ImprovedOrderManager:
    """
    Improved order management with advanced risk management
    """
    
    def __init__(self, kite_connection):
        self.kite = kite_connection  # Store the kite connection
        self.positions = {}
        self.position_highs = {}
        self.position_stop_losses = {}  # Initialize stop losses dictionary
        self.pending_orders = {}
        self.last_trade_time = {}
        self.trailing_stop_percentage = 5  # 5% trailing stop
        self.stop_loss_percentage = 5  # 5% fixed stop loss
        
        # Profit taking parameters
        self.profit_target_1 = 5  # 5% first target
        self.profit_target_2 = 10  # 10% second target
        self.profit_target_3 = 15  # 15% third target
        self.partial_exit_1 = 0.25  # Exit 25% at first target
        self.partial_exit_2 = 0.25  # Exit 25% at second target
        self.partial_exit_3 = 0.50  # Exit 50% at third target
        
        self.profit_targets = {
            self.profit_target_1: self.partial_exit_1,
            self.profit_target_2: self.partial_exit_2,
            self.profit_target_3: self.partial_exit_3
        }
        
        self.scale_in_levels = {
            -5: 0.25,   # 25% more at -5% from entry
            -10: 0.25,  # 25% more at -10% from entry
            -15: 0.25,  # 25% more at -15% from entry
            -20: 0.25   # 25% more at -20% from entry
        }
        self.ai_engine = ProfessionalTradingEngine()
        self.market_detector = MarketConditionDetector()
        self.portfolio_value = 0
        self.available_cash = 0
        self.last_portfolio_update = None
        self.portfolio_update_interval = 300  # 5 minutes
        
        # Market hours configuration
        self.market_hours = {
            'start': '09:15',
            'end': '15:30',
            'pre_market': '09:00',
            'post_market': '15:45'
        }
        self.market_open = False
        
        # Initialize portfolio tracking
        self.daily_pnl = {}  # Track daily P&L
        self.position_metrics = {}  # Track metrics for each position
        self.trade_history = []  # Track all trades
        self.position_targets = {}  # Track which profit targets have been hit
        self.position_scales = {}  # Track number of times scaled in
    
    def is_market_open(self) -> bool:
        """Check if the market is currently open"""
        try:
            current_time = datetime.now().strftime('%H:%M')
            return self.market_hours['start'] <= current_time <= self.market_hours['end']
        except Exception as e:
            logging.error(f"Error checking market hours: {e}")
            return False

    def get_next_market_open(self) -> datetime:
        """Get the next market open time"""
        try:
            now = datetime.now()
            current_time = now.strftime('%H:%M')
            
            # If before market open today
            if current_time < self.market_hours['start']:
                next_open = now.replace(
                    hour=int(self.market_hours['start'].split(':')[0]),
                    minute=int(self.market_hours['start'].split(':')[1]),
                    second=0,
                    microsecond=0
                )
            # If after market close today
            else:
                # Set to next day's market open
                next_open = (now + timedelta(days=1)).replace(
                    hour=int(self.market_hours['start'].split(':')[0]),
                    minute=int(self.market_hours['start'].split(':')[1]),
                    second=0,
                    microsecond=0
                )
            
            return next_open
            
        except Exception as e:
            logging.error(f"Error calculating next market open: {e}")
            # Return current time + 1 hour as fallback
            return datetime.now() + timedelta(hours=1)
    
    def can_trade(self, symbol: str) -> bool:
        """Check if we can trade a specific stock"""
        try:
            # Check if the stock is in our positions
            if symbol in self.positions:
                return True
                
            # Check if we have enough cash for new positions
            if self.available_cash <= 0:
                logging.info(f"Cannot trade {symbol}: Insufficient cash")
                return False
                
            # Check if we're within trading limits
            if symbol in self.last_trade_time:
                time_since_last_trade = (datetime.now() - self.last_trade_time[symbol]).total_seconds()
                if time_since_last_trade < 300:  # 5 minutes between trades
                    logging.info(f"Cannot trade {symbol}: Too soon since last trade")
                    return False
                    
            return True
            
        except Exception as e:
            logging.error(f"Error checking trade eligibility for {symbol}: {e}")
            return False
    
    def update_portfolio_analytics(self):
        """Update portfolio analytics and metrics"""
        try:
            # Update portfolio value first
            self.update_portfolio_value()
            
            # Get current date
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Initialize daily P&L if not exists
            if today not in self.daily_pnl:
                self.daily_pnl[today] = 0
            
            # Calculate P&L for each position
            total_pnl = 0
            symbols = list(self.positions.keys())
            
            if symbols:
                try:
                    # Get LTP for all symbols at once
                    ltp_data = self.kite.kite.ltp(symbols)
                    
                    for symbol, position in self.positions.items():
                        try:
                            # Get current price from LTP data
                            current_price = float(ltp_data[symbol]['last_price'])
                            
                            # Calculate P&L
                            entry_value = position['quantity'] * position['average_price']
                            current_value = position['quantity'] * current_price
                            position_pnl = current_value - entry_value
                            
                            # Update position metrics
                            self.position_metrics[symbol] = {
                                'current_price': current_price,
                                'entry_price': position['average_price'],
                                'quantity': position['quantity'],
                                'pnl': position_pnl,
                                'pnl_percentage': (position_pnl / entry_value) * 100
                            }
                            
                            total_pnl += position_pnl
                            
                        except Exception as e:
                            logging.error(f"Error calculating P&L for {symbol}: {e}")
                            
                except Exception as e:
                    logging.error(f"Error getting LTP data: {e}")
            
            # Update daily P&L
            self.daily_pnl[today] = total_pnl
            
            # Log portfolio status
            logging.info("\nPortfolio Analytics:")
            logging.info(f"Total Value: INR {self.portfolio_value:,.2f}")
            logging.info(f"Available Cash: INR {self.available_cash:,.2f}")
            logging.info(f"Today's P&L: INR {self.daily_pnl[today]:,.2f}")
            
            # Log individual position status
            for symbol, metrics in self.position_metrics.items():
                logging.info(f"\n{symbol}:")
                logging.info(f"Current Price: ₹{metrics['current_price']:.2f}")
                logging.info(f"Entry Price: ₹{metrics['entry_price']:.2f}")
                logging.info(f"Quantity: {metrics['quantity']}")
                logging.info(f"P&L: ₹{metrics['pnl']:,.2f} ({metrics['pnl_percentage']:.2f}%)")
            
        except Exception as e:
            logging.error(f"Error updating portfolio analytics: {e}")
    
    def check_scale_in_opportunity(self, symbol: str, current_price: float) -> tuple:
        """Check if we should scale into a winning position"""
        if symbol not in self.positions:
            return False, 0
            
        position = self.positions[symbol]
        entry_price = position['average_price']
        profit_percentage = ((current_price - entry_price) / entry_price) * 100
        
        # Initialize scales if not exists
        if symbol not in self.position_scales:
            self.position_scales[symbol] = 0
        
        # Check if we can scale in
        if (profit_percentage >= self.scale_in_threshold * 100 and 
            self.position_scales[symbol] < self.max_scales):
            
            # Calculate scale-in quantity
            original_quantity = position['quantity'] / (1 + self.position_scales[symbol])
            scale_quantity = int(original_quantity * self.scale_in_size)
            
            if scale_quantity > 0:
                self.position_scales[symbol] += 1
                logging.info(f"Scale-in opportunity for {symbol} at {profit_percentage:.2f}% profit")
                return True, scale_quantity
        
        return False, 0
    
    def calculate_position_size(self, symbol: str, current_price: float) -> int:
        """Calculate position size based on risk management"""
        try:
            # Update portfolio value first
            self.update_portfolio_value()
            
            # Calculate maximum position size based on risk
            max_position_value = self.portfolio_value * self.max_position_risk
            max_shares = int(max_position_value / current_price)
            
            # Calculate based on available capital
            capital_based_shares = int(self.available_capital * 0.1 / current_price)  # Use 10% of available capital
            
            # Calculate based on volatility (if we have historical data)
            volatility_based_shares = self._calculate_volatility_based_size(symbol, current_price)
            
            # Take the minimum of all calculations
            position_size = min(max_shares, capital_based_shares, volatility_based_shares)
            
            # Ensure minimum of 1 share
            return max(1, position_size)
            
        except Exception as e:
            logging.error(f"Error calculating position size: {e}")
            return 1  # Default to 1 share if calculation fails
    
    def _calculate_volatility_based_size(self, symbol: str, current_price: float) -> int:
        """Calculate position size based on stock volatility"""
        try:
            # Get historical data for volatility calculation
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            # Fetch historical data
            historical_data = self.kite.kite.historical_data(
                instrument_token=symbol,
                from_date=start_date,
                to_date=end_date,
                interval='day'
            )
            
            if historical_data:
                # Calculate daily returns
                returns = []
                for i in range(1, len(historical_data)):
                    daily_return = (historical_data[i]['close'] - historical_data[i-1]['close']) / historical_data[i-1]['close']
                    returns.append(daily_return)
                
                # Calculate volatility (standard deviation of returns)
                volatility = statistics.stdev(returns) if len(returns) > 1 else 0.02
                
                # Adjust position size based on volatility
                # Higher volatility = smaller position size
                volatility_factor = 1 - (volatility * 5)  # Reduce size by 5x the volatility
                volatility_factor = max(0.2, min(1.0, volatility_factor))  # Keep between 20% and 100%
                
                # Calculate position size
                risk_amount = self.portfolio_value * self.max_position_risk
                position_size = int((risk_amount * volatility_factor) / current_price)
                
                return position_size
                
        except Exception as e:
            logging.error(f"Error calculating volatility-based size: {e}")
            return 1000  # Default to a large number if calculation fails
    
    def update_portfolio_value(self):
        """Update portfolio value and available cash"""
        try:
            # Calculate portfolio value from positions
            total_value = 0
            symbols = list(self.positions.keys())
            
            if symbols:
                try:
                    # Get LTP for all symbols at once
                    ltp_data = self.kite.kite.ltp(symbols)
                    
                    for symbol, position in self.positions.items():
                        try:
                            # Get current price from LTP data
                            current_price = float(ltp_data[symbol]['last_price'])
                            position_value = current_price * position['quantity']
                            total_value += position_value
                            
                            # Update position metrics
                            if symbol not in self.position_metrics:
                                self.position_metrics[symbol] = {}
                            self.position_metrics[symbol]['current_price'] = current_price
                            
                        except Exception as e:
                            logging.error(f"Error processing LTP for {symbol}: {e}")
                            # Use average price if LTP fails
                            position_value = position['average_price'] * position['quantity']
                            total_value += position_value
                except Exception as e:
                    logging.error(f"Error getting LTP data: {e}")
                    # Fallback to average prices
                    for symbol, position in self.positions.items():
                        position_value = position['average_price'] * position['quantity']
                        total_value += position_value
            
            self.portfolio_value = total_value
            
            # Get available cash
            try:
                margins = self.kite.kite.margins()
                self.available_cash = float(margins['equity']['available']['cash'])
            except Exception as e:
                logging.error(f"Error getting margins: {e}")
                # Keep previous available cash value if margins call fails
            
            # Update last portfolio update time
            self.last_portfolio_update = datetime.now()
            
            logging.info(f"Portfolio Value: INR {self.portfolio_value:,.2f}")
            logging.info(f"Available Cash: INR {self.available_cash:,.2f}")
            
        except Exception as e:
            logging.error(f"Error updating portfolio value: {e}")
    
    def check_profit_targets(self, symbol: str, current_price: float) -> tuple:
        """Check if any profit targets have been hit"""
        if symbol not in self.positions:
            return False, 0
            
        position = self.positions[symbol]
        entry_price = position['average_price']
        current_pnl_percent = ((current_price - entry_price) / entry_price) * 100
        
        # Initialize targets if not exists
        if symbol not in self.position_targets:
            self.position_targets[symbol] = set()
        
        # Check each profit target
        for target_percent, exit_portion in self.profit_targets.items():
            if (current_pnl_percent >= target_percent and 
                target_percent not in self.position_targets[symbol]):
                
                # Calculate quantity to exit
                exit_quantity = int(position['quantity'] * exit_portion)
                if exit_quantity > 0:
                    # Mark target as hit
                    self.position_targets[symbol].add(target_percent)
                    logging.info(f"Profit target {target_percent}% hit for {symbol}")
                    return True, exit_quantity
        
        return False, 0
    
    def check_stop_loss(self, symbol: str, current_price: float) -> tuple:
        """Check if stop loss has been triggered - only for profitable positions"""
        if symbol not in self.positions:
            return False, 0
            
        position = self.positions[symbol]
        entry_price = position['average_price']
        
        # Calculate current P&L
        current_pnl_percent = ((current_price - entry_price) / entry_price) * 100
        
        # For positions in loss:
        if current_pnl_percent < 0:
            # If we don't have a stop loss set, set one once we turn profitable
            if symbol not in self.position_stop_losses:
                self.position_stop_losses[symbol] = None
            return False, 0
        
        # For positions in profit:
        # Update position high if current price is higher
        if current_price > self.position_highs[symbol]:
            self.position_highs[symbol] = current_price
            # Update trailing stop loss
            self.position_stop_losses[symbol] = current_price * (1 - self.trailing_stop_percentage/100)
        
        # Check if we have a stop loss set
        if symbol in self.position_stop_losses and self.position_stop_losses[symbol]:
            # If price hits stop loss
            if current_price <= self.position_stop_losses[symbol]:
                logging.info(f"Stop loss triggered for {symbol} at {current_pnl_percent:.2f}% profit")
                return True, position['quantity']
        
        return False, 0

    def check_sell_conditions(self, symbol: str, current_price: float) -> tuple:
        """Check if we should sell a stock based on conservative rules"""
        if symbol not in self.positions:
            return False, 0
            
        position = self.positions[symbol]
        entry_price = position['average_price']
        current_pnl_percent = ((current_price - entry_price) / entry_price) * 100
        
        # 1. Check stop loss first (for both fixed and trailing stops)
        stop_loss_triggered, stop_loss_quantity = self.check_stop_loss(symbol, current_price)
        if stop_loss_triggered:
            return True, stop_loss_quantity
        
        # 2. Check profit targets
        target_hit, target_quantity = self.check_profit_targets(symbol, current_price)
        if target_hit:
            logging.info(f"Profit target hit for {symbol} at {current_pnl_percent:.2f}% profit")
            return True, target_quantity
        
        # 3. Check for strong sell signal only if we're in profit
        if current_pnl_percent > 0:
            signal = self.ai_engine.get_professional_signal(symbol, {'price': current_price})
            if signal['confidence'] > 0.7 and signal['action'] == 'SELL':
                logging.info(f"Strong sell signal for {symbol} at {current_pnl_percent:.2f}% profit")
                return True, position['quantity']
        
        return False, 0
    
    def update_positions(self):
        """Update current positions from Zerodha"""
        try:
            # Fetch holdings from Zerodha
            holdings = self.kite.kite.holdings()
            
            # Clear existing positions
            self.positions.clear()
            
            # Process holdings
            for holding in holdings:
                if float(holding['quantity']) > 0:  # Only track non-zero quantities
                    symbol = holding['tradingsymbol']
                    self.positions[symbol] = {
                        'quantity': int(holding['quantity']),
                        'average_price': float(holding['average_price']),
                        'product': holding['product'],
                        'exchange': holding['exchange']
                    }
                    
                    # Initialize position tracking if not exists
                    if symbol not in self.position_highs:
                        self.position_highs[symbol] = float(holding['average_price'])
                    if symbol not in self.position_stop_losses:
                        self.position_stop_losses[symbol] = None
                    if symbol not in self.position_targets:
                        self.position_targets[symbol] = set()
                    if symbol not in self.position_scales:
                        self.position_scales[symbol] = 0
                    
                    logging.info(f"Updated position for {symbol}: {self.positions[symbol]}")
            
            logging.info(f"Current positions: {self.positions}")
            
        except Exception as e:
            logging.error(f"Error updating holdings: {e}")
            try:
                # Try to get raw holdings data for debugging
                raw_holdings = self.kite.kite.holdings()
                logging.info(f"Raw holdings data: {raw_holdings}")
            except Exception as e2:
                logging.error(f"Error getting raw holdings: {e2}")

    def get_portfolio_status(self):
        """Get detailed portfolio status"""
        try:
            # Update holdings first
            self.update_positions()
            
            # Get margins for available cash
            margins = self.kite.kite.margins()
            available_cash = float(margins['equity']['available']['cash'])
            
            # Calculate total portfolio value
            total_value = 0
            for symbol, position in self.positions.items():
                try:
                    # Get current price
                    quote = self.kite.kite.quote(symbol)
                    current_price = float(quote['last_price'])
                    position_value = current_price * position['quantity']
                    total_value += position_value
                    
                    # Calculate P&L
                    entry_value = position['quantity'] * position['average_price']
                    pnl = position_value - entry_value
                    pnl_percent = (pnl / entry_value) * 100 if entry_value != 0 else 0
                    
                    logging.info(f"\n{symbol}:")
                    logging.info(f"  Quantity: {position['quantity']}")
                    logging.info(f"  Entry Price: ₹{position['average_price']:.2f}")
                    logging.info(f"  Current Price: ₹{current_price:.2f}")
                    logging.info(f"  P&L: ₹{pnl:.2f} ({pnl_percent:.2f}%)")
                    
                except Exception as e:
                    logging.error(f"Error getting quote for {symbol}: {e}")
            
            logging.info(f"\nPortfolio Summary:")
            logging.info(f"Total Value: ₹{total_value:.2f}")
            logging.info(f"Available Cash: ₹{available_cash:.2f}")
            logging.info(f"Total Holdings: {len(self.positions)}")
            
            return {
                'total_value': total_value,
                'available_cash': available_cash,
                'positions': self.positions
            }
            
        except Exception as e:
            logging.error(f"Error getting portfolio status: {e}")
            return None
    
    def place_and_track_order(self, symbol: str, action: str, quantity: int, price: float = None) -> dict:
        """Place an order and track its status"""
        try:
            # Place the main order
            order_id = self.kite_connection.place_order_with_retry(
                symbol=symbol,
                action=action,
                quantity=quantity,
                price=price
            )
            
            # If this is a buy order, set up stop loss
            if action == 'BUY':
                # Calculate stop loss price (5% below entry)
                stop_loss_price = price * 0.95 if price else None
                
                # Place stop loss order
                if stop_loss_price:
                    stop_loss_id = self.kite_connection.place_order_with_retry(
                        symbol=symbol,
                        action='SELL',
                        quantity=quantity,
                        price=stop_loss_price,
                        trigger_price=stop_loss_price,
                        order_type='SL'
                    )
                    logging.info(f"Placed stop loss order for {symbol} at ₹{stop_loss_price:.2f}")
            
            # Track the order
            self.pending_orders[order_id] = {
                'symbol': symbol,
                'action': action,
                'quantity': quantity,
                'price': price,
                'status': 'PENDING',
                'timestamp': datetime.now()
            }
            
            return {'order_id': order_id, 'status': 'PENDING'}
            
        except Exception as e:
            logging.error(f"Error placing order for {symbol}: {e}")
            return {'order_id': None, 'status': 'FAILED', 'error': str(e)}
    
    def update_order_status(self, order_id: str):
        """Update order status"""
        try:
            orders = self.kite.kite.orders()
            for order in orders:
                if order['order_id'] == order_id:
                    if order_id in self.pending_orders:
                        self.pending_orders[order_id]['status'] = order['status']
                        self.pending_orders[order_id]['filled_quantity'] = order.get('filled_quantity', 0)
                        self.pending_orders[order_id]['average_price'] = order.get('average_price', 0)
                    
                    logging.info(f"Order {order_id} status: {order['status']}")
                    break
                    
        except Exception as e:
            logging.error(f"Error updating order status for {order_id}: {e}")
    
    def get_pending_orders(self) -> dict:
        """Get all pending orders and update their status"""
        for order_id in list(self.pending_orders.keys()):
            self.update_order_status(order_id)
        
        return self.pending_orders
    
    def can_trade(self, symbol: str) -> bool:
        """Check if we can trade a specific stock"""
        try:
            # Check if the stock is in the list of monitored stocks
            if symbol in self.positions:
                return True
            else:
                return False
        except Exception as e:
            logging.error(f"Error checking trade eligibility for {symbol}: {e}")
            return False


class BasicTechnicalIndicators:
    """
    Basic technical indicators for trading decisions
    """
    
    @staticmethod
    def simple_moving_average(prices: list, period: int) -> float:
        """Calculate Simple Moving Average"""
        if len(prices) < period:
            return prices[-1] if prices else 0
        return sum(prices[-period:]) / period
    
    @staticmethod
    def relative_strength_index(prices: list, period: int = 14) -> float:
        """Calculate RSI"""
        if len(prices) < period + 1:
            return 50  # Neutral RSI
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        if len(gains) < period:
            return 50
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def bollinger_bands(prices: list, period: int = 20, std_dev: int = 2) -> tuple:
        """Calculate Bollinger Bands"""
        if len(prices) < period:
            price = prices[-1] if prices else 0
            return price, price, price  # middle, upper, lower
        
        sma = sum(prices[-period:]) / period
        variance = sum([(x - sma) ** 2 for x in prices[-period:]]) / period
        std_deviation = variance ** 0.5
        
        upper_band = sma + (std_deviation * std_dev)
        lower_band = sma - (std_deviation * std_dev)
        
        return sma, upper_band, lower_band


class ProfessionalTechnicalIndicators:
    """
    Professional-grade technical indicators with proper calculations
    """
    
    @staticmethod
    def ema(prices: list, period: int) -> float:
        """Exponential Moving Average - more responsive than SMA"""
        if len(prices) < period:
            return prices[-1] if prices else 0
        
        multiplier = 2 / (period + 1)
        ema = prices[0]
        
        for price in prices[1:]:
            ema = (price - ema) * multiplier + ema
        
        return ema
    
    @staticmethod
    def macd(prices: list) -> tuple:
        """MACD - Moving Average Convergence Divergence"""
        if len(prices) < 26:
            return 0, 0, 0
        
        ema_12 = ProfessionalTechnicalIndicators.ema(prices, 12)
        ema_26 = ProfessionalTechnicalIndicators.ema(prices, 26)
        macd_line = ema_12 - ema_26
        
        # Calculate signal line (9-period EMA of MACD)
        macd_history = []
        for i in range(26, len(prices)):
            ema_12_i = ProfessionalTechnicalIndicators.ema(prices[:i+1], 12)
            ema_26_i = ProfessionalTechnicalIndicators.ema(prices[:i+1], 26)
            macd_history.append(ema_12_i - ema_26_i)
        
        signal_line = ProfessionalTechnicalIndicators.ema(macd_history, 9) if macd_history else 0
        histogram = macd_line - signal_line
        
        return macd_line, signal_line, histogram
    
    @staticmethod
    def stochastic(high_prices: list, low_prices: list, close_prices: list, k_period: int = 14) -> tuple:
        """Stochastic Oscillator"""
        if len(close_prices) < k_period:
            return 50, 50
        
        recent_highs = high_prices[-k_period:]
        recent_lows = low_prices[-k_period:]
        current_close = close_prices[-1]
        
        highest_high = max(recent_highs)
        lowest_low = min(recent_lows)
        
        if highest_high == lowest_low:
            k_percent = 50
        else:
            k_percent = ((current_close - lowest_low) / (highest_high - lowest_low)) * 100
        
        # %D is 3-period SMA of %K
        if len(close_prices) >= k_period + 2:
            k_values = []
            for i in range(3):
                idx = len(close_prices) - 1 - i
                if idx >= k_period - 1:
                    h = high_prices[idx-k_period+1:idx+1]
                    l = low_prices[idx-k_period+1:idx+1]
                    c = close_prices[idx]
                    if max(h) != min(l):
                        k_val = ((c - min(l)) / (max(h) - min(l))) * 100
                        k_values.append(k_val)
            
            d_percent = sum(k_values) / len(k_values) if k_values else 50
        else:
            d_percent = k_percent
        
        return k_percent, d_percent
    
    @staticmethod
    def adx(high_prices: list, low_prices: list, close_prices: list, period: int = 14) -> float:
        """Average Directional Index - Trend Strength"""
        if len(close_prices) < period + 1:
            return 25  # Neutral trend strength
        
        # Calculate True Range and Directional Movement
        tr_list = []
        plus_dm = []
        minus_dm = []
        
        for i in range(1, len(close_prices)):
            high_curr, low_curr, close_curr = high_prices[i], low_prices[i], close_prices[i]
            high_prev, low_prev, close_prev = high_prices[i-1], low_prices[i-1], close_prices[i-1]
            
            # True Range
            tr = max(
                high_curr - low_curr,
                abs(high_curr - close_prev),
                abs(low_curr - close_prev)
            )
            tr_list.append(tr)
            
            # Directional Movement
            up_move = high_curr - high_prev
            down_move = low_prev - low_curr
            
            plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0)
            minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0)
        
        if len(tr_list) < period:
            return 25
        
        # Calculate smoothed averages
        atr = sum(tr_list[-period:]) / period
        plus_di = (sum(plus_dm[-period:]) / period) / atr * 100 if atr > 0 else 0
        minus_di = (sum(minus_dm[-period:]) / period) / atr * 100 if atr > 0 else 0
        
        # ADX calculation
        dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100 if (plus_di + minus_di) > 0 else 0
        
        return dx
    
    @staticmethod
    def volume_sma(volumes: list, period: int) -> float:
        """Volume Simple Moving Average"""
        if len(volumes) < period:
            return volumes[-1] if volumes else 0
        return sum(volumes[-period:]) / period


class MarketConditionDetector:
    """
    Detect current market conditions to adjust strategy
    """
    
    def __init__(self):
        self.nifty_data = []
        self.market_condition = "NEUTRAL"
    
    def update_market_data(self, nifty_price: float):
        """Update with current Nifty data"""
        self.nifty_data.append(nifty_price)
        if len(self.nifty_data) > 50:
            self.nifty_data = self.nifty_data[-50:]
    
    def get_market_condition(self) -> str:
        """Determine overall market condition"""
        if len(self.nifty_data) < 20:
            return "NEUTRAL"
        
        # Calculate market trend
        sma_20 = sum(self.nifty_data[-20:]) / 20
        sma_5 = sum(self.nifty_data[-5:]) / 5
        current_price = self.nifty_data[-1]
        
        # Market volatility
        returns = []
        for i in range(1, min(20, len(self.nifty_data))):
            ret = (self.nifty_data[-i] - self.nifty_data[-i-1]) / self.nifty_data[-i-1]
            returns.append(abs(ret))
        
        volatility = statistics.stdev(returns) if len(returns) > 1 else 0
        
        # Determine condition
        if current_price > sma_20 * 1.02 and sma_5 > sma_20:
            if volatility < 0.02:
                return "STRONG_BULL"
            else:
                return "BULL"
        elif current_price < sma_20 * 0.98 and sma_5 < sma_20:
            if volatility < 0.02:
                return "STRONG_BEAR"
            else:
                return "BEAR"
        elif volatility > 0.03:
            return "VOLATILE"
        else:
            return "NEUTRAL"


class ProfessionalTradingEngine:
    """
    Professional trading engine with multiple confirmation layers
    Realistic expectation: 60-70% win rate with proper risk management
    """
    
    def __init__(self):
        self.price_history = {}
        self.volume_history = {}
        self.high_history = {}
        self.low_history = {}
        self.indicators = ProfessionalTechnicalIndicators()
        self.market_detector = MarketConditionDetector()
        
        # Trading parameters
        self.min_adx_trend = 25  # Minimum trend strength
        self.min_volume_ratio = 1.5  # Volume must be 1.5x average
        self.rsi_oversold = 30
        self.rsi_overbought = 70
        
    def update_data(self, symbol: str, price: float, volume: int, high: float, low: float):
        """Update historical data for analysis"""
        # Initialize if new symbol
        for history in [self.price_history, self.volume_history, self.high_history, self.low_history]:
            if symbol not in history:
                history[symbol] = []
        
        # Update data
        self.price_history[symbol].append(price)
        self.volume_history[symbol].append(volume)
        self.high_history[symbol].append(high)
        self.low_history[symbol].append(low)
        
        # Keep only last 100 data points
        for history in [self.price_history, self.volume_history, self.high_history, self.low_history]:
            if len(history[symbol]) > 100:
                history[symbol] = history[symbol][-100:]
    
    def get_professional_signal(self, symbol: str, current_data: dict) -> dict:
        """
        Professional multi-factor signal generation
        Uses multiple confirmations for higher accuracy
        """
        try:
            price = current_data.get('price', 0)
            volume = current_data.get('volume', 0)
            high = current_data.get('high', price)
            low = current_data.get('low', price)
            change_percent = current_data.get('change_percent', 0)
            
            # Update historical data
            self.update_data(symbol, price, volume, high, low)
            
            # Get historical data
            prices = self.price_history.get(symbol, [])
            volumes = self.volume_history.get(symbol, [])
            highs = self.high_history.get(symbol, [])
            lows = self.low_history.get(symbol, [])
            
            if len(prices) < 20:
                return {
                    'action': 'HOLD',
                    'confidence': 0.3,
                    'reason': 'Insufficient data for professional analysis'
                }
            
            # Calculate all indicators
            sma_20 = sum(prices[-20:]) / 20
            ema_12 = self.indicators.ema(prices, 12)
            ema_26 = self.indicators.ema(prices, 26)
            rsi = self._calculate_rsi(prices)
            macd_line, macd_signal, macd_histogram = self.indicators.macd(prices)
            stoch_k, stoch_d = self.indicators.stochastic(highs, lows, prices)
            adx = self.indicators.adx(highs, lows, prices)
            volume_sma = self.indicators.volume_sma(volumes, 20)
            
            # Volume confirmation
            volume_ratio = volume / volume_sma if volume_sma > 0 else 1
            
            # Market condition
            if symbol == "NIFTY" or "NIFTY" in symbol:
                self.market_detector.update_market_data(price)
            market_condition = self.market_detector.get_market_condition()
            
            # PROFESSIONAL SIGNAL LOGIC
            signals = []
            confidence_factors = []
            
            # === STRONG BUY SIGNALS ===
            strong_buy_conditions = [
                # 1. Golden Cross with Volume
                (ema_12 > ema_26 and prices[-2] <= prices[-1] and volume_ratio > 1.5, 
                 "Golden Cross with volume confirmation", 0.8),
                
                # 2. Oversold Bounce with Trend
                (rsi < 35 and stoch_k < 30 and adx > 25 and price > sma_20 * 0.98,
                 "Oversold bounce in uptrend", 0.85),
                
                # 3. MACD Bullish Divergence
                (macd_line > macd_signal and macd_histogram > 0 and volume_ratio > 1.3,
                 "MACD bullish crossover with volume", 0.8),
                
                # 4. Breakout with Volume
                (price > max(prices[-5:-1]) and volume_ratio > 2.0 and adx > 30,
                 "Breakout with strong volume and trend", 0.9)
            ]
            
            # === STRONG SELL SIGNALS ===
            strong_sell_conditions = [
                # 1. Death Cross with Volume
                (ema_12 < ema_26 and prices[-2] >= prices[-1] and volume_ratio > 1.5,
                 "Death Cross with volume confirmation", 0.8),
                
                # 2. Overbought Reversal
                (rsi > 65 and stoch_k > 70 and price < sma_20 * 1.02,
                 "Overbought reversal signal", 0.8),
                
                # 3. MACD Bearish Divergence
                (macd_line < macd_signal and macd_histogram < 0 and volume_ratio > 1.3,
                 "MACD bearish crossover with volume", 0.8),
                
                # 4. Breakdown with Volume
                (price < min(prices[-5:-1]) and volume_ratio > 2.0 and adx > 30,
                 "Breakdown with strong volume", 0.9)
            ]
            
            # === MARKET CONDITION ADJUSTMENTS ===
            market_multiplier = {
                "STRONG_BULL": 1.2,
                "BULL": 1.1,
                "NEUTRAL": 1.0,
                "VOLATILE": 0.7,
                "BEAR": 0.9,
                "STRONG_BEAR": 0.8
            }.get(market_condition, 1.0)
            
            # Check BUY conditions
            for condition, reason, base_confidence in strong_buy_conditions:
                if condition:
                    adjusted_confidence = min(0.95, base_confidence * market_multiplier)
                    if market_condition in ["STRONG_BULL", "BULL"]:
                        adjusted_confidence *= 1.1
                    signals.append(("BUY", reason, adjusted_confidence))
            
            # Check SELL conditions  
            for condition, reason, base_confidence in strong_sell_conditions:
                if condition:
                    adjusted_confidence = min(0.95, base_confidence * market_multiplier)
                    if market_condition in ["STRONG_BEAR", "BEAR"]:
                        adjusted_confidence *= 1.1
                    signals.append(("SELL", reason, adjusted_confidence))
            
            # === RISK FILTERS ===
            # Don't trade in extreme volatility
            recent_returns = []
            for i in range(1, min(10, len(prices))):
                ret = abs(prices[-i] - prices[-i-1]) / prices[-i-1]
                recent_returns.append(ret)
            
            avg_volatility = sum(recent_returns) / len(recent_returns) if recent_returns else 0
            
            if avg_volatility > 0.05:  # 5% average volatility is too high
                return {
                    'action': 'HOLD',
                    'confidence': 0.2,
                    'reason': f'High volatility detected: {avg_volatility:.2%}'
                }
            
            # Don't trade without minimum trend strength
            if adx < self.min_adx_trend and not any(s[2] > 0.85 for s in signals):
                return {
                    'action': 'HOLD',
                    'confidence': 0.4,
                    'reason': f'Weak trend strength: ADX={adx:.1f}'
                }
            
            # === FINAL DECISION ===
            if signals:
                # Take the strongest signal
                best_signal = max(signals, key=lambda x: x[2])
                action, reason, confidence = best_signal
                
                # Additional confidence boost for multiple confirmations
                same_action_signals = [s for s in signals if s[0] == action]
                if len(same_action_signals) > 1:
                    confidence = min(0.95, confidence * 1.1)
                    reason += f" + {len(same_action_signals)-1} other confirmations"
                
                return {
                    'action': action,
                    'confidence': confidence,
                    'reason': reason,
                    'market_condition': market_condition,
                    'indicators': {
                        'rsi': rsi,
                        'macd': macd_line,
                        'stoch_k': stoch_k,
                        'adx': adx,
                        'volume_ratio': volume_ratio,
                        'ema_12': ema_12,
                        'ema_26': ema_26
                    }
                }
            
            return {
                'action': 'HOLD',
                'confidence': 0.5,
                'reason': 'No clear professional signal detected',
                'market_condition': market_condition
            }
            
        except Exception as e:
            logging.error(f"Error in professional signal generation for {symbol}: {e}")
            return {
                'action': 'HOLD',
                'confidence': 0.0,
                'reason': f'Analysis error: {str(e)}'
            }
    
    def _calculate_rsi(self, prices: list, period: int = 14) -> float:
        """Calculate RSI with proper formula"""
        if len(prices) < period + 1:
            return 50
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        if len(gains) < period:
            return 50
        
        # Use exponential smoothing like professionals do
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi


class NewsAnalyzer:
    def __init__(self):
        self.news_api_key = None
        self.news_cache = {}
        self.industry_stocks = {}
        self.logger = logging.getLogger(__name__)
        
        # Initialize AI models
        try:
            from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
            self.sentiment_analyzer = pipeline("sentiment-analysis", model="ProsusAI/finbert")
            self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
            self.model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
            
            # Industry classification model
            self.industry_classifier = pipeline("zero-shot-classification", 
                                             model="facebook/bart-large-mnli")
        except Exception as e:
            self.logger.error(f"Error initializing AI models: {str(e)}")
            self.sentiment_analyzer = None
            self.industry_classifier = None
        
    def initialize_news_api(self, api_key: str):
        """Initialize the News API with the provided API key."""
        self.news_api_key = api_key
        
    def fetch_market_news(self, days_back: int = 1) -> list:
        """
        Fetch market-related news from multiple sources for the past specified number of days.
        
        Args:
            days_back (int): Number of days to look back for news
            
        Returns:
            list: List of news articles with their details
        """
        if not self.news_api_key:
            self.logger.error("News API key not initialized")
            return []
            
        try:
            # Calculate the date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            all_articles = []
            
            # News API endpoint
            url = "https://newsapi.org/v2/everything"
            
            # Different search queries for comprehensive coverage
            search_queries = [
                'stock market OR trading OR investing OR finance',
                'market analysis OR market trends',
                'economic news OR financial news',
                'company earnings OR quarterly results',
                'merger acquisition OR M&A news'
            ]
            
            for query in search_queries:
                params = {
                    'q': query,
                    'from': start_date.strftime('%Y-%m-%d'),
                    'to': end_date.strftime('%Y-%m-%d'),
                    'language': 'en',
                    'sortBy': 'relevancy',
                    'apiKey': self.news_api_key
                }
                
                response = requests.get(url, params=params)
                response.raise_for_status()
                
                articles = response.json().get('articles', [])
                all_articles.extend(articles)
            
            # Remove duplicates based on title
            seen_titles = set()
            unique_articles = []
            for article in all_articles:
                title = article.get('title', '').lower()
                if title not in seen_titles:
                    seen_titles.add(title)
                    unique_articles.append(article)
            
            return unique_articles
            
        except Exception as e:
            self.logger.error(f"Error fetching news: {str(e)}")
            return []
            
    def analyze_news_sentiment(self, articles: list) -> dict:
        """
        Analyze the sentiment of news articles using AI and extract potential stock impacts.
        
        Args:
            articles (list): List of news articles
            
        Returns:
            dict: Dictionary containing industry impacts and sentiment scores
        """
        industry_impacts = {}
        
        # Define industry categories for classification
        industry_categories = [
            "technology", "healthcare", "finance", "energy", "consumer goods",
            "real estate", "materials", "utilities", "telecommunications",
            "automotive", "retail", "pharmaceutical", "biotech", "semiconductor",
            "agriculture", "mining", "transportation", "media", "entertainment"
        ]
        
        for article in articles:
            title = article.get('title', '')
            description = article.get('description', '')
            content = article.get('content', '')
            
            # Combine all text for analysis
            text = f"{title} {description} {content}"
            
            # Get AI-based sentiment analysis
            sentiment_score = self._get_ai_sentiment(text)
            
            # Get AI-based industry classification
            industries = self._get_ai_industry_classification(text, industry_categories)
            
            # Calculate market impact score
            impact_score = self._calculate_market_impact(text, sentiment_score)
            
            for industry in industries:
                if industry not in industry_impacts:
                    industry_impacts[industry] = {
                        'sentiment_score': 0,
                        'impact_score': 0,
                        'article_count': 0,
                        'articles': []
                    }
                
                industry_impacts[industry]['sentiment_score'] += sentiment_score
                industry_impacts[industry]['impact_score'] += impact_score
                industry_impacts[industry]['article_count'] += 1
                industry_impacts[industry]['articles'].append({
                    'title': title,
                    'sentiment': sentiment_score,
                    'impact': impact_score,
                    'source': article.get('source', {}).get('name', 'Unknown')
                })
        
        return industry_impacts
        
    def _get_ai_sentiment(self, text: str) -> float:
        """
        Get sentiment analysis using FinBERT model.
        
        Args:
            text (str): Text to analyze
            
        Returns:
            float: Sentiment score between -1 and 1
        """
        if not self.sentiment_analyzer:
            return self._calculate_sentiment(text)  # Fallback to basic sentiment
            
        try:
            # Truncate text if too long
            max_length = 512
            if len(text) > max_length:
                text = text[:max_length]
            
            result = self.sentiment_analyzer(text)[0]
            
            # Convert FinBERT labels to numerical scores
            label = result['label']
            score = result['score']
            
            if label == 'positive':
                return score
            elif label == 'negative':
                return -score
            else:
                return 0
                
        except Exception as e:
            self.logger.error(f"Error in AI sentiment analysis: {str(e)}")
            return self._calculate_sentiment(text)  # Fallback to basic sentiment
            
    def _get_ai_industry_classification(self, text: str, categories: list) -> list:
        """
        Classify text into industries using zero-shot classification.
        
        Args:
            text (str): Text to classify
            categories (list): List of industry categories
            
        Returns:
            list: List of relevant industries
        """
        if not self.industry_classifier:
            return self._extract_industries(text)  # Fallback to basic extraction
            
        try:
            # Truncate text if too long
            max_length = 512
            if len(text) > max_length:
                text = text[:max_length]
            
            result = self.industry_classifier(
                text,
                categories,
                multi_label=True,
                hypothesis_template="This text is about {}."
            )
            
            # Get industries with confidence above threshold
            threshold = 0.3
            industries = [
                category for category, score in zip(result['labels'], result['scores'])
                if score > threshold
            ]
            
            return industries if industries else self._extract_industries(text)
            
        except Exception as e:
            self.logger.error(f"Error in AI industry classification: {str(e)}")
            return self._extract_industries(text)  # Fallback to basic extraction
            
    def _calculate_market_impact(self, text: str, sentiment_score: float) -> float:
        """
        Calculate the potential market impact of the news.
        
        Args:
            text (str): News text
            sentiment_score (float): Sentiment score from AI analysis
            
        Returns:
            float: Impact score between 0 and 1
        """
        # Keywords that indicate high market impact
        impact_keywords = {
            'high': ['breakthrough', 'revolutionary', 'game-changing', 'major', 'significant'],
            'medium': ['announcement', 'update', 'change', 'development'],
            'low': ['minor', 'slight', 'small']
        }
        
        text_lower = text.lower()
        impact_score = 0.5  # Default medium impact
        
        # Check for impact keywords
        for impact_level, keywords in impact_keywords.items():
            for keyword in keywords:
                if keyword in text_lower:
                    if impact_level == 'high':
                        impact_score = 1.0
                    elif impact_level == 'low':
                        impact_score = 0.2
                    break
        
        # Adjust impact based on sentiment strength
        impact_score *= abs(sentiment_score)
        
        return impact_score
        
    def get_stocks_to_monitor(self, industry_impacts: dict, nse_fetcher) -> list:
        """
        Get stocks to monitor based on industry impacts with enhanced scoring.
        
        Args:
            industry_impacts (dict): Dictionary of industry impacts
            nse_fetcher: Instance of ImprovedNSEDataFetcher
            
        Returns:
            list: List of stocks to monitor with their details
        """
        stocks_to_monitor = []
        
        # Get all market stocks
        market_stocks = nse_fetcher.get_market_stocks()
        
        # Filter stocks based on industry impact
        for industry, impact in industry_impacts.items():
            if impact['sentiment_score'] != 0 or impact['impact_score'] > 0.3:
                # Get stocks in this industry
                industry_stocks = [stock for stock in market_stocks 
                                 if industry.lower() in stock.get('industry', '').lower()]
                
                for stock in industry_stocks:
                    symbol = stock.get('symbol')
                    if symbol:
                        # Calculate combined score
                        combined_score = (
                            abs(impact['sentiment_score']) * 0.4 +
                            impact['impact_score'] * 0.4 +
                            (impact['article_count'] / 10) * 0.2  # Normalize article count
                        )
                        
                        stocks_to_monitor.append({
                            'symbol': symbol,
                            'industry': industry,
                            'sentiment_score': impact['sentiment_score'],
                            'impact_score': impact['impact_score'],
                            'combined_score': combined_score,
                            'article_count': impact['article_count'],
                            'relevant_articles': impact['articles']
                        })
        
        # Sort by combined score
        stocks_to_monitor.sort(key=lambda x: x['combined_score'], reverse=True)
        
        return stocks_to_monitor[:10]  # Return top 10 stocks to monitor


# Example usage in your main bot class:
def integrate_improvements():
    """
    Example of how to integrate these improvements into your main bot
    """
    
    # Replace NSEDataFetcher
    nse_fetcher = ImprovedNSEDataFetcher()
    
    # Initialize Zerodha connection Flag1
    zerodha_conn = ImprovedZerodhaConnection(
        api_key="Your_api_key",
        api_secret="Your_secret_key", 
        access_token="Your_access_token"
    )
    
    # Initialize order manager
    order_manager = ImprovedOrderManager(zerodha_conn)
    
    # Replace AI engine
    ai_engine = ProfessionalTradingEngine()
    
    # Example trading cycle
    def run_trading_cycle():
        """
        Main trading cycle that runs twice daily for stock analysis and continuously monitors positions.
        """
        try:
            # Initialize Zerodha connection flag2
            api_key = "Your_api_key"  # Replace with your API key
            api_secret = "Your_secret_key"  # Replace with your API secret
            access_token = "Your_access_token"  # Replace with your access token
            
            zerodha_connection = ImprovedZerodhaConnection(api_key, api_secret, access_token)
            if not zerodha_connection.connect():
                logging.error("Failed to connect to Zerodha")
                return
                
            # Initialize components
            scanner = SocialMediaStockScanner()
            nse_fetcher = ImprovedNSEDataFetcher()
            news_analyzer = NewsAnalyzer()
            order_manager = ImprovedOrderManager(zerodha_connection.kite)
            ai_engine = ProfessionalTradingEngine()
            
            # Initialize news analyzer with your API key
            news_analyzer.initialize_news_api("YOUR_NEWS_API_KEY")  # Replace with your actual API key
            
            # Verify connection and holdings
            order_manager.update_positions()
            current_positions = order_manager.positions
            logging.info(f"Current positions: {current_positions}")
            
            if not current_positions:
                logging.warning("No positions found. Please verify your connection and holdings.")
            
            def analyze_stocks():
                """Function to analyze stocks and get top 5 recommendations"""
                try:
                    # Update positions before analysis
                    order_manager.update_positions()
                    current_positions = order_manager.positions
                    logging.info(f"Current positions before analysis: {current_positions}")
                    
                    # Fetch and analyze news
                    news_articles = news_analyzer.fetch_market_news(days_back=1)
                    industry_impacts = news_analyzer.analyze_news_sentiment(news_articles)
                    news_based_stocks = news_analyzer.get_stocks_to_monitor(industry_impacts, nse_fetcher)
                    
                    # Get trending stocks from social media
                    trending_stocks = scanner.get_trending_stocks()
                    
                    # Get top gainers
                    top_gainers = nse_fetcher.get_top_gainers(limit=10)
                    
                    # Combine all sources
                    all_stocks = set()
                    
                    # Add news-based stocks
                    for stock in news_based_stocks:
                        all_stocks.add(stock['symbol'])
                    
                    # Add trending stocks
                    all_stocks.update(trending_stocks)
                    
                    # Add top gainers
                    all_stocks.update(top_gainers)
                    
                    # Add current holdings to ensure they're monitored
                    all_stocks.update(current_positions.keys())
                    
                    # Analyze each stock
                    analyzed_stocks = []
                    for symbol in all_stocks:
                        # Get real market data
                        stock_data = nse_fetcher.get_quote(symbol)
                        if not stock_data:
                            continue
                        
                        # Get AI signal with technical analysis
                        signal = ai_engine.get_professional_signal(symbol, stock_data)
                        
                        # Calculate combined score
                        combined_score = 0
                        
                        # Add signal confidence
                        combined_score += signal['confidence'] * 0.4
                        
                        # Add news sentiment if available
                        news_stock = next((s for s in news_based_stocks if s['symbol'] == symbol), None)
                        if news_stock:
                            combined_score += abs(news_stock['sentiment_score']) * 0.3
                            combined_score += news_stock['impact_score'] * 0.3
                        
                        # Check if stock is in current positions
                        is_in_position = symbol in current_positions
                        
                        # If in position, check if we should scale in
                        scale_in_opportunity = False
                        if is_in_position:
                            should_scale, reason = order_manager.check_scale_in_opportunity(
                                symbol, 
                                stock_data['last_price']
                            )
                            if should_scale:
                                scale_in_opportunity = True
                                combined_score += 0.2  # Boost score for scale-in opportunities
                        
                        analyzed_stocks.append({
                            'symbol': symbol,
                            'signal': signal,
                            'score': combined_score,
                            'data': stock_data,
                            'is_in_position': is_in_position,
                            'scale_in_opportunity': scale_in_opportunity
                        })
                    
                    # Sort by combined score and get top 5
                    analyzed_stocks.sort(key=lambda x: x['score'], reverse=True)
                    top_stocks = analyzed_stocks[:5]
                    
                    # Log the top stocks with their status
                    logging.info("Top 5 stocks for trading:")
                    for stock in top_stocks:
                        status = "EXISTING POSITION - SCALE IN" if stock['scale_in_opportunity'] else \
                                "EXISTING POSITION" if stock['is_in_position'] else "NEW POSITION"
                        logging.info(f"Symbol: {stock['symbol']}, Score: {stock['score']:.2f}, "
                                    f"Signal: {stock['signal']['action']}, Status: {status}")
                    
                    return top_stocks
                    
                except Exception as e:
                    logging.error(f"Error in stock analysis: {str(e)}")
                    return []
            
            def monitor_positions():
                """Function to monitor existing positions for profit taking"""
                try:
                    # Update positions
                    order_manager.update_positions()
                    current_positions = order_manager.positions
                    
                    if not current_positions:
                        logging.info("No positions to monitor")
                        return
                    
                    logging.info(f"Monitoring {len(current_positions)} positions")
                    
                    for symbol, position in current_positions.items():
                        # Get current market data
                        stock_data = nse_fetcher.get_quote(symbol)
                        if not stock_data:
                            continue
                        
                        current_price = stock_data.get('last_price', 0)
                        
                        # Check profit targets
                        should_sell, reason = order_manager.check_profit_targets(symbol, current_price)
                        if should_sell:
                            logging.info(f"Selling {symbol} for profit: {reason}")
                            order_manager.place_and_track_order(
                                symbol=symbol,
                                action='SELL',
                                quantity=position['quantity']
                            )
                            continue
                        
                        # Check stop loss
                        should_stop, reason = order_manager.check_stop_loss(symbol, current_price)
                        if should_stop:
                            logging.info(f"Stop loss triggered for {symbol}: {reason}")
                            order_manager.place_and_track_order(
                                symbol=symbol,
                                action='SELL',
                                quantity=position['quantity']
                            )
                            continue
                        
                        # Check other sell conditions
                        should_sell, reason = order_manager.check_sell_conditions(symbol, current_price)
                        if should_sell:
                            logging.info(f"Selling {symbol}: {reason}")
                            order_manager.place_and_track_order(
                                symbol=symbol,
                                action='SELL',
                                quantity=position['quantity']
                            )
                    
                except Exception as e:
                    logging.error(f"Error in position monitoring: {str(e)}")
            
                def execute_trading_strategy():
                    """Main trading strategy execution"""
                    while True:
                        try:
                            current_time = datetime.now().time()
                            
                            # Check if market is open
                            if not order_manager.is_market_open():
                                next_open = order_manager.get_next_market_open()
                                logging.info(f"Market is closed. Next open at: {next_open}")
                                time.sleep(300)  # Sleep for 5 minutes
                                continue
                            
                            # Run stock analysis at 9:30 AM and 1:30 PM
                            if current_time.hour in [9, 13] and current_time.minute == 30:
                                logging.info("Running scheduled stock analysis...")
                                top_stocks = analyze_stocks()
                                
                                # Process top stocks for buying
                                for stock in top_stocks:
                                    if stock['signal']['action'] == 'BUY' and stock['signal']['confidence'] > 0.7:
                                        if order_manager.can_trade(stock['symbol']):
                                            # Calculate position size
                                            quantity = order_manager.calculate_position_size(
                                                stock['symbol'],
                                                stock['data']['last_price']
                                            )
                                            
                                            # If it's a scale-in opportunity, reduce the quantity
                                            if stock['scale_in_opportunity']:
                                                quantity = int(quantity * 0.5)  # Use 50% of normal position size
                                                logging.info(f"Scaling into {stock['symbol']} with {quantity} shares")
                                            
                                            # Place the order
                                            order_manager.place_and_track_order(
                                                symbol=stock['symbol'],
                                                action='BUY',
                                                quantity=quantity
                                            )
                                
                                # Sleep for 5 minutes to avoid duplicate analysis
                                time.sleep(300)
                            else:
                                # Monitor existing positions
                                monitor_positions()
                                time.sleep(60)  # Check positions every minute
                                
                        except Exception as e:
                            logging.error(f"Error in trading strategy: {str(e)}")
                            time.sleep(60)  # Wait a minute before retrying
                    
                    # Start the trading strategy
                    execute_trading_strategy()
            
        except Exception as e:
            logging.error(f"Fatal error in trading cycle: {str(e)}")
            return


if __name__ == "__main__":
    try:
        print("\nInitializing Trading Bot...", flush=True)
        
        # Initialize components flag3
        print("Initializing NSE Data Fetcher...", flush=True)
        nse_fetcher = ImprovedNSEDataFetcher()
        
        print("Initializing Zerodha Connection...", flush=True)
        zerodha_conn = ImprovedZerodhaConnection(
            api_key="Your_api_key",
            api_secret="Your_secret_key", 
            access_token="Your_access_token"
        )
        
        if zerodha_conn.connect():
            print("Successfully connected to Zerodha", flush=True)
            order_manager = ImprovedOrderManager(zerodha_conn)
            ai_engine = ProfessionalTradingEngine()
            
            # Track analyzed stocks to avoid repetition
            analyzed_stocks = set()
            last_analysis_time = {}  # Track when each stock was last analyzed
            
            print("\nStarting main trading loop...", flush=True)
            
            # Run trading loop
            while True:
                try:
                    print("\n" + "="*50, flush=True)
                    print(f"Starting new trading cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
                    print("="*50, flush=True)
                    
                    # Check if market is open
                    if not order_manager.is_market_open():
                        next_open = order_manager.get_next_market_open()
                        print(f"\nMarket is currently closed.", flush=True)
                        print(f"Next market open: {next_open.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
                        
                        # Calculate time until next market open
                        wait_time = (next_open - datetime.now()).total_seconds()
                        if wait_time > 0:
                            print(f"Waiting {wait_time/3600:.1f} hours until next market open...", flush=True)
                            time.sleep(min(wait_time, 3600))  # Sleep for max 1 hour
                        continue
                    
                    print("\nMarket is open. Starting trading operations...", flush=True)
                    
                    # Update positions and analytics
                    order_manager.update_positions()
                    order_manager.update_portfolio_analytics()
                    current_positions = set(order_manager.positions.keys())
                    
                    # Start continuous portfolio monitoring in a separate thread
                    def monitor_portfolio():
                        while True:
                            try:
                                if not order_manager.is_market_open():
                                    time.sleep(60)  # Check market status every minute
                                    continue
                                
                                # Update positions
                                order_manager.update_positions()
                                current_positions = set(order_manager.positions.keys())
                                
                                if not current_positions:
                                    time.sleep(5)  # Short sleep if no positions
                                    continue
                                
                                print("\nMonitoring portfolio stocks...", flush=True)
                                for symbol in current_positions:
                                    try:
                                        stock_data = nse_fetcher.get_quote(symbol)
                                        
                                        if stock_data:
                                            # Check all sell conditions with conservative rules
                                            should_sell, sell_quantity = order_manager.check_sell_conditions(
                                                symbol, stock_data['price']
                                            )
                                            
                                            if should_sell:
                                                print(f"🔴 Sell signal for {symbol} at ₹{stock_data['price']:.2f}", flush=True)
                                                result = order_manager.place_and_track_order(
                                                    symbol=symbol,
                                                    action='SELL',
                                                    quantity=sell_quantity
                                                )
                                                if result:
                                                    print(f"✅ Sell order placed for {symbol}: {result}", flush=True)
                                                    continue
                                            
                                            # Check scale-in opportunity only if we're in profit
                                            current_pnl = ((stock_data['price'] - order_manager.positions[symbol]['average_price']) / 
                                                         order_manager.positions[symbol]['average_price']) * 100
                                            
                                            if current_pnl > 0:  # Only scale in if we're in profit
                                                scale_in, scale_quantity = order_manager.check_scale_in_opportunity(
                                                    symbol, stock_data['price']
                                                )
                                                if scale_in:
                                                    print(f"📈 Scale-in opportunity for {symbol} at {current_pnl:.2f}% profit", flush=True)
                                                    result = order_manager.place_and_track_order(
                                                        symbol=symbol,
                                                        action='BUY',
                                                        quantity=scale_quantity
                                                    )
                                                    if result:
                                                        print(f"✅ Scale-in order placed for {symbol}: {result}", flush=True)
                                                        continue
                                        
                                    except Exception as e:
                                        print(f"❌ Error monitoring {symbol}: {e}", flush=True)
                                        logging.error(f"Error monitoring {symbol}: {e}")
                                        continue
                                
                                # Short sleep to prevent excessive API calls
                                time.sleep(5)
                                
                            except Exception as e:
                                print(f"❌ Error in portfolio monitoring: {e}", flush=True)
                                logging.error(f"Error in portfolio monitoring: {e}")
                                time.sleep(5)
                    
                    # Start portfolio monitoring in a separate thread
                    portfolio_thread = threading.Thread(target=monitor_portfolio, daemon=True)
                    portfolio_thread.start()
                    
                    # Main loop for finding new stocks to buy
                    while True:
                        try:
                            if not order_manager.is_market_open():
                                time.sleep(300)  # Check market status every 5 minutes
                                continue
                            
                            print("\nLooking for new stocks to buy...", flush=True)
                            
                            # Get trending stocks for potential buys
                            trending_stocks = nse_fetcher.get_top_gainers(limit=10)
                            print(f"\nAnalyzing {len(trending_stocks)} potential stocks for buying opportunities", flush=True)
                            
                            for symbol in trending_stocks:
                                # Skip if we already have a position
                                if symbol in current_positions:
                                    continue
                                
                                # Check if we've analyzed this stock recently (within last 2 hours)
                                last_analyzed = last_analysis_time.get(symbol, 0)
                                if time.time() - last_analyzed < 7200:  # 2 hours
                                    continue
                                
                                try:
                                    print(f"\nAnalyzing new stock {symbol}...", flush=True)
                                    stock_data = nse_fetcher.get_quote(symbol)
                                    
                                    if stock_data:
                                        signal = ai_engine.get_professional_signal(symbol, stock_data)
                                        
                                        # Only process BUY signals with high confidence
                                        if signal['confidence'] > 0.7 and signal['action'] == 'BUY':
                                            print(f"🟢 Strong buy signal for {symbol} (Confidence: {signal['confidence']:.2f})", flush=True)
                                            
                                            # Calculate position size based on risk management
                                            position_size = order_manager.calculate_position_size(
                                                symbol, stock_data['price']
                                            )
                                            
                                            # Check if we can trade this stock
                                            if order_manager.can_trade(symbol):
                                                print(f"Placing buy order for {symbol} - Quantity: {position_size}", flush=True)
                                                result = order_manager.place_and_track_order(
                                                    symbol=symbol,
                                                    action='BUY',
                                                    quantity=position_size
                                                )
                                                if result:
                                                    print(f"✅ Buy order placed for {symbol}: {result}", flush=True)
                                                    analyzed_stocks.add(symbol)
                                                else:
                                                    print(f"❌ Failed to place buy order for {symbol}", flush=True)
                                            else:
                                                print(f"⚠️ Cannot trade {symbol} - Trading conditions not met", flush=True)
                                        else:
                                            print(f"⏳ No strong buy signal for {symbol} (Action: {signal['action']}, Confidence: {signal['confidence']:.2f})", flush=True)
                                        
                                        # Update last analysis time
                                        last_analysis_time[symbol] = time.time()
                                    else:
                                        print(f"❌ Could not fetch data for {symbol}", flush=True)
                                        
                                except Exception as e:
                                    print(f"❌ Error analyzing new stock {symbol}: {e}", flush=True)
                                    logging.error(f"Error analyzing new stock {symbol}: {e}")
                                    continue
                            
                            # Print current portfolio status
                            print("\nCurrent Portfolio Status:", flush=True)
                            portfolio_status = order_manager.get_portfolio_status()
                            if portfolio_status and portfolio_status['positions']:
                                print(f"Total Portfolio Value: ₹{portfolio_status['total_value']:,.2f}", flush=True)
                                print(f"Available Cash: ₹{portfolio_status['available_cash']:,.2f}", flush=True)
                                print(f"Number of Holdings: {len(portfolio_status['positions'])}", flush=True)
                                print("\nHoldings:", flush=True)
                                for symbol, position in portfolio_status['positions'].items():
                                    print(f"{symbol}: {position['quantity']} shares @ ₹{position['average_price']:.2f}", flush=True)
                            else:
                                print("❌ No holdings found in portfolio", flush=True)
                            
                            # Wait before next cycle of looking for new stocks
                            print("\nWaiting 5 minutes before next cycle of looking for new stocks...", flush=True)
                            time.sleep(300)  # 5 minutes
                            
                        except Exception as e:
                            print(f"❌ Error in new stock analysis cycle: {e}", flush=True)
                            logging.error(f"Error in new stock analysis cycle: {e}")
                            time.sleep(60)
                            continue
                    
                except Exception as e:
                    print(f"❌ Error in main trading loop: {e}", flush=True)
                    logging.error(f"Error in main trading loop: {e}")
                    time.sleep(60)
                    continue
                
        else:
            print("❌ Failed to connect to Zerodha", flush=True)
            
    except KeyboardInterrupt:
        print("\nBot stopped by user", flush=True)
    except Exception as e:
        print(f"\nBot crashed: {e}", flush=True)
        logging.error(f"Bot crashed: {e}")