import ystockquote
import rethinkdb as r
import threading

def initdb():
	r.connect('localhost', 28015).repl()	
	return r.db('stockdb').table('quotes')

def main():
	stockdb = initdb()
	print "Fetch..."
	stock_price = ystockquote.get_all('GOOG');
	stockdb.insert({
		'timestamp' : r.now(),
		'price' : float(stock_price['price']),
		'price_book_ratio' : float(stock_price['price_book_ratio']),
		'stock_exchange' : stock_price['stock_exchange'][1:-1],
		'volume' : int(stock_price['volume']),
		'market_cap' : float(stock_price['market_cap'][:-1]),
		'change' : float(stock_price['change'][1:]),
		'price_sales_ratio' : float(stock_price['price_sales_ratio']),
		'price_earnings_growth_ratio' : float(stock_price['price_earnings_growth_ratio']),
		'earnings_per_share' : float(stock_price['earnings_per_share']),
		'short_ratio' : float(stock_price['short_ratio']),
		'avg_daily_volume' : int(stock_price['avg_daily_volume']),
		'price_earnings_ratio' : float(stock_price['price_earnings_ratio']),
		'book_value' : float(stock_price['book_value']),
	}).run()
	#threading.Timer(5.0, main).start()

if __name__ == "__main__":
	main()
