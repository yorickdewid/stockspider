import ystockquote
import rethinkdb as r
import sched, time
s = sched.scheduler(time.time, time.sleep)

def initdb():
	r.connect('localhost', 28015).repl()	
	return r.db('quotes').table('GOOG')

def build_doc(ret):
	dict = {}
	dict['timestamp'] = r.now()
	dict['price'] = float(ret['price'])
	if ret['price_book_ratio'] != 'N/A':
		dict['price_book_ratio'] = float(ret['price_book_ratio'])
	dict['stock_exchange'] = ret['stock_exchange'][1:-1]
	dict['volume'] = int(ret['volume'])
	if ret['market_cap'] != 'N/A':
		dict['market_cap'] = float(ret['market_cap'][:-1])
	dict['change'] = float(ret['change'][1:])
	if ret['price_sales_ratio'] != 'N/A':
		dict['price_sales_ratio'] = float(ret['price_sales_ratio'])
	dict['price_earnings_growth_ratio'] = float(ret['price_earnings_growth_ratio'])
	if ret['earnings_per_share'] != 'N/A':
		dict['earnings_per_share'] = float(ret['earnings_per_share'])
	dict['short_ratio'] = float(ret['short_ratio'])
	dict['avg_daily_volume'] = int(ret['avg_daily_volume'])
	if ret['price_earnings_ratio'] != 'N/A':
		dict['price_earnings_ratio'] = float(ret['price_earnings_ratio'])
	dict['book_value'] = float(ret['book_value'])
	return dict

def do_fetch(sc, stockdb):
	print "Fetch..."
	stock_price = ystockquote.get_all('GOOG')
	stockdb.insert(build_doc(stock_price)).run()
	sc.enter(900, 1, do_fetch, (sc, stockdb))

def main():
	db = initdb()
	s.enter(900, 1, do_fetch, (s, db))
	s.run()

if __name__ == "__main__":
	main()
