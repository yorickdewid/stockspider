import ystockquote
import rethinkdb as r

r.connect('localhost', 28015).repl()
db = r.db('quotes').table('active')

quotes = [
	'GOOG',
	'AAPL',
	'AF.AS',
	'UNIA.AS',
	'HEIA.AS'
];

def build_doc(ret, q):
	dict = {}
	dict['timestamp'] = r.now()
	dict['quote'] = q
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

def do_fetch():
	print "Fetch..."
	for q in quotes:
		stock_price = ystockquote.get_all(q)
		db.insert(build_doc(stock_price, q)).run()

def main():
	do_fetch()

if __name__ == "__main__":
	main()
