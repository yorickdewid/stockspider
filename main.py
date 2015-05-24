import ystockquote
import rethinkdb as r

r.connect('localhost', 28015).repl()

def build_doc(ret, q):
	dict = {}
	dict['timestamp'] = r.now()
	dict['quote'] = q
	if ret['price'] != 'N/A':
		dict['price'] = float(ret['price'])
	if ret['price_book_ratio'] != 'N/A':
		dict['price_book_ratio'] = float(ret['price_book_ratio'])
	if ret['stock_exchange'] != 'N/A':
		dict['stock_exchange'] = ret['stock_exchange'][1:-1]
	if ret['volume'] != 'N/A':
		dict['volume'] = int(ret['volume'])
	if ret['market_cap'] != 'N/A':
		dict['market_cap'] = float(ret['market_cap'][:-1])
	if ret['change'] != 'N/A':
		dict['change'] = float(ret['change'][1:])
	if ret['price_sales_ratio'] != 'N/A':
		dict['price_sales_ratio'] = float(ret['price_sales_ratio'])
	if ret['price_earnings_growth_ratio'] != 'N/A':
		dict['price_earnings_growth_ratio'] = float(ret['price_earnings_growth_ratio'])
	if ret['earnings_per_share'] != 'N/A':
		dict['earnings_per_share'] = float(ret['earnings_per_share'])
	if ret['short_ratio'] != 'N/A':
		dict['short_ratio'] = float(ret['short_ratio'])
	if ret['avg_daily_volume'] != 'N/A':
		dict['avg_daily_volume'] = int(ret['avg_daily_volume'])
	if ret['price_earnings_ratio'] != 'N/A':
		dict['price_earnings_ratio'] = float(ret['price_earnings_ratio'])
	if ret['book_value'] != 'N/A':
		dict['book_value'] = float(ret['book_value'])
	return dict

def do_fetch():
	db = r.db('quotes').table('active')
	for q in r.db('quotes').table('fetchlist').run():
		print "Fetch...", q['quote']
		stock_price = ystockquote.get_all(q['quote'])
		db.insert(build_doc(stock_price, q['quote'])).run()

def main():
	do_fetch()

if __name__ == "__main__":
	main()
