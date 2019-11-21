import requests
import yaml
import pandas as pd


with open('config.yml') as config_file:
    config = yaml.safe_load(config_file)

fixer_key = config.get('fixer-key')
response = requests.get(f'http://data.fixer.io/api/2019-11-10?access_key={fixer_key}')

rates = response.json().get('rates')
usd = rates.get('USD')

usd_base_rate = []
for cur, rate in rates.items():
    usd_base_rate.append([cur, rate / usd])

currency_conversion = pd.DataFrame.from_records(usd_base_rate)
currency_conversion.columns = ['Code', 'ExchangeRate']


currency_codes = pd.read_html('http://www.iban.com/currency-codes')[0]
currency_codes.Country = currency_codes.Country.str.title()


currency_exchange = currency_codes.set_index('Code').join(currency_conversion.set_index('Code'), how='left', lsuffix='conv')
currency_exchange.to_csv('results/currency_exchange.csv')