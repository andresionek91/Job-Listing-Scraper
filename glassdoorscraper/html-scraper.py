import requests
from bs4 import BeautifulSoup
import yaml

with open('config.yml') as config_file:
    config = yaml.safe_load(config_file)


def list_jobs(search_term, country_code, page):
    str_length = len(search_term) + 3
    html_page = requests.get('https://www.glassdoor.co.uk/Job/us-'
                             '{search_term}-jobs-SRCH_IL.0,2_IN{country_code}_KO3,'
                             '{str_length}_IP{page}.htm'.format(search_term=search_term,
                                                                country_code=country_code,
                                                                str_length=str_length,
                                                                page=page),
                             headers=config['headers']).text
    return html_page

