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


def get_total_pages(html_page):
    soup = BeautifulSoup(html_page, 'html.parser')
    pages_text = soup.find_all("div", class_="cell middle hideMob padVertSm")[0].text
    total_pages = pages_text.split(' ')[-1]
    return total_pages


def get_job_ids(html_page):
    soup = BeautifulSoup(html_page, 'html.parser')
    jobs_list = soup.find_all("li", class_="jl")
    jobs_ids = []
    for job in jobs_list:
        jobs_ids.append(job.get('data-id'))
    return jobs_ids

