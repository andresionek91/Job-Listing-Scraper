import requests
from bs4 import BeautifulSoup
import yaml
import logging
from multiprocessing import Pool
from itertools import repeat

logging.getLogger().setLevel(logging.INFO)


with open('config.yml') as config_file:
    config = yaml.safe_load(config_file)


def list_jobs(search_term, country_code, page):
    str_length = len(search_term) + 3
    url = 'https://www.glassdoor.co.uk/Job/us-'\
          '{search_term}-jobs-SRCH_IL.0,2_IN{country_code}_KO3,'\
          '{str_length}_IP{page}.htm'.format(search_term=search_term,
                                             country_code=country_code,
                                             str_length=str_length,
                                             page=page)
    html_page = requests.get(url, headers=config['headers']).text
    return html_page


def get_total_pages(html_page):
    soup = BeautifulSoup(html_page, 'html.parser')
    try:
        pages_text = soup.find_all("div", class_="cell middle hideMob padVertSm")[0].text
        total_pages = pages_text.split(' ')[-1]
        return int(total_pages)
    except IndexError:
        return 0


def get_job_ids(html_page):
    soup = BeautifulSoup(html_page, 'html.parser')
    jobs_list = soup.find_all("li", class_="jl")
    jobs_ids = []
    for job in jobs_list:
        jobs_ids.append(job.get('data-id'))
    return jobs_ids


def write_jobs_ids_to_file(jobs_ids):
    for job_id in jobs_ids:
        with open(config['jobs-ids-filename'], 'a') as dest_file:
            dest_file.write(job_id)
            dest_file.write('\n')
    num_lines = sum(1 for line in open(config['jobs-ids-filename']))
    logging.info('Total ids collected: {}'.format(num_lines))


def execute(country_code, search_term):
    page_count = 1
    total_pages = 1
    while page_count <= total_pages:
        html_page = list_jobs(search_term, country_code, page_count)
        total_pages = get_total_pages(html_page)
        jobs_ids = get_job_ids(html_page)
        write_jobs_ids_to_file(jobs_ids)
        page_count += 1


if __name__ == '__main__':
    country_codes = list(range(1, config['country_max'] + 1))
    for search_term in config['search-terms']:
        pool = Pool()
        pool.starmap(execute, zip(country_codes, repeat(search_term)))
