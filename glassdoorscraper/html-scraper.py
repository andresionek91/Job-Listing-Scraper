import requests
from bs4 import BeautifulSoup
import yaml
import logging
from multiprocessing import Pool, Manager
import json

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
    with open(config['jobs-ids-filename'], 'a') as dest_file:
        for job_id in jobs_ids:
            dest_file.write(job_id)
            dest_file.write('\n')


def write_jobs_params_to_file(jobs_params):
    with open(config['jobs-params-filename'], 'a') as dest_file:
        for param in jobs_params:
            dest_file.write(json.dumps(param))
            dest_file.write('\n')


def worker(row, country_code, search_term, page_count):
    html_page = list_jobs(search_term, country_code, page_count)
    logging.info('{}: extracting ids {}, {}, {}'.format(len(row), search_term, country_code, page_count))
    jobs_ids = get_job_ids(html_page)
    row += jobs_ids


def build_job_params(params, country_code, search_term):
    page_html = list_jobs(search_term, country_code, 2)
    total_pages = get_total_pages(page_html)
    # Glassdor only returns 30 first pages for each search
    total_pages = total_pages if total_pages <= 30 else 30
    for page_count in range(1, total_pages + 1):
        params.append([country_code, search_term, page_count])
    logging.info('building job for {}, {}, total pages {}'.format(search_term, country_code, total_pages))


def execute_job_param():
    country_codes = list(range(1, config['country_max'] + 1))
    with Manager() as mgr:
        params = mgr.list([])

        build_params = []
        for country_code in country_codes:
            for search_term in config['search-terms']:
                build_params.append([params, country_code, search_term])

        with Pool() as p:
            p.starmap(build_job_params, build_params)
            p.close()
            p.join()

        write_jobs_params_to_file(params)


def execute():
    with Manager() as mgr:
        row = mgr.list([])

        params = []
        with open(config['jobs-params-filename']) as params_file:
            for line in params_file:
                params.append([row] + json.loads(line))

        with Pool() as p:
            p.starmap(worker, params)

        write_jobs_ids_to_file(row)


if __name__ == '__main__':
    execute_job_param()
    execute()
