import requests
import yaml
import logging
from multiprocessing import Pool, Manager
import json
import time

logging.getLogger().setLevel(logging.INFO)


with open('config.yml') as config_file:
    config = yaml.safe_load(config_file)


def load_unique_job_ids():
    job_ids = set()
    with open(config['jobs-ids-filename']) as jobs_ids_file:
        for row in jobs_ids_file:
            job_ids.add(row)
    return list(job_ids)


def get_job_info(job_id):
    url = 'https://www.glassdoor.co.uk/Job/json/details.htm?jobListingId={job_id}'.format(job_id=job_id)
    counter = 1
    while True:
        try:
            json_data = requests.get(url, headers=config['headers']).json()
            break
        except json.decoder.JSONDecodeError:
            if counter > 10:
                raise ValueError()
            else:
                print('ERROR FOUND: retrying request')
                time.sleep(5)
                counter += 1
    return json_data


def write_json(data, idx):
    with open('results/file_{}.json'.format(idx), 'w') as json_file:
        for result in data:
            json_file.write(json.dumps(result, ensure_ascii=False))
            json_file.write('\n')
    logging.info('file {} written'.format(idx))


def worker(row, job_id):
    json_data = get_job_info(job_id)
    row.append(json_data)


def divide_chunks(data, size):
    for i in range(0, len(data), size):
        yield data[i:i + size]


def execute():
    data = load_unique_job_ids()
    chunks = divide_chunks(data, 400)

    for idx, chunk in enumerate(chunks):
        with Manager() as mgr:
            row = mgr.list([])

            params = []
            for job_id in chunk:
                params.append([row, job_id])

            with Pool() as p:
                p.starmap(worker, params)

            write_json(row, idx)


if __name__ == '__main__':
    execute()
