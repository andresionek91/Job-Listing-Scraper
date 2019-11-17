# Glassdoor Scraper

Simple script to scrap [Glassdoor](https://www.glassdoor.co.uk/) job listings.

## How does it works

### 1. Finding Jobs IDs
First the `html_scraper.py` scraps the first 30 pages of every search term defined 
in the `config.yml` file for every country. In this step we look for the job id 
of each position and append them to a `jobs_ids.txt` file. 

### 2. Getting jobs information
Having the jobs IDs of our interest we start scrapping the actual information of 
each listing. The Glassdoor API returns a json file for each listing. We then collect 
the information in blocks of 400 listings and save the `json` files to the results folder.

### 3. Processing data
I uploaded the data manually to a S3 bucket, where I crawled the data using a Glue Crawler.
It was latter transformed to parquet using a Glue Job with the script provided in 
`glue-job-script.py`. No automation on this step though, because I ran it only once.


At the time I scrapped it, there were about 160k listings for the terms I've searched for.
The data will be uploaded to a Kaggle Dataset.

# Disclaimer
I don't have any connection with Glassdoor and this project is neither 
approved or endorsed by them. The data collected with this script was publicly 
accessible at the moment it was collected. This script was created for educational 
purposes.