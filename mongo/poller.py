from google.cloud import bigquery
import os
from datetime import datetime, timedelta
import pymongo
from tqdm import tqdm
import json
import os
import re
from dateutil.relativedelta import relativedelta

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/ignatov/dev/query/shaped-canyon-309513-b51434997ca9.json'

client = bigquery.Client()


def query_cloud(year, month, day):
    query_job = client.query(
        f"""
        SELECT *
        FROM `bigquery-public-data.pypi.file_downloads`
        WHERE DATE(timestamp) = "{year}-{month}-{day}" and
        file.project = 'deeppavlov'"""
    )

    results = query_job.result()
    return results


def get_db(host: str, port: int, user: str, password: str, db_name: str, collection_name: str = 'files'):
        uri = f"mongodb://{host}:{port}"
        client = pymongo.MongoClient(uri)
        db = client[db_name]
        files = db[collection_name]
        return files


def add_day(db, table, data):
    with db.connect() as conn:
        insert_statement = table.insert().values([dict(row.items()) for row in data])
        conn.execute(insert_statement)


def get_pypi(f_collection):
    date = datetime(year=2019, month=1, day=1)
    today = datetime.now()
    today = datetime(year=today.year, month=today.month, day=today.day)
    days = []
    while True:
        if date == today:
            break
        days.append(date)
        date += timedelta(days=1)
    for date in tqdm(days):
        end = date + timedelta(days=1)
        cnt = f_collection.count_documents({'timestamp':{'$gte': date, '$lt': end}})
        if cnt:
            continue
        data = query_cloud(date.year, date.month, date.day)
        data = [dict(row.items()) for row in data]
        if not data:
            print(date, 'empty data')
            continue
        result = f_collection.insert_many(data)

def get_whois(ip):
    return os.popen(f'whois {ip}').read()


def cnt(md5=False, start='2018-01-01', end='2077-01-01', country='United States'):
    from stats.models import Record
    from django.db.models import Count
    qset = Record.objects.filter(response_code=200, file__md5=md5, ip__country=country,
                                 time__gte=start, time__lte=end).values(
        'ip__ip').annotate(total=Count('ip__ip')).order_by('-total')
    ip_to_downloads = {i['ip__ip']: i['total'] for i in qset}
    ip_collection = get_db('mongo', 27017, 'stat', 'stat', 'pypistat', 'ips')
    files = ip_collection.find()
    ip_to_org = dict()
    a, b, c = 0, 0, 0
    for file in files:
        match = re.search('OrgName:\s+(.+)', file['whois']) if file['whois'] is not None else None
        match2 = re.search('org-name:\s+(.+)', file['whois']) if file['whois'] is not None else None
        if match is not None:
            ip_to_org[file['ip']] = match.group(1)
            a+=1
        elif match2 is not None:
            ip_to_org[file['ip']] = match2.group(1)
            b+=1
        else:
            c+=1
    raise ValueError(f'{a} {b} {c}')
    org_to_downloads = dict()
    for ip, org in ip_to_org.items():
        if ip in ip_to_downloads:
            dic = org_to_downloads.get(org, {})
            dic['downloads'] = dic.get('downloads', 0) + ip_to_downloads[ip]
            dic['ips'] = dic.get('ips', 0) + 1
            org_to_downloads[org] = dic
    return org_to_downloads

from pathlib import Path
def save_to_pd(data: dict, name, dir=Path('/tmp')):
    dat = []
    for k, v in data.items():
        v['company'] = k
        dat.append(v)
    from pandas import DataFrame
    df = DataFrame.from_records(dat)
    df.to_excel(dir/f'{name}.xlsx')


def months(md5=False, country='United States', dir=Path('/tmp')):
    start = datetime(year=2019, month=9, day=1)
    while start != datetime(year=2022, month=10, day=1):
        end = start + relativedelta(months=1) - relativedelta(days=1)
        data = cnt(md5, f'{start.year}-{start.month}-{start.day}', f'{end.year}-{end.month}-{end.day}',
                   country=country)
        save_to_pd(data, f'{start.year}_{start.month}', dir)
        start += relativedelta(months=1)


def years(md5=False, country='United States', dir=Path('/tmp')):
    for year in range(2019, 2022):
        data = cnt(md5, f'{year}-09-01', f'{year+1}-09-01',
                   country=country)
        save_to_pd(data, f'{year}_09_{year+1}_09', dir)


def get_all(md5, country, dir=Path('/tmp')):
    months(md5, country, dir)
    years(md5, country, dir)
    save_to_pd(cnt(md5, country=country), 'all', dir)


def process_countries(countries=None):
    countries = countries or ['United States']
    tmp_dir = Path('/tmp')
    for country in countries:
        md5_path = tmp_dir / country / 'md5'
        not_md5_path = tmp_dir / country / 'not_md5'
        md5_path.mkdir(parents=True)
        not_md5_path.mkdir()
        get_all(False, country, not_md5_path)
        get_all(True, country, md5_path)

def old_one():
    files_collection = get_db('0.0.0.0', 27017, 'stat', 'stat', 'pypistat')
    ip_collection = get_db('0.0.0.0', 27017, 'stat', 'stat', 'pypistat', 'ips')

    # with open('../ips/file.json') as fin:
    #     lines = [json.loads(l) for l in fin.readlines()]
    # max_id = ip_collection.find_one(sort=[("id", -1)])['id']
    # for line in tqdm(lines):
    #     if line['id'] <= max_id:
    #         continue
    #     try:
    #         whois = get_whois(line['ip'])
    #     except UnicodeDecodeError:
    #         whois = None
    #         print(f'error {line["ip"]}')
    #     line['whois'] = whois
    #     ip_collection.insert_one(line)

    files = ip_collection.find()
    ans = list()
    for file in files:
        match = re.search('OrgName:\s+(.+)', file['whois']) if file['whois'] is not None else None
        if match is not None:
            ans.append(match.group(1))
        # else:
        #     print(file['whois'])
        #     input()
    ans = {a: ans.count(a) for a in sorted(set(ans))}
    for a in sorted(ans, key=lambda x: ans[x], reverse=True):
        print(a, ans[a])
    print(len(ans))

def upd():
    files_collection = get_db('mongo', 27017, 'stat', 'stat', 'pypistat')
    get_pypi(files_collection)

if __name__ == '__main__':
    upd()
