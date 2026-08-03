#!/usr/local/bin/python3
#!/home/nathan/report-docker/.python3/bin/python3
import math
import sqlite3
import psycopg2
import requests
from requests.adapters import HTTPAdapter, Retry
from urllib.parse import urljoin
import re
import os
import json
import pandas as pd
import boto3
from datetime import datetime, timezone
import time
from botocore.exceptions import ClientError
from slack_notify import slack_send
import datetime
from kubernetes import client, config
import argparse
import sched


apiversion = "v4"

today_date = datetime.date.today()
#today_date = datetime.datetime.strptime("2026-07-02", "%Y-%m-%d").date()

parser = argparse.ArgumentParser(description="Script with flags")
parser.add_argument("-r", "--report", action="store_true", help="Run usage report")
args = parser.parse_args()
run_report = args.report
if not run_report:
    run_report = os.environ.get("RUN_REPORT", False)

def _db_conn():
    db_user = os.environ["REPORT_DB_USER"]
    db_pass = os.environ["REPORT_DB_PASS"]
    db_host = os.environ["REPORT_DB_HOST"]
    conn = psycopg2.connect(
        "postgresql://{0}:{1}@{2}/defaultdb?sslmode=require".format(
            db_user, db_pass, db_host
        )
    )

    return conn

def _db_init():
    conn = _db_conn()
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS buckets (bucket_name varchar(500), contract_id varchar(12), datacenter varchar(50), size real, checked_date date, PRIMARY KEY (checked_date, bucket_name, datacenter))"
    )
    return cursor, conn

def _db_region_init():
    conn = _db_conn()
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS regions (region varchar(50), vos360_buckets SMALLINT, msl_buckets SMALLINT, sandbox_buckets SMALLINT, vos360_counts SMALLINT, msl_counts SMALLINT, sandbox_counts SMALLINT, last_checked_date TIMESTAMPTZ, last_error_site varchar(6), error_code int2, PRIMARY KEY (region))"
    )
    return cursor, conn

def get_regions(token):
    headers = {"accept": "application/json", "authorization": "Bearer {}".format(token)}
    url = "https://api.linode.com/{0}/object-storage/endpoints".format(apiversion)

    s = requests.Session()

    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])

    s.mount("https://", HTTPAdapter(max_retries=retries))

    response = s.get(url, headers=headers)
    data = response.json()
    cursor, conn = _db_region_init()
    #print(data)
    if response.status_code in [400, 422]:
        response.raise_for_status()
        print("Terminating: {0}".format(response.status_code))
        exit()

    elif response.status_code == 200:
        regions = []
        for d in data['data']:
            regions.append(d['region'])

        dedupe_list = list(dict.fromkeys(regions))
        for region in dedupe_list:
            print(region)
            print(today_date)
            cursor.execute(
                    "INSERT INTO regions (region) VALUES (%s) ON CONFLICT (region) DO NOTHING;",
                    (region,),
            )

    conn.commit()
    #conn.close()

def restart_deployment(name, namespace="metrics-system"):
    deployment_name = "aclp-collector-{0}".format(name)
    config.load_incluster_config()
    apps_v1 = client.AppsV1Api()

    # now = datetime.datetime.utcnow().isoformat() + "Z"
    now = datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z"

    body = {
        "spec": {
            "template": {
                "metadata": {"annotations": {"kubectl.kubernetes.io/restartedAt": now}}
            }
        }
    }

    apps_v1.patch_namespaced_deployment(deployment_name, namespace, body)
    print(f"Restarted deployment: {deployment_name}")


def get_dc(k):
    dc = {
        "dallas": "Dallas, TX (us-central)",
        "us-central": "Dallas, TX (us-central)",
        "fremont": "Fremont, CA (us-west)",
        "us-west": "Fremont, CA (us-west)",
        "atlanta": "Atlanta, GA (us-southeast)",
        "us-southeast": "Atlanta, GA (us-southeast)",
        "newark": "Newark, NJ (us-east)",
        "us-east": "Newark, NJ (us-east)",
        "london": "London, UK (eu-west)",
        "eu-west": "London, UK (eu-west)",
        "ap-northeast-1a": "Tokyo, JP (ap-northeast-1a)",
        "singapore": "Singapore, SG (ap-south)",
        "frankfurt": "Frankfurt, DE (eu-central)",
        "eu-central": "Frankfurt, DE (eu-central)",
        "shinagawa1": "Tokyo 2, JP (ap-northeast)",
        "us-east-1b": "Cedar Knolls, NJ, USA (us-east-1b)",
        "philadelphia": "Philadelphia, PA, USA (philadelphia)",
        "mum1": "Mumbai, IN (ap-west)",
        "tor1": "Toronto, CA (ca-central)",
        "syd1": "Sydney, AU (ap-southeast)",
        "iad3": "Washington, DC (us-iad)",
        "us-iad": "Washington, DC (us-iad)",
        "ord2": "Chicago, IL (us-ord)",
        "us-ord": "Chicago, IL (us-ord)",
        "us-ord-1": "Chicago, IL (us-ord-1)",
        "us-ord-10": "Chicago, IL (us-ord-10)",  # changed
        "fr-par": "Paris, FR (fr-par)",
        "sea1": "Seattle, WA (us-sea)",
        "us-sea-9": "Seattle, WA (us-sea)",
        "us-sea-1": "Seattle, WA (us-sea)",
        "us-sea": "Seattle, WA (us-sea)",
        "gru1": "Sao Paulo, BR (br-gru)",
        "br-gru": "Sao Paulo, BR (br-gru)",
        "ams2": "Amsterdam, NL (nl-ams)",
        "nl-ams": "Amsterdam, NL (nl-ams)",
        "sto2": "Stockholm, SE (se-sto)",
        "mad2": "Madrid, ES (es-mad)",
        "maa1": "Chennai, IN (in-maa)",
        "osa1": "Osaka, JP (jp-osa)",
        "mil1": "Milan, IT (it-mil)",
        "us-mia": "Miami, FL (us-mia)",
        "cgk1": "Jakarta, ID (id-cgk)",
        "lax3": "Los Angeles, CA (us-lax)",
        "us-lax": "Los Angeles, CA (us-lax)",
        "us-lax-1": "Los Angeles, CA (us-lax-1)",
        "us-hou": "Houston, TX (us-hou)",
        "nz-akl-1": "Auckland, NZ (nz-akl-1)",
        "pl-krk": "Krakow, PL (pl-krk)",
        "us-den-1": "Denver, CO (us-den-1)",
        "de-ham-1": "Hamburg, DE (de-ham-1)",
        "fr-mrs-1": "Marseille, FR (fr-mrs-1)",
        "za-jnb-1": "Johannesburg, ZA (za-jnb-1)",
        "my-kul-1": "Kuala Lumpur, MY (my-kul-1)",
        "hk-hkg-1": "Hong Kong, HK (hk-hkg-1)",
        "co-bog-1": "Bogotá, CO (co-bog-1)",
        "mx-qro-1": "Querétaro, MX (mx-qro-1)",
        "us-hou-1": "Houston, TX (us-hou-1)",
        "cl-scl-1": "Santiago, CL (cl-scl-1)",
        "gb-lon-4": "London 4, UK (gb-lon-4)",
        "gb-lon-1": "London, UK (gb-lon-1)",  # changed
        "gb-lon-2": "London 2, UK (gb-lon-2)",  # changed
        "gb-lon": "London, UK (gb-lon)",  # changed
        "mel1": "Melbourne, AU (au-mel)",
        "au-mel": "Melbourne, AU (au-mel)",
        "au-mel-1": "Melbourne, AU (au-mel-1)",
        "bom1": "Mumbai 2, IN (in-bom-2)",
        "in-bom-1": "Mumbai, IN (in-bom-1)",  # changed
        "in-bom-2": "Mumbai 2, IN (in-bom-2)",
        "de-fra-1": "Frankfurt, DE (de-fra-1)",  # changed
        "de-fra-2": "Frankfurt 2, DE (de-fra-2)",
        "sg-sin-2": "Singapore 2, SG (sg-sin-2)",
        "sg-sin-1": "Singapore 2, SG (sg-sin-1)",
        "sg-sin": "Singapore 2, SG (sg-sin)",
        "tyo2": "Tokyo 3, JP (jp-tyo-3)",
        "jp-tyo-1": "Tokyo, JP (jp-tyo-1)",
        "jp-tyo": "Tokyo, JP (jp-tyo)",
        "jp-tyo-2": "Tokyo 2, JP (jp-tyo-2)",
        "jp-tyo-3": "Tokyo 3, JP (jp-tyo-3)",
        "de-ber": "Berlin, DE (de-ber)",
        "de-ber-1": "Berlin, DE (de-ber-1)",
        "no-osl-1": "Oslo, NO (no-osl-1)",
        "no-osl": "Oslo, NO (no-osl)",
    }
    try:
        response = dc[k]
    except:
        response = k
    return response


def get_cid(env):
    cid = json.load(open("/root/accountids.json"))
    try:
        response = cid[env]
    except:
        response = "000"
    return response


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client(
        "s3",
        region_name="us-ord-1",
        endpoint_url=f"https://us-ord-1.linodeobjects.com",
        aws_access_key_id=os.environ["REPORT_ACCESS_KEY"],
        aws_secret_access_key=os.environ["REPORT_SECRET_ACCESS_KEY"],
    )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True

def get_single_region(env, conn):
    sql_query = "SELECT * FROM regions;"
    region_df = pd.read_sql_query(sql_query, conn)
    env_buckets = "{0}_buckets".format(env)
    env_counts = "{0}_counts".format(env)
    df_sorted = region_df.sort_values(by=env_counts)
    #ascending=False
    for index, row in df_sorted.iterrows():
        print("{0} - {1}".format(row[env_buckets], row[env_counts]))
        if row[env_buckets] == 0 and row[env_counts] > 2:
            continue
        elif row[env_buckets] == None or row[env_counts] == None or math.isnan(row[env_counts]) or math.isnan(row[env_counts]):
            return row 
        elif row[env_buckets] > 0 and row[env_counts] >= 0:
            return row 
        elif row[env_buckets] == 0 and row[env_counts] <= 1:
            return row 

        if row[env_counts] == None or math.isnan(row[env_counts]):
            row[env_counts] = 0

def get_bucket_detail(token, env, cursor, conn, region, row):
    dt_utc = datetime.datetime.now(timezone.utc)
    env_buckets = "{0}_buckets".format(env)
    env_counts = "{0}_counts".format(env)

    print("Region {0} - Env: {1}".format(region, env))
    headers = {"accept": "application/json", "authorization": "Bearer {}".format(token)}
    url = "https://api.linode.com/{0}/object-storage/buckets/{1}".format(apiversion, region)

    s = requests.Session()

    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    response = s.get(url, headers=headers)
    print("{}".format(response.headers))
    data = response.json()
    print(data["data"])

    if response.status_code in [400, 422]:
        response.raise_for_status()
        print("Terminating: {0}".format(response.status_code))
        exit()

    elif response.status_code == 200:
        sql = "INSERT INTO regions (region, {0}_buckets, {0}_counts, last_checked_date) VALUES (%s, %s, %s, %s) ON CONFLICT (region) DO UPDATE SET {0}_buckets = EXCLUDED.{0}_buckets, {0}_counts = EXCLUDED.{0}_counts, last_checked_date = EXCLUDED.last_checked_date;".format(env)

        if row[env_counts] == None or math.isnan(row[env_counts]):
            row[env_counts] = 0

        val = int(row[env_counts])+1
        cursor.execute(
            sql,
            (region, data["results"], val, dt_utc),
        )
        conn.commit()

        print("Pages {}".format(data["pages"]))

        if data["pages"] == 1:
            return bucket_detail(data["data"])
        else:
            page = 2
            while page <= data["pages"]:
                print("Page {} of {}".format(page, data["pages"]))
                url = (
                    "https://api.linode.com/{0}/object-storage/buckets/{1}?page={2}".format(
                        apiversion, region, page
                    )
                )
                try:
                    response_p = s.get(url, headers=headers)
                    print("{}".format(response_p.headers))
                except requests.exceptions.RequestException as e:
                    print(f"The request ultimately failed: {e}")
                # response_p = requests.get(url, headers=headers)
                finally:
                    datapage = response_p.json()
                for x in datapage["data"]:
                    data["data"].append(x)
                page += 1

            return bucket_detail(data["data"])
    elif response.status_code == 504:
        sql = "INSERT INTO regions (region, last_error_site, error_code) VALUES (%s, %s, %s) ON CONFLICT (region) DO UPDATE SET last_error_site = EXCLUDED.last_error_site, error_code = EXCLUDED.error_code;"
        cursor.execute(
            sql,
            (region, env, response.status_code),
        )
        conn.commit()
        return


def bucket_detail(data):
    df = pd.DataFrame(data)
    # df.style.hide(axis='index')

    return df



def _do_query(cursor, conn, env, token, region, row):
    resp = get_bucket_detail(token, env, cursor, conn, region, row)
    print(resp)
    linec = 0
    for line in resp.itertuples():
        label = line.label
        region = line.region
        size = line.size
        print("{},{},{}".format(label, region, size))
        linec += 1
        # l = line.decode("utf-8")
        # size = float(size)
        cursor.execute(
            "INSERT INTO buckets (bucket_name, contract_id, datacenter, size, checked_date) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (checked_date, bucket_name, datacenter) DO UPDATE SET contract_id = EXCLUDED.contract_id, size = EXCLUDED.size;",
            (label, get_cid(env)[0], get_dc(region), size, today_date),
        )
    print(linec)


report_cycle = 60


def reporting_task(scheduler):
    envs = [
        "vos360",
        "msl",
        "sandbox",
    ]
    scheduler.enter(report_cycle, 1, reporting_task, (scheduler,))
    print("Checking APIs...")
    cursor, conn = _db_init()
    for env in envs:
        row = get_single_region(env, conn)
        region = row["region"]
        time.sleep(30)
        _do_query(cursor, conn, env, get_cid(env)[1], region, row)
        time.sleep(5)
        conn.commit()
    print("API Polling Complete: {0}".format(env))
    conn.close()


if not run_report:
    envs = [
        "vos360",
        "msl",
        "sandbox",
    ]
    print("Polling records...")
    get_regions(get_cid("vos360")[1])
    report_scheduler = sched.scheduler(time.time, time.sleep)
    report_scheduler.enter(report_cycle, 1, reporting_task, (report_scheduler,))
    report_scheduler.run()
    
    

if run_report:
    print("Running report...")
    cursor, conn = _db_init()
    sql_query = "SELECT * FROM buckets WHERE checked_date = CURRENT_DATE - INTERVAL '1 day';"
    #sql_query = "SELECT * FROM buckets WHERE checked_date = CURRENT_DATE;"
    df = pd.read_sql_query(sql_query, conn)
    df = df.drop(columns='checked_date')
    df.columns = ["Bucket Name", "Contract ID", "Datacenter", "Size in Bytes"]
    print(df.to_string())

    os.makedirs("/var/report", exist_ok=True)
    filename = "ObjectStoragesUsage_{0}.csv".format(
        datetime.date.today() - datetime.timedelta(1)
    )
    df.to_csv(
        "/var/report/{0}".format(filename),
        mode="a",
        index=False,
        float_format="{:.0f}".format,
    )
    sql = "update regions SET (vos360_counts, msl_counts, sandbox_counts) = (0, 0, 0)";
    cursor.execute(sql)
    conn.commit()
    conn.close()
    upload_file("/var/report/{0}".format(filename), os.environ["REPORT_BUCKET_NAME"])
    slack_send(filename)
