import numpy as np
import pandas as pd

import bql
bq = bql.Service()

from gzip import decompress
from bloomberg.gdto.bcos.bcos_client import BCOSClient, BCOS_PROD_URL, CORP_TO_PROD_PROXIES

from datetime import datetime
import json


def fetch_csmg_deep_data(ticker, bucket, client):
    req = bql.Request(ticker, {'bbid': bq.data.id_bb_company()} )
    res = bq.execute(req)
    bbid_map = bql.combined_df(res).bbid.to_dict()
    bobject = bbid_map[ticker]
    key = f'{bobject}/company-tots.json'
    
    response_object = client.get_object_from_bucket(bucket, key).content
    try:
        payload = json.loads(response_object)
    except:
        print('using decompress')
        payload = json.loads(decompress(response_object).decode('utf-8'))
    finally:
        return payload


def parse_individual_bucket_main(ticker, csmg_deep_data, calcrt, segment_id):
    buckets = csmg_deep_data['buckets']
    periods = csmg_deep_data['periods']
    calcrt_bucket, attributes, fiscal_period = get_individual_bucket(buckets, periods, calcrt, segment_id, ticker)
    bucket_data = make_bucket_dataframe(calcrt_bucket, attributes, fiscal_period, ticker=ticker)
    output = filter_buckets_by_dt(bucket_data)
    output = melt_result(output)
    return output    


def make_bbid_map(tickers):
    if not isinstance(test, list):
        tickers = list(tickers)
    req = bql.Request(tickers, {'bbid': bq.data.id_bb_company()} )
    res = bq.execute(req)
    bbid_map = bql.combined_df(res).bbid.to_dict()
    return bbid_map


def fetch_modl_template(ticker): 
    output = (
        pyrefdata
          .get_data(session, ticker, 'ct001', ignore_errors=True)
          .loc[ticker, 'ct001']
          .rename_axis(columns=None, index=None)
    )
    output.columns = output.columns.str.lower().str.replace(' ', '_')
    output = output.loc[output.field_name != '--'].reset_index(drop=True)
    return output


def make_bucket_dataframe(calcrt_bucket, attributes, fiscal_period, ticker):
    bucket_data = (
        pd.DataFrame(calcrt_bucket['members'])
          .dropna(subset=['data'])
          .reset_index(drop=True)
          .assign(
              calcrt = attributes['calcrts'][0],
              segment_id = attributes['segmentId'],
              currency = attributes['currency'],
              bucket_id = calcrt_bucket['bucketId']
          )
    )
    bucket_data = bucket_data.rename(columns={'brokerId': 'broker_code'})
    bucket_data.labels = bucket_data.labels.apply(lambda x: x[-1])
    bucket_data.data = bucket_data.data.apply(lambda estimates: [{'fiscal_period': fp, 'estimate': est['value']} for est, fp in zip(estimates, fiscal_period) if est])
    bucket_data.segment_id = bucket_data.segment_id.fillna(ticker)
    bucket_data.receivedDateTime = pd.to_datetime(bucket_data.receivedDateTime)
    bucket_data.reportTime = pd.to_datetime(bucket_data.reportTime)
    return bucket_data


def filter_buckets_by_dt(bucket_data):
    received_datetime = bucket_data.groupby('broker_code').receivedDateTime
    received_datetime = received_datetime.min().reset_index()
    bucket_data = received_datetime.merge(bucket_data, on=['broker_code', 'receivedDateTime'])
    
    idx = ['ardId', 'fileId', 'calcrt', 'segment_id', 'currency', 'bucket_id', 'broker_code', 'receivedDateTime']
    df = bucket_data.set_index(idx).data.explode().apply(pd.Series)
    df = df.set_index('fiscal_period', append=True).unstack('fiscal_period').reset_index().sort_values('broker_code').reset_index(drop=True)
    df.columns = [i if j == '' else j for i, j in df.columns]
    df1 = df.set_index(['ardId', 'broker_code']).isnull().mean(1).unstack('broker_code')
    argmax = df1.apply(np.argmax).to_dict()
    ard_ids = df1.index.values
    cols = df1.columns
    ard_ids = [ard_ids[argmax[key]] for key in argmax]
    bucket_data = df.loc[df.ardId.isin(ard_ids)].reset_index(drop=True)
    return bucket_data


def get_individual_bucket(buckets, periods, calcrt, segment_id, ticker):
    segment_id_inner = segment_id if segment_id != ticker else None
    calcrt_bucket = [
        bucket for bucket in buckets 
        if (bucket['attributes'] is not None) and ((calcrt in bucket['attributes']['calcrts']) and (bucket['attributes']['segmentId'] == segment_id_inner))
    ][0]
    attributes = calcrt_bucket['attributes']
    fiscal_period = [parse_fiscal_period(p, periods) for p in periods]
    return calcrt_bucket, attributes, fiscal_period


def parse_fiscal_period(period, periods):
    fp = periods[period]['fiscalPeriod']
    fp = fp[1:-2] if fp[0] == 'F' else fp[:-2]
    year, period = fp.split('-')
    period = 'A' if 'A' in period else period
    fp = f'{year}{period}'
    return fp


def melt_result(pivoted_dataframe):
    output = pivoted_dataframe.melt(id_vars=['ardId', 'fileId', 'calcrt', 'segment_id', 'currency', 'bucket_id', 'broker_code', 'receivedDateTime'], var_name='period').dropna(subset=['value'])
    output = output.groupby(['calcrt', 'segment_id', 'period']).value.mean().reset_index().rename(columns={'value': 'broker_actuals'})
    return output