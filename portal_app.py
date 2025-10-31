import os
import re
import json
from datetime import datetime, date
import io

import pandas as pd
from flask import Flask, render_template, jsonify, request
import boto3
from botocore.exceptions import ClientError
import awswrangler as wr

# --- Konfigurasjon ---
app = Flask (__name__)

# Les alle konfigurasjonsverdier fra miljøvariabler
AWS_KEY = os.environ.get ('AWS_ACCESS_KEY_ID')
AWS_SECRET = os.environ.get ('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.environ.get ('AWS_REGION')
S3_BUCKET_NAME = os.environ.get ('S3_BUCKET_NAME', 'prisanalyse-data')
ATHENA_DATABASE = os.environ.get ('ATHENA_DATABASE', 'bil_finn_daglig')
DEFAULT_STARTDATE = date (2025, 5, 1)

if not all ([AWS_KEY, AWS_SECRET, AWS_REGION]):
    print (
        "ADVARSEL: Kritiske AWS-miljøvariabler mangler (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION). Appen vil sannsynligvis feile.")


# --- Hjelpefunksjoner ---
def find_latest_file_in_s3(s3_client, bucket, prefix, file_pattern):
    try:
        response = s3_client.list_objects_v2 (Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response: return None
        latest_file, latest_date = None, None
        for obj in response['Contents']:
            key = obj['Key']
            match = re.search (file_pattern, key)
            if match:
                file_date = datetime.strptime (match.group (1), '%d-%m-%Y')
                if latest_date is None or file_date > latest_date:
                    latest_date, latest_file = file_date, key
        return latest_file
    except ClientError as e:
        print (f"Kunne ikke liste objekter i S3: {e}")
        return None


# --- Ruter ---
@app.route ('/')
def forside():
    return render_template ('landing_page.html')


@app.route ('/bolig')
def bolig_analyse_side():
    try:
        s3_client = boto3.client ('s3', region_name=AWS_REGION, aws_access_key_id=AWS_KEY,
                                  aws_secret_access_key=AWS_SECRET)
        latest_file_key = find_latest_file_in_s3 (s3_client, S3_BUCKET_NAME, 'raw/bolig-daglig/',
                                                  r'bolig_X_(\d{2}-\d{2}-\d{4})\.csv')
        filter_data = {'fylker': [], 'boligtyper': [], 'meglere': [], 'annonsepakker': []}
        if latest_file_key:
            obj = s3_client.get_object (Bucket=S3_BUCKET_NAME, Key=latest_file_key)
            df = pd.read_csv (io.BytesIO (obj['Body'].read ()), sep=';', encoding='utf-16', on_bad_lines='skip')
            df.columns = df.columns.str.strip ()
            if 'fylke' in df.columns: filter_data['fylker'] = sorted (df['fylke'].dropna ().unique ().tolist ())
            if 'boligtype' in df.columns: filter_data['boligtyper'] = sorted (
                df['boligtype'].dropna ().unique ().tolist ())
            if 'broker_name' in df.columns: filter_data['meglere'] = sorted (
                df['broker_name'].dropna ().unique ().tolist ())
            if 'annonsepakke' in df.columns: filter_data['annonsepakker'] = sorted (
                df['annonsepakke'].dropna ().unique ().tolist ())
    except Exception as e:
        print (f"Feil under forberedelse av bolig-filtre: {e}")
        filter_data = {'fylker': [], 'boligtyper': [], 'meglere': [], 'annonsepakker': []}

    # Utvider filter_data med ** for å pakke ut dictionaryen til nøkkelordargumenter
    return render_template ('analyse_template.html', tittel="Prisanalyse: Boliger for salg i Norge",
                            data_url="/get_bolig_data", **filter_data)


@app.route ('/get_bolig_data', methods=['POST'])
def get_bolig_data():
    try:
        s3_client = boto3.client ('s3', region_name=AWS_REGION, aws_access_key_id=AWS_KEY,
                                  aws_secret_access_key=AWS_SECRET)
        latest_file_key = find_latest_file_in_s3 (s3_client, S3_BUCKET_NAME, 'raw/bolig-daglig/',
                                                  r'bolig_X_(\d{2}-\d{2}-\d{4})\.csv')
        if not latest_file_key: return jsonify ({"error": "Ingen bolig-datafil funnet"}), 404

        obj = s3_client.get_object (Bucket=S3_BUCKET_NAME, Key=latest_file_key)
        df = pd.read_csv (io.BytesIO (obj['Body'].read ()), sep=';', encoding='utf-16', on_bad_lines='skip')
        df.columns = df.columns.str.strip ()

        if 'publisert_dato' in df.columns:
            df['publisert_dato_dt'] = pd.to_datetime (df['publisert_dato'], errors='coerce', utc=True)
            now_utc = pd.Timestamp.now ('UTC')
            df['dager_paa_markedet'] = (now_utc - df['publisert_dato_dt']).dt.days
        else:
            df['dager_paa_markedet'] = None

        filters = request.get_json ().get ('filters', {})
        for col in ['totalpris', 'M2-pris', 'dager_paa_markedet']:
            if col in df.columns: df[col] = pd.to_numeric (df[col], errors='coerce')

        if filters.get ('fylke') and filters['fylke'] != 'Alle': df = df[df['fylke'] == filters['fylke']]
        if filters.get ('totalpris_fra'): df = df[df['totalpris'] >= int (filters['totalpris_fra'])]
        if filters.get ('totalpris_til'): df = df[df['totalpris'] <= int (filters['totalpris_til'])]
        if filters.get ('dager_fra'): df = df[df['dager_paa_markedet'] >= int (filters['dager_fra'])]
        if filters.get ('dager_til'): df = df[df['dager_paa_markedet'] <= int (filters['dager_til'])]

        df = df.where (pd.notna (df), None)
        return jsonify (json.loads (df.to_json (orient='records')))
    except Exception as e:
        print (f"Feil i /get_bolig_data: {e}")
        return jsonify ({"error": "Intern feil"}), 500


@app.route('/bil')
def bil_analyse_side():
    """Viser den avanserte analysesiden for bil og henter metadata."""
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
        meta_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key='calc/metadata.json')
        metadata = json.loads(meta_obj['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"ADVARSEL: Kunne ikke laste metadata for bil. Feil: {e}")
        metadata = {}

    # ENDRING: Vi sender nå hver variabel eksplisitt i stedet for å bruke **metadata
    return render_template(
        'bil_analyse_template.html',
        tittel="Analyse av bruktbilmarkedet",
        data_url="/get_bil_data",
        produsenter=metadata.get('produsenter', []),
        drivstoff_opts=metadata.get('drivstoff_opts', []),
        hjuldrift_opts=metadata.get('hjuldrift_opts', []),
        models_by_prod=json.dumps(metadata.get('models_by_prod', {})),
        year_min=metadata.get('year_min', 2000),
        year_max=metadata.get('year_max', date.today().year),
        km_min=metadata.get('km_min', 0),
        km_max=metadata.get('km_max', 300000)
    )

@app.route ('/get_bil_data', methods=['POST'])
def get_bil_data():
    try:
        my_session = boto3.Session (aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, region_name=AWS_REGION)
        filters = request.get_json ().get ('filters', {})

        where_clauses = [f"date(dato) >= DATE('{filters.get ('startdato', DEFAULT_STARTDATE.isoformat ())}')"]
        # DENNE BLOKKEN ER KORREKT OG REN
        if filters.get ('produsent'):
            where_clauses.append (f"produsent = '{filters['produsent']}'")

        if filters.get ('modell'):
            safe_modell = filters['modell'].replace ("'", "''")
            where_clauses.append (f"modell = '{safe_modell}'")

        if filters.get ('modell_sok'):
            safe_sok = filters['modell_sok'].lower ().replace ("'", "''")
            where_clauses.append (f"LOWER(overskrift) LIKE '%{safe_sok}%'")

        if filters.get ('seller_sok'):
            safe_sok = filters['seller_sok'].lower ().replace ("'", "''")
            where_clauses.append (f"LOWER(selger) LIKE '%{safe_sok}%'")

        if filters.get ('range_min'):
            where_clauses.append (f"rekkevidde_str >= {int (filters['range_min'])}")

        if filters.get ('range_max'):
            where_clauses.append (f"rekkevidde_str <= {int (filters['range_max'])}")

        query = f"SELECT * FROM database_biler_parquet WHERE {' AND '.join (where_clauses)}"
        df = wr.athena.read_sql_query (sql=query, database=ATHENA_DATABASE,
                                       s3_output=f"s3://{S3_BUCKET_NAME}/athena-results/", boto3_session=my_session)

        if df.empty:
            return jsonify ({'historikk': [], 'daily_stats': []})
        df.columns = [c.lower () for c in df.columns]

        if filters.get ('drivstoff'): df = df[df['drivstoff'].isin (filters['drivstoff'])]
        if filters.get ('hjuldrift'): df = df[df['hjuldrift'].isin (filters['hjuldrift'])]
        if filters.get ('year_min'): df = df[df['årstall'] >= int (filters['year_min'])]
        if filters.get ('year_max'): df = df[df['årstall'] <= int (filters['year_max'])]
        if filters.get ('km_min'): df = df[df['kjørelengde'] >= int (filters['km_min'])]
        if filters.get ('km_max'): df = df[df['kjørelengde'] <= int (filters['km_max'])]

        if df.empty: return jsonify ({'historikk': [], 'daily_stats': []})

        daily_stats_df = df.groupby ('dato').agg (Antall_Solgt=('pris_num', lambda x: (x == 0).sum ()),
                                                  Median_Pris_Usolgt=(
                                                  'pris_num', lambda x: x[x > 0].median ())).reset_index ()
        daily_stats_df['Dato'] = pd.to_datetime (daily_stats_df['dato']).dt.strftime ('%Y-%m-%d')
        daily_stats = json.loads (daily_stats_df.to_json (orient='records'))

        historikk_df = df.sort_values ('dato').groupby ('finnkode').agg (
            overskrift=('overskrift', 'last'), årstall=('årstall', 'last'), kjørelengde=('kjørelengde', 'last'),
            drivstoff=('drivstoff', 'last'), hjuldrift=('hjuldrift', 'last'), rekkevidde=('rekkevidde_str', 'last'),
            selger=('selger', 'last'), dato_start=('dato', 'first'), dato_end=('dato', 'last'),
            pris_start=('pris_num', 'first'),
            pris_last=('pris_num', lambda x: x[x > 0].iloc[-1] if not x[x > 0].empty else None)
        ).reset_index ()
        historikk_df['dager'] = (
                    pd.to_datetime (historikk_df['dato_end']) - pd.to_datetime (historikk_df['dato_start'])).dt.days
        historikk_df['prisfall'] = historikk_df['pris_last'] - historikk_df['pris_start']
        historikk = json.loads (historikk_df.to_json (orient='records'))

        return jsonify ({'historikk': historikk, 'daily_stats': daily_stats})

    except Exception as e:
        print (f"Feil i /get_bil_data: {e}")
        return jsonify ({"error": str (e)}), 500


if __name__ == '__main__':
    app.run (debug=True)