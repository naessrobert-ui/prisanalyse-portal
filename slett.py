import os
import re
import json
from datetime import datetime, date
from io import StringIO

import pandas as pd
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, State, dash_table
from dash.exceptions import PreventUpdate
import awswrangler as wr
import boto3
from dotenv import load_dotenv
import plotly.express as px

# ===================== Konfig =====================
BASE_DIR = os.path.dirname (os.path.abspath (__file__))
DOTENV_PATH = os.path.join (BASE_DIR, '.env')
if os.path.exists (DOTENV_PATH):
    print (f"Laster .env fil fra: {DOTENV_PATH}")
    load_dotenv (dotenv_path=DOTENV_PATH)

AWS_S3_BUCKET_NAME = os.environ.get ("AWS_S3_BUCKET_NAME")
AWS_S3_REGION = os.environ.get ("AWS_S3_REGION")
ATHENA_DATABASE = os.environ.get ("ATHENA_DATABASE", "bil_finn_daglig")
DEFAULT_STARTDATE = date (2025, 5, 1)  # Justert startdato for å inkludere mer data

if AWS_S3_REGION:
    boto3.setup_default_session (region_name=AWS_S3_REGION)

# ===================== App-initialisering =====================
app = dash.Dash (__name__, external_stylesheets=[dbc.themes.FLATLY])
server = app.server

# ===================== Metadata-innlesing =====================
prod_list, models_by_prod_initial, drivstoff_pref_opts, hjuldrift_pref_opts = [], {}, [], []
year_min, year_max, km_min, km_max, latest_dt = 2000, date.today ().year, 0, 300000, date.today ()
try:
    if AWS_S3_BUCKET_NAME and AWS_S3_REGION:
        s3 = boto3.client ('s3')
        metadata_key = 'calc/metadata.json'
        print (f"Henter metadata fra: s3://{AWS_S3_BUCKET_NAME}/{metadata_key}")
        meta_obj = s3.get_object (Bucket=AWS_S3_BUCKET_NAME, Key=metadata_key)
        metadata = json.loads (meta_obj['Body'].read ().decode ('utf-8'))
    else:
        # Fallback for lokal kjøring uten S3
        LOCAL_METADATA_FIL = 'metadata.json'
        with open (LOCAL_METADATA_FIL, 'r', encoding='utf-8') as f:
            metadata = json.load (f)

    prod_list = metadata.get ('produsenter', [])
    models_by_prod_initial = metadata.get ('models_by_prod', {})
    drivstoff_pref_opts = metadata.get ('drivstoff_opts', [])
    hjuldrift_pref_opts = metadata.get ('hjuldrift_opts', [])
    year_min = metadata.get ('year_min', 2000)
    year_max = metadata.get ('year_max', date.today ().year)
    km_min = metadata.get ('km_min', 0)
    km_max = metadata.get ('km_max', 300000)
    latest_dt = date.fromisoformat (metadata.get ('latest_dt', date.today ().isoformat ()))
except Exception as e:
    print (f"ADVARSEL: Kunne ikke laste metadata. Feil: {e}")


# ===================== Hovedlogikk med Athena =====================
def load_and_process_data(selected_produsent, selected_modell, min_dato, max_dato, **kwargs):
    where_clauses = [
        f"date(dato) >= DATE('{min_dato.isoformat ()}')",
        f"date(dato) <= DATE('{max_dato.isoformat ()}')"
    ]

    if selected_produsent:
        where_clauses.append (f"produsent = '{selected_produsent}'")

    if selected_modell:
        safe_modell = selected_modell.replace ("'", "''")
        where_clauses.append (f"modell = '{safe_modell}'")

    modell_sok = kwargs.get ('pf_modell_sok')
    if modell_sok and modell_sok.strip ():
        safe_sok = modell_sok.replace ("'", "''").lower ()
        where_clauses.append (f"LOWER(overskrift) LIKE '%{safe_sok}%'")

    seller_sok = kwargs.get ('pf_seller_sok')
    if seller_sok and seller_sok.strip ():
        safe_sok = seller_sok.replace ("'", "''").lower ()
        where_clauses.append (f"LOWER(selger) LIKE '%{safe_sok}%'")

    # NYTT: Legg til rekkevidde i SQL-spørringen
    range_min = kwargs.get ('pf_range_min')
    if range_min is not None and str (range_min).strip ():
        where_clauses.append (f"rekkevidde_str >= {int (range_min)}")

    range_max = kwargs.get ('pf_range_max')
    if range_max is not None and str (range_max).strip ():
        where_clauses.append (f"rekkevidde_str <= {int (range_max)}")

    query = f"SELECT * FROM database_biler_parquet WHERE {' AND '.join (where_clauses)}"

    print (f"Kjører Athena-spørring: {query}")
    try:
        df = wr.athena.read_sql_query (sql=query, database=ATHENA_DATABASE,
                                       s3_output=f"s3://{AWS_S3_BUCKET_NAME}/athena-results/")
    except Exception as e:
        print (f"Feil under kjøring av Athena-spørring: {e}")
        return pd.DataFrame (), []

    if df.empty: return pd.DataFrame (), []

    df.columns = [c.lower () for c in df.columns]

    # Post-filtrering (for filtre som er enklere i Pandas)
    if kwargs.get ('pf_drivstoff'): df = df[df['drivstoff'].isin (kwargs['pf_drivstoff'])]
    if kwargs.get ('pf_hjuldrift'): df = df[df['hjuldrift'].isin (kwargs['pf_hjuldrift'])]
    if kwargs.get ('pf_year_min') is not None and str (kwargs.get ('pf_year_min')).strip ():
        df = df[df['årstall'] >= int (kwargs['pf_year_min'])]
    if kwargs.get ('pf_year_max') is not None and str (kwargs.get ('pf_year_max')).strip ():
        df = df[df['årstall'] <= int (kwargs['pf_year_max'])]
    if kwargs.get ('pf_km_min') is not None and str (kwargs.get ('pf_km_min')).strip ():
        df = df[df['kjørelengde'] >= int (kwargs['pf_km_min'])]
    if kwargs.get ('pf_km_max') is not None and str (kwargs.get ('pf_km_max')).strip ():
        df = df[df['kjørelengde'] <= int (kwargs['pf_km_max'])]

    df_filtered = df.copy ()
    if df_filtered.empty: return pd.DataFrame (), []

    # Aggregert statistikk
    daily_stats_df = df_filtered.groupby ('dato').agg (Antall_Totalt=('finnkode', 'size'),
                                                       Antall_Solgt=('pris_num', lambda x: (x == 0).sum ()),
                                                       Median_Pris_Usolgt=(
                                                       'pris_num', lambda x: x[x > 0].median ())).reset_index ()
    daily_stats_df['Median_Pris_Usolgt'] = daily_stats_df['Median_Pris_Usolgt'].astype (object).where (
        pd.notna (daily_stats_df['Median_Pris_Usolgt']), None)
    daily_stats_df['Dato'] = pd.to_datetime (daily_stats_df['dato']).dt.strftime ('%Y-%m-%d')
    daily_stats_df = daily_stats_df.drop (columns=['dato'])
    daily_stats = daily_stats_df.to_dict ('records')

    # Bygg historikk
    historikk = df_filtered.sort_values ('dato').groupby ('finnkode').agg (
        Produsent=('produsent', 'last'), Modell=('modell', 'last'), Overskrift=('overskrift', 'last'),
        årstall=('årstall', 'last'), kjørelengde=('kjørelengde', 'last'), drivstoff=('drivstoff', 'last'),
        hjuldrift=('hjuldrift', 'last'), Rekkevidde_str=('rekkevidde_str', 'last'), selger=('selger', 'last'),
        Dato_start=('dato', 'first'), Dato_end=('dato', 'last'), Pris_start=('pris_num', 'first'),
        Pris_last=('pris_num', lambda x: x[x > 0].iloc[-1] if not x[x > 0].empty else None)
    ).reset_index ()
    historikk.columns = [c.lower () for c in historikk.columns]
    historikk['dager'] = (pd.to_datetime (historikk['dato_end']) - pd.to_datetime (historikk['dato_start'])).dt.days
    historikk['prisfall'] = historikk['pris_last'] - historikk['pris_start']
    historikk['rekkevidde'] = historikk['rekkevidde_str']
    historikk['dato_start'] = pd.to_datetime (historikk['dato_start']).dt.strftime ('%Y-%m-%d')
    historikk['dato_end'] = pd.to_datetime (historikk['dato_end']).dt.strftime ('%Y-%m-%d')

    return historikk, daily_stats


# ===================== Health Check =====================
@server.route ('/health')
def health_check(): return "OK", 200


# ===================== Layout =====================
app.layout = dbc.Container ([
    dcc.Store (id='stored-cars-data'), dcc.Store (id='stored-daily-stats'),
    dcc.Store (id='models-by-prod-store', data=models_by_prod_initial),
    dbc.Row (dbc.Col (html.H1 ("Analyse av bruktbilmarkedet", className="text-center my-4"))),
    dbc.Row (dbc.Col (dbc.Alert ([html.H4 ("Velkommen!"), dcc.Markdown (
        "1. **Velg filtre** for å definere ditt søk.\n2. Du kan søke på tvers av alle merker ved å bruke søkefeltene.\n3. Klikk **\"Last inn data\"** for å hente resultater fra databasen.")],
                                 color="info"))),
    dbc.Row (dbc.Col (html.H4 ("1. Velg data og forhåndsfiltrer"), className="mt-4")),

    dbc.Row ([
        dbc.Col ([html.Label ("Produsent"), dcc.Dropdown (id='dropdown-produsent', options=prod_list)], md=2),
        dbc.Col ([html.Label ("Modell"), dcc.Dropdown (id='dropdown-modell', disabled=True)], md=2),
        dbc.Col ([html.Label ("Startdato"),
                  dcc.DatePickerSingle (id='input-startdato', date=DEFAULT_STARTDATE, display_format='DD-MM-YYYY',
                                        clearable=True, style={'width': '100%'})], md=2),
        dbc.Col (
            [html.Label ("Drivstoff"), dcc.Dropdown (id='prefilt-drivstoff', options=drivstoff_pref_opts, multi=True)],
            md=3),
        dbc.Col (
            [html.Label ("Hjuldrift"), dcc.Dropdown (id='prefilt-hjuldrift', options=hjuldrift_pref_opts, multi=True)],
            md=3),
    ], className="mb-3 g-2"),
    dbc.Row ([
        dbc.Col ([html.Label ("Årstall fra/til"), dbc.Row ([
            dbc.Col (dcc.Input (id='prefilt-year-fra', type='number', placeholder=year_min)),
            dbc.Col (dcc.Input (id='prefilt-year-til', type='number', placeholder=year_max))
        ])], md=2),
        dbc.Col ([html.Label ("Kjørelengde fra/til"), dbc.Row ([
            dbc.Col (dcc.Input (id='prefilt-km-fra', type='number', placeholder=km_min)),
            dbc.Col (dcc.Input (id='prefilt-km-til', type='number', placeholder=km_max))
        ])], md=2),
        # NYTT: Rekkevidde-filter er lagt til her
        dbc.Col ([html.Label ("Rekkevidde fra/til"), dbc.Row ([
            dbc.Col (dcc.Input (id='prefilt-range-fra', type='number', placeholder=0)),
            dbc.Col (dcc.Input (id='prefilt-range-til', type='number', placeholder=800))
        ])], md=2),
        dbc.Col ([html.Label ("Søk i annonseoverskrift"),
                  dcc.Input (id='prefilt-modell-sok', placeholder='F.eks. hengerfeste...', debounce=True,
                             className="w-100")], md=3),
        dbc.Col ([html.Label ("Selger"),
                  dcc.Input (id='prefilt-seller-sok', placeholder='F.eks. Møller...', debounce=True,
                             className="w-100")], md=3)
    ], className="mb-3 g-2"),

    dbc.Row (
        dbc.Col (dbc.Button ('Last inn data', id='load-data-button', color="primary", size="lg", className="w-100"),
                 md=4), className="mb-3"),
    dbc.Row (dbc.Col (html.Div (id='loading-output', className="text-center text-muted"))),
    dbc.Row (dbc.Col (html.Hr (), className="my-4")),
    dbc.Row (dbc.Col (html.H2 ("Aggregert statistikk", className="text-center mb-3"))),
    dbc.Row (id='kpi-output', className="mb-4"),
    dbc.Row ([
        dbc.Col (dbc.Spinner (dcc.Graph (id='price-line-chart')), md=6),
        dbc.Col (dbc.Spinner (dcc.Graph (id='sold-bar-chart')), md=6),
    ]),
    dbc.Row (dbc.Col (html.Hr (), className="my-4")),
    dbc.Row (dbc.Col (html.H2 ("2. Utforsk resultater", className="text-center mb-3"))),

    dbc.Card ([dbc.CardHeader ("Visningsfiltre"), dbc.CardBody ([
        dbc.Row ([
            dbc.Col ([html.Label ("Drivstoff"), dcc.Dropdown (id='res-drivstoff', multi=True)], md=3),
            dbc.Col ([html.Label ("Hjuldrift"), dcc.Dropdown (id='res-hjuldrift', multi=True)], md=3),
            dbc.Col ([html.Label ("Årstall fra/til"), dbc.Row ([
                dbc.Col (dcc.Input (id='res-year-fra', type='number', placeholder=year_min)),
                dbc.Col (dcc.Input (id='res-year-til', type='number', placeholder=year_max))
            ])], md=3),
            dbc.Col ([html.Label ("Dager til salgs"),
                      dcc.RangeSlider (id='res-days-slider', min=0, max=365, step=5, value=[0, 365], marks=None,
                                       tooltip={"placement": "bottom", "always_visible": True})], md=3)
        ], className="mb-3 g-2"),
        dbc.Row ([
            dbc.Col ([html.Label ("Pris"),
                      dcc.RangeSlider (id='res-price-slider', min=0, max=2000000, step=10000, value=[0, 2000000],
                                       marks=None,
                                       tooltip={"placement": "bottom", "always_visible": True})], md=6),
            dbc.Col ([html.Label ("Kjørelengde"),
                      dcc.RangeSlider (id='res-km-slider', min=0, max=300000, step=5000, value=[0, 300000], marks=None,
                                       tooltip={"placement": "bottom", "always_visible": True})], md=6)
        ], className="g-2")
    ])], className="mb-4"),

    dbc.Row (dbc.Col (html.Div (id='antall-biler-funnet', className="text-center fw-bold fs-5 mb-3"))),
    dbc.Spinner (html.Div (id='output-bil-tabell'))
], fluid=True, className="dbc")


# ===================== Callbacks =====================
@app.callback (Output ('dropdown-modell', 'options'), Output ('dropdown-modell', 'disabled'),
               Input ('dropdown-produsent', 'value'), State ('models-by-prod-store', 'data'))
def set_modell_options(selected_produsent, models_by_prod_data):
    if not selected_produsent: return [], True
    return models_by_prod_data.get (selected_produsent, []), False


@app.callback (
    Output ('stored-cars-data', 'data'), Output ('stored-daily-stats', 'data'),
    Output ('loading-output', 'children'), Output ('res-drivstoff', 'options'),
    Output ('res-hjuldrift', 'options'), Input ('load-data-button', 'n_clicks'),
    [
        State ('dropdown-produsent', 'value'), State ('dropdown-modell', 'value'),
        State ('input-startdato', 'date'), State ('prefilt-drivstoff', 'value'),
        State ('prefilt-hjuldrift', 'value'), State ('prefilt-km-fra', 'value'),
        State ('prefilt-km-til', 'value'), State ('prefilt-year-fra', 'value'),
        State ('prefilt-year-til', 'value'), State ('prefilt-modell-sok', 'value'),
        State ('prefilt-seller-sok', 'value'),
        State ('prefilt-range-fra', 'value'),  # NYTT
        State ('prefilt-range-til', 'value')  # NYTT
    ]
)
def load_selected_data(n_clicks, produsent, modell, startdato_str, pf_drivstoff, pf_hjuldrift,
                       km_fra, km_til, year_fra, year_til, modell_sok, seller_sok, range_fra, range_til):
    if not n_clicks: raise PreventUpdate
    if not produsent and not modell_sok and not seller_sok:
        return dash.no_update, dash.no_update, "Vennligst velg en produsent eller bruk et av søkefeltene.", dash.no_update, dash.no_update

    min_dato = date.fromisoformat (startdato_str) if startdato_str else DEFAULT_STARTDATE

    df_loaded, daily_stats = load_and_process_data (
        produsent, modell, min_dato, date.today (),
        pf_drivstoff=pf_drivstoff or [], pf_hjuldrift=pf_hjuldrift or [],
        pf_km_min=km_fra, pf_km_max=km_til, pf_year_min=year_fra, pf_year_max=year_til,
        pf_modell_sok=modell_sok, pf_seller_sok=seller_sok,
        pf_range_min=range_fra, pf_range_max=range_til  # NYTT
    )

    res_drivstoff_opts = sorted (
        df_loaded['drivstoff'].dropna ().unique ()) if 'drivstoff' in df_loaded and not df_loaded.empty else []
    res_hjuldrift_opts = sorted (
        df_loaded['hjuldrift'].dropna ().unique ()) if 'hjuldrift' in df_loaded and not df_loaded.empty else []
    msg = f"Lastet inn {len (df_loaded)} bilhistorikker."
    return df_loaded.to_json (orient='split'), json.dumps (daily_stats), msg, res_drivstoff_opts, res_hjuldrift_opts


@app.callback (
    Output ('kpi-output', 'children'), Output ('price-line-chart', 'figure'),
    Output ('sold-bar-chart', 'figure'), Input ('stored-daily-stats', 'data')
)
def update_statistics_and_graphs(daily_stats_json):
    # Denne funksjonen er uendret
    if not daily_stats_json:
        empty_figure = {'data': [], 'layout': {}, 'frames': []}
        return [], empty_figure, empty_figure
    df_stats = pd.DataFrame (json.loads (daily_stats_json))
    if df_stats.empty:
        empty_figure = {'data': [], 'layout': {}, 'frames': []}
        alert = dbc.Alert ("Ingen statistikk funnet.", color="warning", className="text-center w-75 mx-auto")
        return [alert], empty_figure, empty_figure
    df_stats['Antall_Solgt'] = pd.to_numeric (df_stats['Antall_Solgt'], errors='coerce').fillna (0)
    df_stats['Median_Pris_Usolgt'] = pd.to_numeric (df_stats['Median_Pris_Usolgt'], errors='coerce')
    avg_sold_per_day = df_stats['Antall_Solgt'].mean ()
    avg_median_price = df_stats[df_stats['Median_Pris_Usolgt'].notna () & (df_stats['Median_Pris_Usolgt'] > 0)][
        'Median_Pris_Usolgt'].mean ()
    avg_sold_text = f"{avg_sold_per_day:.1f}"
    avg_price_text = f"{int (avg_median_price):,}".replace (",", " ") if pd.notna (avg_median_price) else "N/A"
    kpi_cards = dbc.Row ([
        dbc.Col (dbc.Card ([dbc.CardHeader ("Gj.snitt antall solgt per dag"),
                            dbc.CardBody ([html.H4 (avg_sold_text, className="card-title text-center")])
                            ], color="primary", outline=True), md=3),
        dbc.Col (dbc.Card ([dbc.CardHeader ("Gj.snitt medianpris (usolgt)"),
                            dbc.CardBody ([html.H4 (f"{avg_price_text} kr", className="card-title text-center")])
                            ], color="primary", outline=True), md=3)
    ], justify="center", className="mb-4")
    fig_price = px.line (
        df_stats, x='Dato', y='Median_Pris_Usolgt', title='Medianpris over tid (usolgte biler)',
        labels={'Dato': 'Dato', 'Median_Pris_Usolgt': 'Medianpris (NOK)'},
        template='plotly_white', markers=True)
    fig_price.update_layout (title_x=0.5)
    fig_sold = px.bar (
        df_stats, x='Dato', y='Antall_Solgt', title='Antall solgte biler per dag',
        labels={'Dato': 'Dato', 'Antall_Solgt': 'Antall solgt'}, template='plotly_white')
    fig_sold.update_layout (title_x=0.5)
    return kpi_cards, fig_price, fig_sold


@app.callback (
    Output ('output-bil-tabell', 'children'), Output ('antall-biler-funnet', 'children'),
    [
        Input ('stored-cars-data', 'data'), Input ('res-drivstoff', 'value'),
        Input ('res-hjuldrift', 'value'), Input ('res-year-fra', 'value'),
        Input ('res-year-til', 'value'), Input ('res-price-slider', 'value'),
        Input ('res-km-slider', 'value'), Input ('res-days-slider', 'value')
    ]
)
def update_table(stored_json, f_drivstoff, f_hjuldrift, year_fra, year_til,
                 price_range, km_range, days_range):
    # Denne funksjonen er nå mye enklere
    if not stored_json:
        return html.P ("Velg filtre og trykk «Last inn data» for å begynne."), "Antall biler i utvalget: 0"

    df = pd.read_json (StringIO (stored_json), orient='split')
    if df.empty:
        return html.P ("Ingen biler lastet inn."), "Antall biler i utvalget: 0"

    if f_drivstoff: df = df[df['drivstoff'].isin (f_drivstoff)]
    if f_hjuldrift: df = df[df['hjuldrift'].isin (f_hjuldrift)]
    if year_fra is not None and str (year_fra).strip (): df = df[df['årstall'] >= int (year_fra)]
    if year_til is not None and str (year_til).strip (): df = df[df['årstall'] <= int (year_til)]
    df = df[df['pris_last'].between (price_range[0], price_range[1])]
    df = df[df['kjørelengde'].between (km_range[0], km_range[1])]
    df = df[df['dager'].between (days_range[0], days_range[1])]

    ant_txt = f"Antall biler i utvalget: {len (df)}"
    if df.empty:
        return dbc.Alert ("Ingen biler passer til de valgte visningsfiltrene.", color="warning"), ant_txt

    f_display = df.copy ()
    if 'årstall' in f_display.columns: f_display['årstall'] = f_display['årstall'].astype ('Int64')
    for col in ['dato_start', 'dato_end']:
        if col in f_display.columns: f_display[col] = pd.to_datetime (f_display[col]).dt.strftime ('%d.%m.%y')
    base_url = "https://www.finn.no/car/used/ad.html?finnkode="
    f_display['finnkode'] = f_display['finnkode'].apply (lambda fk: f'[{fk}]({base_url}{fk})')
    display_cols = ['finnkode', 'overskrift', 'årstall', 'kjørelengde', 'rekkevidde', 'pris_last', 'prisfall', 'dager',
                    'drivstoff', 'hjuldrift', 'selger', 'dato_start', 'dato_end']
    table_columns = []
    for col in display_cols:
        name = "Annonseoverskrift" if col == "overskrift" else col.replace ("_", " ").title ()
        if col == 'finnkode':
            table_columns.append ({"name": "FINN-kode", "id": col, "presentation": "markdown"})
        else:
            table_columns.append ({"name": name, "id": col})

    tbl = dash_table.DataTable (
        id='table', columns=table_columns, data=f_display.to_dict ('records'),
        markdown_options={'link_target': '_blank'}, sort_action="native",
        sort_by=[{'column_id': 'pris_last', 'direction': 'asc'}], page_size=15,
        style_cell={'textAlign': 'left', 'padding': '5px', 'whiteSpace': 'normal', 'height': 'auto'},
        style_header={'fontWeight': 'bold'}, style_table={'overflowX': 'auto'})

    return tbl, ant_txt


if __name__ == "__main__":
    app.run (debug=True, port=8050)