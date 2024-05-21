from flask import Flask, Response
import requests
import pandas as pd
from datetime import datetime, timedelta

# Constants
PROMETHEUS_URL = 'http://prometheus:9090/api/v1/query_range'
QUERY = 'people_count'
START = (datetime.utcnow() - timedelta(days=30)).isoformat()+'Z'
END = datetime.utcnow().isoformat()+'Z'
STEP = '30m'

app = Flask(__name__)

# Function to fetch data from Prometheus
def fetch_prometheus_data(query, start, end, step):
    params = {
        'query': query,
        'start': start,
        'end': end,
        'step': step
    }
    response = requests.get(PROMETHEUS_URL, params=params)
    return response.json()

@app.route('/metrics')
def metrics():
    data = fetch_prometheus_data(QUERY, START, END, STEP)
    df = pd.DataFrame(data['data']['result'])

    output = {}

    # Process data for each series
    for index, row in df.iterrows():
        metric = row['metric']
        label = metric.get('name')
        values = row['values']

        df = pd.DataFrame(values, columns=['timestamp', 'value'])
        df['value'] = pd.to_numeric(df['value'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df.set_index('timestamp', inplace=True)
        result = df.groupby([df.index.dayofweek, pd.Grouper(freq='30min')]).mean()
        all_combinations = pd.MultiIndex.from_product(
            [range(7), pd.date_range('00:00', '23:30', freq='30min').time],
            names=['weekday', 'time']
        )
        result = result.reindex(all_combinations, fill_value=0)
        result.reset_index(inplace=True)
        output[label] = result.groupby('weekday')['value'].apply(list).to_dict()

    return Response(output, mimetype='application/json')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
