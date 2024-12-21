import requests
import numpy as np
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

def fetch_fred_data(series_ids, start=None, end=None):

    if isinstance(series_ids, str):
        series_ids = [series_ids]

    metadata_dict = {}
    data_frames = []

    for series_id in series_ids:

        html_content = requests.get(f'https://fred.stlouisfed.org/data/{series_id}').text
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract metadata
        metadata = {}
        metadata_table = soup.find_all('table')[0]

        for row in metadata_table.find_all('tr'):
            key = row.find('th').text.strip().lower().replace(' ', '_')
            value = ' '.join([str(elem) for elem in row.find('td').contents]).strip()
            metadata[key] = value

        metadata_dict[series_id] = metadata

        # Extract table data
        data_table = soup.find_all('table')[1]

        data = []

        # Skip header row
        for row in data_table.find_all('tr')[1:]:

            cells = row.find_all(['th', 'td'])
            date = cells[0].text.strip()
            value = cells[1].text.strip()
            data.append([date, value])

       # Check for extra rows in the div with id="extra-rows"
       # E.g., some series such as 'DEXUSEU' have this
        extra_rows_div = soup.find('div', id='extra-rows')

        if extra_rows_div and extra_rows_div.text.strip():
            extra_rows = extra_rows_div.text.strip().split('\n')

            for extra_row in extra_rows:
                extra_row = extra_row.replace('#', '')
                date, value = extra_row.split('|')
                data.append([date.strip(), value.strip()])

        df = pd.DataFrame(data, columns=['date', series_id])

        df['date'] = pd.to_datetime(df['date'])

        data_frames.append(df)

        df[series_id] = df[series_id].replace('.', np.nan)

    # Merge all data frames on date column using outer join
    if data_frames:
        result_df = data_frames[0]
        for df in data_frames[1:]:
            result_df = pd.merge(result_df, df, on='date', how='outer')
        result_df = result_df.set_index('date')

        # Filter DataFrame by the start and end dates if provided
        if start:
            result_df = result_df[result_df.index >= pd.to_datetime(start)]
        if end:
            result_df = result_df[result_df.index <= pd.to_datetime(end)]
    else:
        result_df = pd.DataFrame()

    result_df = result_df.sort_index()
    result_df = result_df.astype(float)

    return result_df, metadata_dict

series_ids = ['GDP', 'DEXUSEU', 'CBBTCUSD']

start_date = '2000-01-01'
end_date = datetime.now().strftime('%Y-%m-%d')

df, metadata_dict = fetch_fred_data(series_ids, start=start_date, end=end_date)
