import datetime
import time
import feedparser
import requests
import psycopg2

def get_edgar_entries(feed):
    return feed.get('entries', [])

def convert_htm_to_txt(link):
    return link.replace('-index.htm', '.txt')

def search_for_bitcoin(link):
    if link[-len('.txt')] != '.txt':
        link = convert_htm_to_txt(link)

    text = str(requests.get(link).text).lower()
    for x in ['bitcoin']:
        if x in text:
            return True
    return False

def bitcoin_edgar_entry(entry):
    link = entry.get('link')

    if search_for_bitcoin(link):
        return {
            'title': entry.get('title'),
            'link': entry.get('link'),
            'updated': entry.get('updated'),
            'form': str([x.get('term') for x in entry.get('tags', []) if x.get('term')]),
            'isBitcoin': True
        }

def get_connection(database):
    return psycopg2.connect(f"dbname={database}")

def create_edgar_table(connection):
    cursor = connection.cursor()
    try:
        cursor.execute('''
            CREATE TABLE EDGAR_BITCOIN_FILING (
                id serial PRIMARY KEY,
                title varchar,
                link varchar,
                updated timestamp,
                form varchar,
                isBitcoin boolean
            );
        ''')
        connection.commit()
        cursor.close()
        return True
    except Exception as e:
        print(e)
        return False

def filter_bitcoin_entries(entries, connection, table='EDGAR_BITCOIN_FILING'):
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table} WHERE updated > now() - interval '1 day';")
    existing_rows = cursor.fetchall()
    output_entries = []
    for entry in entries:
        isNewEntry = True
        for existing_row in existing_rows:
            if (entry.get('title') == existing_row[1]):
                isNewEntry = False

        if isNewEntry:
            output_entries.append(entry)

    return output_entries


def insert_bitcoin_entries(entries, connection, table='EDGAR_BITCOIN_FILING'):
    cursor = connection.cursor()
    query = cursor.mogrify("INSERT INTO {} ({}) VALUES {} RETURNING {}".format(
                        table,
                        ', '.join(entries[0].keys()),
                        ', '.join(['%s'] * len(entries)),
                        'id'
                    ), [tuple(v.values()) for v in entries])
    try:
        cursor.execute(query)
        connection.commit()
        cursor.close()
        return True
    except Exception as e:
        print(e)
        return False

def print_bitcoin_entries(entries):
    output = ''
    for entry in entries:
        title = entry.get('title')
        link = entry.get('link')
        output += f'{title}: {link}\n'
    return output

if __name__ == '__main__':
    EDGAR_RSS = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=s-1&company=&dateb=&owner=include&start=0&count=40&output=atom'
    SLEEP_HOURS = 0
    SLEEP_MINUTES = 5
    SLEEP_TIME = SLEEP_HOURS*60*60 + SLEEP_MINUTES*60 # HOURS * 60 min/h * 60 s/min MINUTES * 60 s/min

    database = 'edgar_bitcoin_filings'

    connection = get_connection(database)
    print(create_edgar_table(connection))
    connection.close()

    while True:
        edgar_feed = feedparser.parse(EDGAR_RSS)
        entries = get_edgar_entries(edgar_feed)

        bitcoin_entries = []
        for entry in entries:
            temp_entry = bitcoin_edgar_entry(entry)
            if temp_entry:
                bitcoin_entries.append(temp_entry)

        connection = get_connection(database)
        bitcoin_entries = filter_bitcoin_entries(bitcoin_entries, connection)

        if bitcoin_entries:
            print(f'BITCOIN: {len(bitcoin_entries)} entry(s)!')
            print(print_bitcoin_entries(bitcoin_entries))
            insert_bitcoin_entries(bitcoin_entries, connection, table='EDGAR_BITCOIN_FILING')
        else:
            now = datetime.datetime.now()
            print(f'{now} NO BTC ETF NEWS...')
        connection.close()
        time.sleep(SLEEP_TIME)

