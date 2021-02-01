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

def get_connection(username, password, database):
    return psycopg2.connect(f"dbname={database} user={username}")

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

def insert_bitcoin_entries(entries, connection, table='EDGAR_BITCOIN_FILING'):
    cursor = connection.cursor()
    print(entries)
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
    
if __name__ == '__main__':
    EDGAR_RSS = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=&company=&dateb=&owner=include&start=0&count=40&output=atom'
    SLEEP_HOURS = 0
    SLEEP_MINUTES = 10
    SLEEP_TIME = SLEEP_HOURS*60*60 + SLEEP_MINUTES*60 # HOURS * 60 min/h * 60 s/min MINUTES * 60 s/min
    
    username = 'edgar_user'
    password = 'ElongatedMuskrat'
    database = 'edgar_bitcoin_filings'

    connection = get_connection(username, password, database)
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
        
        print(bitcoin_entries)
        if bitcoin_entries:
            print(f'BITCOIN: {len(bitcoin_entries)} entry(s)!')
            connection = get_connection(username, password, database)
            insert_bitcoin_entries(bitcoin_entries, connection, table='EDGAR_BITCOIN_FILING')
            connection.close()
        print('SLEEPING...')
        time.sleep(SLEEP_TIME)

