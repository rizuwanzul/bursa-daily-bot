"""
Simple Bot to send Telegram messages for latest research reports.
This script performs web scraping to extract latest available research reports
and send it to Telegram Channel - BursaDaily.
"""

import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
import urllib.parse
import time
import io
import fitz
import os
import telegram
from telegram.utils.helpers import escape_markdown


def main():
    DATE = pd.Timestamp.today(
        tz='Asia/Kuala_Lumpur').floor('d').tz_localize(None)
    CHAT_ID = os.environ.get('CHAT_ID')
    CHAT_ID_LOG = os.environ.get('CHAT_ID_LOG')
    TOKEN = os.environ.get('TOKEN')

    # Load latest stocks with price target list
    latest_df = get_latest_price_target(DATE)

    bot = telegram.Bot(token=TOKEN)
    if len(latest_df) == 0:
        bot.send_message(
            chat_id=CHAT_ID_LOG,
            text='No latest report is available.',
            disable_notification=True,
            timeout=30
        )
    else:
        # Load stocks dataframe
        stocks_df = get_stocks()

        # If 'all new' i.e. 50 rows of price target, then check individual stocks if any new price target
        stocks = latest_df['Stock Name'].unique().tolist()
        if len(latest_df) == 50:
            last_row = latest_df.loc[49, 'Stock Name']
            cond = stocks_df['Stock Name'] > last_row
            stocks = stocks + stocks_df[cond]['Stock Name'].tolist()
            stocks = sorted(list(set(stocks)))

        # Load price target for each stock into dataframe
        appended_df = [get_price_target_by_stock(stock) for stock in stocks]
        report_df = pd.concat(appended_df, ignore_index=True)

        # Filter latest price
        is_latest = report_df['Date'] >= DATE
        new_report_df = report_df[is_latest].copy().reset_index(drop=True)
        new_report_df = new_report_df.sort_values(
            by=['Date', 'Stock Name'], ignore_index=True)

        # Extract details info for price target
        new_report_df['Title'], new_report_df['Post'], new_report_df['Pdf'] = zip(
            *new_report_df['Link'].apply(get_link_details))

        # Add shariah status into dataframe
        new_report_df = pd.merge(
            new_report_df, stocks_df, how='left', on='Stock Name')
        new_report_df['Shariah'] = new_report_df['Shariah'].fillna('')

        # Generate caption and text message
        new_report_df['Caption'], new_report_df['Text'] = zip(
            *new_report_df.apply(generate_caption_text, axis=1))

        # Send latest reports to Telegram Channel
        new_report_df['Status'] = ''
        for index, row in new_report_df.iterrows():
            # Initialise var to check if message has been sent
            not_sent = True
            if row['Pdf'] != '':
                try:
                    response = requests.get(
                        f"https:{urllib.parse.quote(row['Pdf'])}")
                    content_type = response.headers.get('content-type')

                    if 'application/pdf' in content_type:
                        png = generate_photo(response.content)
                        bot.send_photo(
                            chat_id=CHAT_ID,
                            photo=png,
                            caption=row['Caption'],
                            parse_mode=telegram.ParseMode.MARKDOWN_V2,
                            disable_notification=True,
                            timeout=30
                        )
                        not_sent = False
                        new_report_df.at[index, 'Status'] = 'Sent'
                        time.sleep(3)
                except:
                    pass

            if not_sent:
                bot.send_message(
                    chat_id=CHAT_ID,
                    text=row['Text'],
                    parse_mode=telegram.ParseMode.MARKDOWN_V2,
                    disable_notification=True,
                    timeout=30
                )
                new_report_df.at[index, 'Status'] = 'Sent'
                time.sleep(3)

        # Send error/completion message
        if new_report_df[new_report_df['Status'] != 'Sent'].shape[0] > 0:
            bot.send_message(
                chat_id=CHAT_ID_LOG,
                text='Not all reports are submitted.',
                disable_notification=True,
                timeout=30
            )
        else:
            bot.send_message(
                chat_id=CHAT_ID_LOG,
                text='Task is completed.',
                disable_notification=True,
                timeout=30
            )


def fetch(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = bs(response.text, 'html.parser')

    return soup


def get_latest_price_target(date):
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(
        'http://klse.i3investor.com/jsp/pt.jsp', headers=headers)
    df = pd.read_html(response.text, attrs={'class': 'nc'})[0]

    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
    df = df[df['Date'] >= date]

    return df


def get_stocks():
    # Get list of stocks
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(
        'https://www.bursamarketplace.com/bin/json/stockheatmap.json', headers=headers)
    stock_df = pd.json_normalize(response.json()['children'])[
        ['name', 'data.shariah']]
    stock_df.columns = ['Stock Name', 'Shariah']

    # Get list of reit
    reit_df = pd.read_html('https://www.isaham.my/sector/reits',
                           attrs={'id': 'myTable'})[0][['Stock']]
    reit_df.columns = ['Stock Name']
    reit_df[['Stock Name', 'Shariah']
            ] = reit_df['Stock Name'].str.split(expand=True)
    di = {None: 'Yes', '[NS]': 'No'}
    reit_df['Shariah'] = reit_df['Shariah'].map(di)

    # Combine into one df
    df = pd.concat([stock_df, reit_df], ignore_index=True)
    df.sort_values(by=['Stock Name'], inplace=True, ignore_index=True)
    di = {'Yes': '', 'No': '[NS] '}
    df['Shariah'] = df['Shariah'].map(di)

    return df


def get_price_target_by_stock(stock):
    soup = fetch(
        f'https://klse.i3investor.com/ptservlet.jsp?sa=pts&q={urllib.parse.quote(stock)}')
    table = soup.find('table', attrs={'class': 'nc'})
    if table:
        if not table.find('span', attrs={'class': 'warn'}):
            rows = table.find_all('tr')
            records = []
            columns = []
            for row in rows:
                headers = row.find_all('th')
                if headers != []:
                    columns = [header.text for header in headers]
                else:
                    cols = row.find_all('td')
                    record = [col.text.strip() for col in cols]
                    link = cols[6].a['href']
                    record.insert(7, link)
                    records.append(record)

            columns.insert(7, 'Link')
            df = pd.DataFrame(data=records, columns=columns)
            df.insert(0, 'Stock Name', stock)
            df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')

            return df

    return None


def get_link_details(link):
    soup = fetch(f'https://klse.i3investor.com{link}')
    post, title, pdf = [''] * 3

    h2 = soup.find('h2')
    if h2:
        title = h2.text

    content = soup.find('div', {'class': 'doccontent'})
    if content:
        p = content.find_all('p')
        if p:
            a = p[-1].find('a', href=True)
            if a:
                post = a['href']
                if post:
                    pdf = get_pdf(post)

    return title, post, pdf


def get_pdf(post):
    soup = fetch(f'https://klse.i3investor.com{post}')
    pdf = soup.find('object')
    if pdf:
        return pdf['data']

    return ''


def generate_caption_text(row):
    broker_house = {
        'BIMB': 'BIMB Securities Research',
        'PUBLIC BANK': 'PublicInvest Research',
        'MIDF': 'MIDF Research',
        'KENANGA': 'Kenanga Research',
        'HLG': 'Hong Leong Investment Bank Research',
        'AmInvest': 'AmInvest Research',
        'AffinHwang': 'Affin Hwang Research',
        'JF APEX': 'JF Apex Securities Research',
        'MalaccaSecurities': 'Mplus Research',
        'RHB-OSK': 'RHB Securities Research',
        'ALLIANCE': 'Alliance Research',
        'MERCURY': 'Mercury Research',
        'TA': 'TA Research',
        'Rakuten': 'Rakuten Research',
        'MACQUARIE GROUP': 'Macquarie Research',
        'CIMB': 'CIMB Research',
        'CREDIT SUISSE': 'Credit Suisse',
        'UBS': 'UBS Research',
        'CITI GROUP': 'Citi Research',
        'UOBKayHian': 'UOB Kay Hian'
    }

    name = escape_markdown(row['Stock Name'], version=2)
    shariah = escape_markdown(row['Shariah'], version=2)
    tp = escape_markdown(row['Target Price'], version=2)
    change = escape_markdown(row['Upside/Downside'], version=2)
    call = escape_markdown(row['Price Call'].title(), version=2)
    title = escape_markdown(row['Title'], version=2)
    date = escape_markdown(row['Date'].strftime('(%d/%m/%Y)'), version=2)
    if row['Source'] in broker_house:
        broker = escape_markdown(broker_house[row['Source']], version=2)
    else:
        broker = escape_markdown(row['Source'], version=2)
    link = escape_markdown(
        f"https://klse.i3investor.com{row['Link']}", version=2)
    pdf = escape_markdown(f"https:{urllib.parse.quote(row['Pdf'])}", version=2)

    caption = f'*{name} {shariah}*\({call}\); Target: RM{tp}\n[{title}]({pdf}) by {broker} {date}\n\n{link}'
    text = f'*{name} {shariah}*\({call}\); Target: RM{tp}\nResearch report by {broker} {date}\n\n{link}'

    return caption, text


def generate_photo(content):
    pdf = io.BytesIO(content)
    doc = fitz.open(stream=pdf, filetype='pdf')
    page = doc[0]
    trans = fitz.Matrix(150/72, 150/72)
    pix = page.getPixmap(matrix=trans)
    png = io.BytesIO(pix.getPNGData())

    return png


if __name__ == "__main__":
    main()
