import re
import os
import json
import requests
import PyPDF2
import numpy as np
import pandas as pd
from datetime import datetime
from html import unescape
from bs4 import BeautifulSoup

from langchain_core.documents import Document
from langchain_community.vectorstores import Chroma
from langchain_community.document_loaders import PyPDFLoader

# A mapping of full Fed member name and title
root_dir = 'data_gather/fed_speech_downloader'  #fill this in with your root directory
NAME_LIST = pd.read_csv(os.path.join(root_dir, 'data/Fed/name_list.csv')).name.to_list()
DATE_FORMAT = '%Y%m%d'
DATA_DIR = os.path.join(root_dir, 'data/Fed')
PERSIST_DIR = os.path.join(root_dir, 'data/Fed/chroma_db')


def download_press(date, base_folder):

    materials_dat = download_fed_materials(date)

    # for now its only press at 2024, we want to include all presses from 2017 to present day and future.
    press_dat = materials_dat[(materials_dat['TYPE'] == 'press') & (materials_dat['Dates'] >= '20240101')]

    for i in range(press_dat.shape[0]):
        url = press_dat.iloc[i]['URL']
        typ = press_dat.iloc[i]['TYPE']
        dte = press_dat.iloc[i]['Dates']
        if not os.path.exists(os.path.join(base_folder, 'raw_data')):
            os.makedirs(os.path.join(base_folder, 'raw_data'))
        if typ not in os.listdir(os.path.join(base_folder, 'raw_data')):
            os.mkdir(os.path.join(base_folder, 'raw_data', typ))
        if f'{typ}_{dte}.pdf' not in os.listdir(os.path.join(base_folder, 'raw_data', typ)):
            response = requests.get(url)
            with open(f'{base_folder}/raw_data/{typ}/{typ}_{dte}.pdf', 'wb') as f:
                f.write(response.content)

    # only save metadatas
    metadatas = []

    for i in range(press_dat.shape[0]):
        url = press_dat.iloc[i]['URL']
        typ = press_dat.iloc[i]['TYPE']
        dte = press_dat.iloc[i]['Dates']
        filename = f'{base_folder}/raw_data/{typ}/{typ}_{dte}.pdf'
        metadata = {}
        metadata['date'] = dte
        metadata['timestamp'] = int(datetime.timestamp(datetime.strptime(metadata['date'], "%Y%m%d")))
        metadata['source'] = url
        pdf_reader = PyPDF2.PdfReader(filename)
        doc_info = pdf_reader.metadata
        title = doc_info.get('/Title', 'No Title Found')
        metadata['speech_title'] = title
        metadata['speaker'] = "Jerome H. Powell"
        metadata['speaker_type'] = 'president' # Change to govenor to be consistent?
        metadatas.append(metadata)
    return metadatas


def download_fed_materials(date):
    # Create a df of links to statement, SEP, minutes, and press transcript
    # 2019-2024

    print(date)
    source_url = requests.get("https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm")
    soup = BeautifulSoup(source_url.content,"html.parser")

    links = []
    types = []
    dates = []
    for link in soup.find_all('a',href=True):
        if link.get('href').endswith(".pdf"):
            url = "https://www.federalreserve.gov/" + link.get('href')
            filename = link.get('href').split('.pdf')[0].split('files/')[-1]
            idx = re.search(r"\d", filename).start()
            links.append(url)
            types.append(filename[:idx])
            dates.append(filename[idx: (idx + 8)])

    dl_dat = pd.DataFrame(np.array([links, types, dates]).T, columns=['URL', 'TYPE', 'Dates'])
    meeting_dates = dl_dat[dl_dat['TYPE'] == 'monetary']['Dates'].unique()

    # get the links (press conference)
    links = []
    for dte in meeting_dates:
        links.append(f"https://www.federalreserve.gov/mediacenter/files/FOMCpresconf{dte}.pdf")


    press_dat = pd.DataFrame(np.array([links, meeting_dates]).T, columns=['URL', 'Dates'])
    press_dat['TYPE'] = 'press'
    dl_dat = pd.concat([dl_dat, press_dat], axis=0)
    del press_dat
    return dl_dat


def fed_ny_update(df, new_urls, metadatas):
    # Extract url and metadata of the speech
    website_ny = "https://www.newyorkfed.org/press#speeches"
    reqs = requests.get(website_ny)
    soup = BeautifulSoup(reqs.content, 'html.parser')
    for link in soup.find_all('a'):
        url = link.get('href')
        if url is not None and "/newsevents/speeches/2024/wil" in url:
            # df and new_urls
            date = url[-6:]
            date = datetime.strptime(date, '%y%m%d')
            url = f'https://www.newyorkfed.org{url}'
            new_urls.append(url)
            speaker = 'John C. Williams'
            df.loc[date, speaker] = 'yes'

            # metadata
            metadata = {}
            metadata['date'] = datetime.strftime(date, DATE_FORMAT)
            metadata['timestamp'] = int(date.timestamp())
            metadata['source'] = url
            title = link.text.split(':')[-1]
            metadata['speech_title'] = title
            metadata['speaker'] = speaker
            metadata['speaker_type'] = 'regional'
            metadatas.append(metadata)

    return df, new_urls, metadatas


def fed_richmond_update(df, new_urls, metadatas):
    # Extract url and metadata of the speech
    # Richmond Fed: website can be very slow somtimes
    website_ric = "https://www.richmondfed.org/press_room/speeches"
    reqs = requests.get(website_ric)
    soup = BeautifulSoup(reqs.content, 'html.parser')
    for link in soup.find_all('a'):
        url = link.get('href')
        try:
            if "/press_room/speeches/thomas_i_barkin/2024/" in url:
                # df and new_urls
                date = url[-8:]
                date = datetime.strptime(date, '%Y%m%d')
                url = f'https://www.richmondfed.org{url}'
                speaker = 'Thomas I. Barkin'
                new_urls.append(url)
                df.loc[date, speaker] = 'yes'

                # metadata
                metadata = {}
                metadata['date'] = datetime.strftime(date, DATE_FORMAT)
                metadata['timestamp'] = int(date.timestamp())
                metadata['source'] = url
                reqs = requests.get(url)
                soup = BeautifulSoup(reqs.content, 'html.parser')
                metadata['speech_title'] = link.text
                metadata['speaker'] = speaker
                metadata['speaker_type'] = 'regional'
                metadatas.append(metadata)
        except:
            continue
    return df, new_urls, metadatas


def fed_sf_update(df, new_urls, metadatas):
    # Extract url and metadata of the speech
    website_sf = "https://www.frbsf.org/news-and-media/speeches/mary-c-daly/"
    reqs = requests.get(website_sf)
    soup = BeautifulSoup(reqs.content, 'html.parser')
    for li_tag in soup.find_all('li'):
        a_tag = li_tag.find('a', href=lambda href: href and '2024' in href)
        if a_tag:
            # df and new_urls
            url = a_tag.get('href')
            new_urls.append(url)
            speaker = 'Mary C. Daly'
            div_tag = li_tag.find('div', class_='wp-block-post-date')
            time_tag = div_tag.find('time')
            datetime_value = time_tag.get('datetime')
            date = datetime_value.split('T')[0]
            date = datetime.strptime(date, '%Y-%m-%d')
            df.loc[date, speaker] = 'yes'

            # metadata
            metadata = {}
            metadata['source'] = url
            reqs = requests.get(url)
            soup = BeautifulSoup(reqs.content, 'html.parser')
            metadata['date'] = datetime.strftime(date, DATE_FORMAT)
            metadata['timestamp'] = int(date.timestamp())
            title = a_tag.text
            metadata['speech_title'] = title
            metadata['speaker'] = speaker
            metadata['speaker_type'] = 'regional'
            metadatas.append(metadata)
    return df, new_urls, metadatas

def fed_cleveland_update(df, new_urls, metadatas):
    # Extract url and metadata of the speech
    website_cle = "https://www.clevelandfed.org/collections/speeches/"
    reqs = requests.get(website_cle)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    string  = soup.find_all(class_="site-search-results")[0].__str__()

    json_str = string.split("data-initialize=\'")[1].split("\' data-properties")[0] # Extract JSON string
    json_str = unescape(json_str)  # Convert HTML entities
    json_obj = json.loads(json_str) # Convert to JSON object
    # print(json.dumps(json_obj, indent=4))

    for link in json_obj['results']:
        if '2024' in link['url']:
            # df and new_urls
            url = f"https://www.clevelandfed.org{link['url']}"
            new_urls.append(url)
            date = url[url.index('2024'): (url.index('2024') + 8)]
            date = datetime.strptime(date, '%Y%m%d')
            speaker = 'Loretta J. Mester'
            df.loc[date, speaker] = 'yes'

            # metadata
            metadata = {}
            metadata['source'] = url
            metadata['date'] = datetime.strftime(date, DATE_FORMAT)
            metadata['timestamp'] = int(date.timestamp())
            reqs = requests.get(url)
            soup = BeautifulSoup(reqs.content, 'html.parser')
            title = soup.find(lambda tag: tag.name and "title" in tag.name).text.split(' | ')[0]
            metadata['speech_title'] = title
            metadata['speaker'] = speaker
            metadata['speaker_type'] = 'regional'
            metadatas.append(metadata)
    return df, new_urls, metadatas


def fed_atlanta_update(df, new_urls, metadatas):
    # Extract url and metadata of the speech
    website_atl = "https://www.atlantafed.org/news/speeches"
    reqs = requests.get(website_atl)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    json_str = soup.find_all('script')[8].text.split("var router054aab78a30940108fd146cd483263504b0b3595611a45398f7ddcda7af1c23c = ")[1].split(';\r\n')[0][1:-1]
    json_str = unescape(json_str)  # Convert HTML entities
    json_obj = json.loads(json_str) # Convert to JSON object
    for item in json_obj['items']:
        # df and new_urls
        url = item['Url']
        new_urls.append(url)
        speaker = 'Raphael W. Bostic'
        date = ''.join(url.split(website_atl)[1][1:11].split('/'))
        date = datetime.strptime(date, '%Y%m%d')
        df.loc[date, speaker] = 'yes'

        # metadata
        metadata = {}
        metadata['source'] = url
        metadata['date'] = datetime.strftime(date, DATE_FORMAT)
        metadata['timestamp'] = int(date.timestamp())
        reqs = requests.get(url)
        soup = BeautifulSoup(reqs.content, 'html.parser')
        title = soup.find(lambda tag: tag.name and "title" in tag.name).text.split(' - ')[0]
        metadata['speech_title'] = title
        metadata['speaker'] = speaker
        metadata['speaker_type'] = 'regional'
        metadatas.append(metadata)
    return df, new_urls, metadatas


def fed_dallas_update(df, new_urls, metadatas):
    # Extract url and metadata of the speech
    website_dallas = 'https://www.dallasfed.org/news/speeches/logan#yr2024'
    reqs = requests.get(website_dallas)
    soup = BeautifulSoup(reqs.content, 'html.parser')
    for link in soup.find_all('a'):
        url = link.get('href')
        if "/news/speeches/logan/2024/" in url:
            # df and new_urls
            url = f'https://www.dallasfed.org{url}'
            new_urls.append(url)
            date = '20'+url[-6:] # e.g. 20240301
            date = datetime.strptime(date, "%Y%m%d")
            speaker = 'Lorie K. Logan'
            df.loc[date, speaker] = 'yes'

            # metadata
            metadata = {}
            metadata['source'] = url
            metadata['date'] = datetime.strftime(date, DATE_FORMAT)
            metadata['timestamp'] = int(date.timestamp())
            reqs = requests.get(url)
            soup = BeautifulSoup(reqs.content, 'html.parser')
            title = soup.find(lambda tag: tag.name and "title" in tag.name).text.split("-")[0].strip()
            metadata['speech_title'] = title
            metadata['speaker'] = 'Lorie K. Logan'
            metadata['speaker_type'] = 'regional'
            metadatas.append(metadata)
    return df, new_urls, metadatas


def fed_reserve_update(df, new_urls, metadatas):
    # Extract url and metadata of the speech
    website = 'https://www.federalreserve.gov/newsevents/speech/2024-speeches.htm'
    reqs = requests.get(website)
    soup = BeautifulSoup(reqs.content, 'html.parser')

    for link in soup.find_all('a'):
        url = link.get('href')
        if "/newsevents/speech/" in url:
            # df and new_urls
            url = f'https://www.federalreserve.gov{url}'
            new_urls.append(url)
            date = url.split('speech/')[-1].split('.htm')[0][-9:-1]
            date = datetime.strptime(date, '%Y%m%d') # datetime object
            speaker = url.split('speech/')[-1].split('.htm')[0][:-9]
            for name in NAME_LIST:
                if name.lower().split(' ')[-1] == speaker:
                    speaker = name
                    break
            df.loc[date, speaker] = 'yes'

            # metadatas
            metadata = {}
            metadata['speaker'] = speaker
            metadata['speaker_type'] = 'governor'

            # 修复此处的问题
            title_element = link.find('em')  # 获取em标签
            if title_element:  # 如果找到了em标签
                metadata['speech_title'] = title_element.text
            else:
                metadata['speech_title'] = "No Title Found"  # 如果没有em标签，默认值

            metadata['date'] = datetime.strftime(date, DATE_FORMAT)
            metadata['timestamp'] = int(date.timestamp())
            metadata['source'] = url
            metadatas.append(metadata)

    return df, new_urls, metadatas



def press_update(df, new_urls, metadatas, base_folder=DATA_DIR):

    metadata_press = download_press(datetime.today(), base_folder)
    for m in metadata_press:
        new_urls.append(m['source'])
        metadatas.append(m)
        df.loc[m['date'], m['speaker']] = 'yes'

    return df, new_urls, metadatas

def update_docs():
    # Update collection to reflect the most recent speeches
    # Compare to the existing urls saved in urls.csv

    # Find new speeches to add
    df = pd.DataFrame(columns=NAME_LIST)
    new_urls = []
    metadatas = []

    df, new_urls, metadatas = fed_reserve_update(df, new_urls, metadatas)
    df, new_urls, metadatas = fed_ny_update(df, new_urls, metadatas)
    df, new_urls, metadatas = fed_richmond_update(df, new_urls, metadatas)
    df, new_urls, metadatas = fed_sf_update(df, new_urls, metadatas)
    df, new_urls, metadatas = fed_cleveland_update(df, new_urls, metadatas)
    df, new_urls, metadatas = fed_atlanta_update(df, new_urls, metadatas)
    df, new_urls, metadatas = fed_dallas_update(df, new_urls, metadatas)
    df, new_urls, metadatas = press_update(df, new_urls, metadatas)

    # Update index.csv file
    df = df.sort_index(ascending=True) # From earlist to latest
    df.to_csv(os.path.join(DATA_DIR, 'index.csv'))

    # Update urls.csv file
    if "urls.csv" in os.listdir(DATA_DIR):
        old_urls = pd.read_csv(os.path.join(DATA_DIR, 'urls.csv'))
        added_urls = list(set(new_urls) - set(old_urls.url))
        all_urls = set(new_urls).union(set(old_urls.url))
        pd.DataFrame(all_urls, columns=['url']).to_csv(os.path.join(DATA_DIR, 'urls.csv'))
    else:
        added_urls = new_urls
        pd.DataFrame(new_urls, columns=['url']).to_csv(os.path.join(DATA_DIR, 'urls.csv'))

    print(f"There are {len(added_urls)} new speeches detected.")

    # Load contents of added speeches
    added_docs = []
    for metadata in metadatas:
        if metadata['source'] in added_urls:
            url = metadata['source']
            speaker_type = metadata['speaker_type']
            if speaker_type != 'president':
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')

                # Parse speeches on differet Fed websites
                if 'federalreserve' in url:
                    article_div = soup.find('div', id='article')
                elif 'dallas' in url:
                    article_div = soup.find('div', class_='dal-main-content')
                elif 'atlanta' in url:
                    article_div = soup.find('div', class_='main-content')
                elif 'cleveland' in url: # long paras
                    article_div = soup.find('div', class_='component rich-text')
                elif 'frbsf' in url: # short paras
                    article_div = soup.find('div', class_='entry-content wp-block-post-content is-layout-flow wp-block-post-content-is-layout-flow')
                elif 'richmond' in url: # medium para
                    article_div = soup.find('div', class_='tmplt__content')
                elif 'newyork' in url: # medium para
                    article_div = soup.find('div', class_='ts-article-text')
                paragraphs = article_div.find_all('p')

                for i, para in enumerate(paragraphs): # govenor speeches
                    if not para.has_attr('class'):
                        # Only include para with length >= 100 (~one line length),
                        # exclude para with 'Return to text', which is a sign of footnote
                        if (len(para.text) >= 120) & ('Return to text' not in para.text):
                            metadata['para_num'] = i+1 # Add para_num as a metadata
                            # dict is mutable, so use .copy()
                            doc = Document(page_content=para.text, metadata=metadata.copy())
                            added_docs.append(doc)

            else:
                dte = metadata['date']
                filename = os.path.join(root_dir, f'data/Fed/raw_data/press/press_{dte}.pdf')
                loader = PyPDFLoader(filename)
                pages = loader.load_and_split()

                # process the paragraphs
                paras = []
                for p in pages:
                    tmp = p.page_content.split('  \n')
                    for t in tmp:
                        if "Chair Powell ’s Press Conference   FINAL" not in t and " \nPage" not in t:
                            paras.append(t)
                new_paras = []
                i = 0
                while i <= len(paras)-2:
                    p = paras[i+1]
                    if p.startswith(" ") and i != 0:
                        new_paras.append(''.join([paras[i], paras[i + 1]]))
                        i += 2
                    else:
                        new_paras.append(paras[i])
                        i += 1

                for i, p in enumerate(new_paras):
                    metadata['paragraph'] = i # Add 'paragraph' number in metadata
                    doc = Document(page_content=p, metadata=metadata)
                    added_docs.append(doc)

    return df, added_urls, added_docs

# 定义main函数
def main():
    # 设置根目录和数据存储路径
    root_dir = 'data_gather/fed_speech_downloader'
    base_folder = os.path.join(root_dir, 'data/Fed')

    # 打印开始时间
    print(f"Starting update process at {datetime.now()}...")

    # 检查数据目录是否存在，如果不存在则创建
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    # 检查是否存在 urls.csv 和 index.csv 文件，如果不存在则创建空的
    urls_path = os.path.join(base_folder, 'urls.csv')
    index_path = os.path.join(base_folder, 'index.csv')

    if not os.path.exists(urls_path):
        with open(urls_path, 'w') as f:
            f.write("url\n")

    if not os.path.exists(index_path):
        df_empty = pd.DataFrame(columns=NAME_LIST)
        df_empty.to_csv(index_path, index=True)

    # 调用 update_docs 函数，更新演讲数据
    df, added_urls, added_docs = update_docs()

    # 打印新增加的演讲数量
    print(f"New speeches added: {len(added_urls)}")

    # 保存最新的index文件
    df.to_csv(index_path)

    # 保存更新后的urls
    pd.DataFrame(added_urls, columns=['url']).to_csv(urls_path, index=False)

    # 打印结束时间
    print(f"Update process completed at {datetime.now()}.")

# 调用main函数
if __name__ == "__main__":
    main()