import requests
from bs4 import BeautifulSoup


url = 'https://www.yandex.ru/search/ads?text='
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0'}


def parse_yandex_moscow(search_query):

    yandex_ad_list = []

    try:
        search_query = search_query.replace(' ', '+')
        req = requests.get(url + search_query + '&lr=1', headers=headers, stream=True)
        soup = BeautifulSoup(req.content.decode('utf-8'), 'html.parser')

        for ad in soup.find_all('li', attrs={'class': 'serp-item'})[:3]:
            head = ad.find('div', attrs={'class': 'OrganicTitle-LinkText'}).text  # заголовок
            ad_text = ad.find('div', attrs={'class': 'Typo_text_m'}).text  # тексты
            domain = ad.find('div', attrs={'class': 'Organic-Path'}).find('b').text  # видимый домен

            yandex_ad_dict = dict()

            yandex_ad_dict['head'] = head
            yandex_ad_dict['ad_text'] = ad_text
            yandex_ad_dict['domain'] = domain

            yandex_ad_list.append(yandex_ad_dict)

        return yandex_ad_list

    except Exception as e:
        print(e)
        return 'yandex_parase_error'
