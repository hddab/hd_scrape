from types import NoneType
import requests
from bs4 import BeautifulSoup
import re
import json
from pathlib import Path


class select_sources:
    def __init__(self):
        self.base_url = "https://www.homedepot.com"
        self.header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'} 
        self.session = requests.Session()
        self.session.headers.update(self.header)

    def get_stores_sitemap(self):

        storemap_url = self.base_url + "/sitemap/HomeServices/store.xml"
        storemap = self.session.get(storemap_url)
        storemap_xml = BeautifulSoup(storemap.content,"html.parser")  

        stores_dict = {}

        for store in storemap_xml.find_all('loc'):
            store_url = store.text
            store_data = store_url.split("/")
            if len(store_data) == 9:
                stores_dict[str(store_data[4])] = {
                                            'State':str(store_data[5]), 
                                            'City':str(store_data[6]), 
                                            'Zip':str(store_data[7]), 
                                            'store_id':int(store_data[8])
                                            }

        return stores_dict

    def get_categories(self):

        cats_url = self.base_url + "/c/site_map"
        cats = self.session.get(cats_url)
        catmap_soup = BeautifulSoup(cats.content,"html.parser").find("div", {'class':'grid','name':'etch-widget'})

        cats_dict = {}
        main_cat = None

        for category in catmap_soup.find_all('a', {'href':re.compile("/b/")}):

            if category['href'] != None and 'homedepot.com' in category['href']:
                category_url = category['href']
            else:
                category_url = self.base_url + category['href']

            category_data = category_url.split("/")

            if category.find('strong') != None:
                main_cat = str(category.text)
                cats_dict[main_cat]=({str(category.text):str(category_url)})
            elif len(category_data) == 6:
                cats_dict[main_cat].update({str(category.text):str(category_url)})

        return cats_dict
    
    def get_brands(self, cat, subcat, subcat_url):

        subcat_navParam = subcat_url.split("/")[5] 
        brands_dict = {}

        for site_number in range(0,10):
            url_pattern = (self.base_url + "/sitemap/d/plp/" + str(cat) + "-" + str(site_number) + ".xml" )
            map_raw = self.session.get(url_pattern)
            if map_raw.status_code in (403,404):
                break
            elif map_raw.status_code == 200:
                catmap_soup = BeautifulSoup(map_raw.content,"html.parser").find_all('loc',text = re.compile(subcat_navParam))
            
            for sub_subcat in catmap_soup:
                sub_subcat_params = sub_subcat.text.split("/")
                if (len(sub_subcat_params)) == 7:
                    brand = sub_subcat_params[5]
                    if brand == '5':
                        brand = 'BestRated'

                    sub_subcat_navParam = sub_subcat_params[6]

                    brands_dict[brand] = sub_subcat_navParam
        return brands_dict

#testing
if __name__ == '__main__':

    sources = select_sources()
    stores = sources.get_stores_sitemap()

    store = 'Manhattan-59th-Street'
    store_id = stores[store]['store_id']
    print(store_id)

    cats = sources.get_categories()
    
    select_category = 'Appliances'
    select_subcategory = 'Dishwashers'

    selected_subcat_url = cats[select_category][select_subcategory]
    brands = sources.get_brands(select_category,select_subcategory, selected_subcat_url)
    
    select_brand = 'Samsung'

    navParam = brands[select_brand]
    print(navParam)