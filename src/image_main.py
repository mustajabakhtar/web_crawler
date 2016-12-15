import requests
from bs4 import BeautifulSoup
import os
import urllib
import logging

config_info = open('../config/configuration')
config_info = config_info.read()
config_info = config_info.split('\n')
converted_into_list = []
gallary_src_url_list = []
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IMAGE_DIR = os.path.join(BASE_DIR,'images')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=os.path.join(BASE_DIR,'logs/image-crawler-logs.log'),
                    filemode='w')

def download_photo(path, img_url, filename):

    try:
        file_path = "%s%s" % (path, filename)
        if not os.path.exists(file_path):
            image_on_web = urllib.urlopen(img_url)
            if image_on_web.headers.maintype == 'image':
                buf = image_on_web.read()
                downloaded_image = file(file_path, "wb")
                downloaded_image.write(buf)
                downloaded_image.close()
                image_on_web.close()
            else:
                logging.info("Unable to write :"+file_path)
    except:
        logging.info("Error occurred :"+file_path)
    logging.info("Successfully wrote the data :"+file_path)

def web_crawler(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    general_data = soup.find_all("div", attrs={"class": "property-photo-summary-wrapper"})
    for item in general_data:
        try:
            property_id_list = item.find_all("div", attrs={"class": "property-photo-summary"})
            photo_url_list = item.find_all("span", attrs={"class": "photo-sprite"})

            for property_id in property_id_list:
                property_id = property_id.get('data-property-id')

            for photo in photo_url_list:
                gallary_src_url_list.append((property_id ,photo.get('data-src')))
                if not os.path.exists(os.path.join(IMAGE_DIR, property_id)):
                    os.makedirs(os.path.join(IMAGE_DIR, property_id))
                    download_photo(os.path.join(IMAGE_DIR, property_id)+'/', photo.get('data-src'), photo.get('data-src').split('/')[-1])
                else:
                    download_photo(os.path.join(IMAGE_DIR, property_id)+'/', photo.get('data-src'), photo.get('data-src').split('/')[-1])
        except Exception as ex:
            print ex



if __name__ == '__main__':
    count = 1
    while (count < 28):
        web_crawler(config_info[0] + str(count))
        count = count + 1

