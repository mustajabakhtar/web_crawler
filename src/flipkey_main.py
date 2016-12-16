import requests
from bs4 import BeautifulSoup
import MySQLdb
import os
import logging

login_info = open('../crediantials/mysql_db_crediantials')
config_info = open('../config/configuration')

config_info = config_info.read()
login_info = login_info.read()

config_info = config_info.split('\n')
login_info = login_info.split('\n')

converted_into_list = []
gallary_src_url_list = []
property_type_list = ["apartment", "house", "villa", "resort", "studio", "cottage", "hut",
                      "bungalow", "camp", "chalet", "condo", "houseboat", "penthouse", "room", "townhouse", "yacht", "fort"]

location_list = ['Goa', 'Wayanad', 'Bangalore', 'Delhi', 'Manali', 'Shimla', 'Alappuzha', 'Munnar', 'Lucknow',
                 'Gokarna', 'Darjeeling', 'Srinagar', 'Jaipur', 'Jodhpur', 'Kullu', 'Hyderabad', 'Lonavala', 'Nainital',
                 'Idukki', 'Mumbai', 'Agra', 'Leh', 'Alibagh']

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=os.path.join(BASE_DIR, 'logs/flipkey-crawler-logs.log'),
                    filemode='w')


def web_crawler(soup, index):
    logging.info("@@@@@@@@@@@@@@@ Crawling started for index :"+ str(index))
    #r = requests.get(url)
    #soup = BeautifulSoup(r.content, 'html.parser')
    general_data = soup.find_all("div", attrs={"class": "property-photo-summary-wrapper"})
    for item in general_data:
        try:
            property_id_list = item.find_all("div", attrs={"class": "property-photo-summary"})
            property_name_title = item.contents[0].find_all("a", attrs={"data-link-name": "title"})
            property_name = item.contents[0].find_all("div", attrs={"class": "location-name"})
            room_data = item.contents[0].find_all("div", attrs={"class": "description"})
            review_count = item.contents[0].find_all("span", attrs={"class": "number"})
            rating_label = item.contents[0].find_all("span", attrs={"class": "rating-label"})
            price = item.contents[0].find_all("span", attrs={"class": "val"})
            photo_url_list = item.find_all("span", attrs={"class": "photo-sprite"})

            for property_id in property_id_list:
                property_id = property_id.get('data-property-id')

            for photo in photo_url_list:
                gallary_src_url_list.append((property_id, photo.get('data-src')))
            if review_count:
                review_count = review_count[0].text
            else:
                review_count = None
            if property_name_title:
                property_name_title = property_name_title[0].text
            else:
                property_name_title = None
            if property_name:
                property_name = property_name[0].text
                # property_type = property_name.rsplit(None, 1)[-1].capitalize()
                for type in property_type_list:
                    if type in property_name:
                        property_type = type.capitalize()
                    elif "bed" in property_name:
                        property_type = "Bed and Breakfast"

            else:
                property_name = None
                property_type = None

            if rating_label:
                rating_label = rating_label[0].text
            else:
                rating_label = None
            if price:
                price = price[0].text
                price = price.replace("Rs", "")
                price = "".join(price.split())
            else:
                price = None

            bed_rooms, bath_attached, guest_can_sleep, source, city_name, region_or_locality, listing_type = None, None, None, "flipkey", location_list[index], location_list[index], None
            if room_data:
                room_data = room_data[0].text
                room_data_list = room_data.split('/')
                for rooms in room_data_list:
                    if "BR" in rooms:
                        bed_rooms = rooms.replace("BR", "")
                    if "BA" in rooms:
                        bath_attached = rooms.replace("BA", "")
                    if "Sleeps" in rooms:
                        guest_can_sleep = rooms
                        guest_can_sleep = guest_can_sleep.replace("Viewed", "").replace("Sleeps", "")

            # print price +" | "+str(property_name_title) +" | "+ str(review_count) +" | "+ str(property_name) +" | "+str(bed_rooms)  + " | "+ str(bath_attached)+ " | "+str(guest_can_sleep)+ " | "+ str(rating_label)
            converted_into_list.append((property_name_title, review_count, property_name, bed_rooms, bath_attached,
                                        guest_can_sleep, price, rating_label, source, city_name, region_or_locality,
                                        listing_type, property_type, property_id))
        except Exception as ex:
            logging.info("Error has occurred while crawling:" + str(ex))

    logging.info("@@@@@@@@@@@@@@@ Crawling ended for index :"+ str(index))
    logging.info(converted_into_list)

def insert_into_db():
    try:
        logging.info("Started to insert data into flipkey_crawled_data table")
        conn = MySQLdb.connect(host=login_info[0], user=login_info[1], passwd=login_info[2], db=login_info[3])
        cursor = conn.cursor()
        query = "INSERT INTO flipkey_crawled_data (property_name_title, review_count, property_name, rooms, bath_attached, guests, price, rating_type, source, city_name, region_or_locality, listing_type, property_type, property_id) VALUES"
        query = query + " ,".join(
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" for _ in converted_into_list)
        flattened_values = [item for sublist in converted_into_list for item in sublist]
        cursor.execute(query, flattened_values)
        conn.commit()
        logging.info("Finished data insertion for table flipkey_crawled_data")
    except Exception as e:
        logging.info("Error occured while inserting data into flipkey_crawled_data :" + str(e))
    finally:
        cursor.close()
        conn.close()


def insert_images_into_db():
    try:
        logging.info("Started to insert data into flipkey_image_url_data table")
        conn = MySQLdb.connect(host=login_info[0], user=login_info[1], passwd=login_info[2], db=login_info[3])
        cursor = conn.cursor()
        query = "INSERT INTO flipkey_image_url_data(property_id, image_url) VALUES"
        query = query + " ,".join("(%s, %s)" for _ in gallary_src_url_list)
        flattened_values = [item for sublist in gallary_src_url_list for item in sublist]
        cursor.execute(query, flattened_values)
        conn.commit()
        logging.info("Finished data insertion for table flipkey_image_url_data")
    except Exception as e:
        logging.info("Error occured while inserting data into flipkey_image_url_data :" + str(e))
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    counter = 0
    while counter < 23:
        try:
            logging.info(config_info[counter] +"1")
            r = requests.get(config_info[counter] + str(counter))
            soup = BeautifulSoup(r.content, 'html.parser')
            num_of_pages = soup.find("div", attrs={"id": "search-pages"}).text

            page_count = num_of_pages.split(' ')[-1]
            count = 1
            while (count < int(page_count)+1):
               logging.info("================Crawling for page : "+config_info[counter] + str(count)+" ==========================")
               r = requests.get(config_info[counter] + str(count))
               soup = BeautifulSoup(r.content, 'html.parser')
               web_crawler(soup, counter)
               count = count + 1

        except Exception as ex:
               logging.info("Error has occured in main:"+str(ex))
               r = requests.get(config_info[counter] + str(1))
               soup = BeautifulSoup(r.content, 'html.parser')
               web_crawler(soup, counter)
        finally:
            counter = counter + 1

insert_into_db()
insert_images_into_db()

