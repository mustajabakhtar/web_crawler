import requests
from bs4 import BeautifulSoup
import MySQLdb

login_info = open('../crediantials/mysql_db_crediantials')
url = "https://www.flipkey.com/goa-vacation-rentals/g297604/?check-in=02/01/2017&check-out=02/28/2017&page="
login_info = login_info.read()
login_info = login_info.split('\n')
converted_into_list = []
conn = MySQLdb.connect(host=login_info[0], user=login_info[1], passwd=login_info[2], db=login_info[3])
def web_crawler(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    general_data = soup.find_all("div", attrs={"class": "property-photo-summary-wrapper"})
    for item in general_data:
        try:
           property_name_title = item.contents[0].find_all("a", attrs = {"data-link-name" : "title"})
           property_name = item.contents[0].find_all("div", attrs = {"class" : "location-name"})
           room_data = item.contents[0].find_all("div", attrs = {"class" : "description"})
           review_count = item.contents[0].find_all("span", attrs={"class": "number"})
           rating_label = item.contents[0].find_all("span", attrs = {"class":"rating-label"})
           price = item.contents[0].find_all("span", attrs = {"class" : "val"})

           if review_count:
               review_count = review_count[0].text.encode('utf8')
           else:
               review_count = None
           if property_name_title:
               property_name_title = property_name_title[0].text.encode('utf8')
           else:
               property_name_title = None
           if property_name:
               property_name = property_name[0].text.encode('utf8')
           else:
               property_name = None
           if rating_label:
               rating_label = rating_label[0].text.encode('utf8')
           else:
               rating_label = None
           if price:
               price = price[0].text
               price = price.replace("Rs","")
               price = "".join(price.split()).encode('utf8')
           else:
               price = None

           bed_rooms, bath_attached, guest_can_sleep, source, city_name, region_or_locality, listing_type = None, None, None, "flipkey", "Goa", "Goa", None
           if room_data:
               room_data = room_data[0].text
               room_data_list = room_data.split('/')
               for rooms in room_data_list:
                   if "BR"  in rooms:
                       bed_rooms = rooms.encode('utf8')
                   if "BA" in rooms:
                       bath_attached = rooms.encode('utf8')
                   if "Sleeps" in rooms:
                       guest_can_sleep = rooms
                       guest_can_sleep = guest_can_sleep.replace("Viewed","").encode('utf8')
           else:
               room_data_list = None

           #print price +" | "+str(property_name_title) +" | "+ str(review_count) +" | "+ str(property_name) +" | "+str(bed_rooms)  + " | "+ str(bath_attached)+ " | "+str(guest_can_sleep)+ " | "+ str(rating_label)
           converted_into_list.append((property_name_title, review_count, property_name, bed_rooms, bath_attached, guest_can_sleep, price, rating_label, source, city_name, region_or_locality, listing_type))
        except Exception as ex:
           print ex

def insert_into_db():
    try:
        print "DB INSET!!!!!!!!!!!!!!!!!"
        cursor = conn.cursor()
        query = "INSERT INTO flipkey_crawled_data (property_name_title, review_count, property_name, rooms, bath_attached, guests, price, rating_type, source, city_name, region_or_locality, listing_type) VALUES"
        query = query + " ,".join("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" for _ in converted_into_list)
        flattened_values = [item for sublist in converted_into_list for item in sublist]
        cursor.execute(query, flattened_values)
        conn.commit()
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()

count = 1
while (count < 19):
    web_crawler(url + str(count))
    count = count + 1

insert_into_db()
