import sys
#import httplib2
import urllib3
import psycopg2
import psycopg2.extras
import lxml.etree
import time
import datetime
import threading
#import psycopg2_bulk_insert
#import psycopg2_etl_utils
#import nike_etl
import uuid
import decimal
import pickle
import ConfigParser
import re
import json
import random
import os
import codecs
import smtplib

http="https://"
hostname="www.bezrealitky.cz"
pripona = "/vyhledat"
pripona2 = "/vypis"
path = "/home/pi/Documents/bezrealitky_proj/rent/"
httpcon = urllib3.PoolManager()
price_reg = re.compile('([0-9]{1,3}.[0-9]{3})+')
pagination_reg = re.compile('([0-9]{1,3})+')
surface_room_reg = re.compile('([[0-9]{1}\+[0-9]{1}|[[0-9]{1}\+kk)+')
surface_flat_reg = re.compile('([0-9]{2,3})+')
pager_bezrealitky="&page={}"
pager_sreality="&strana={}"
price_big = 23000
price_small = 12000
flat_size = ['big','small']
hour = [6,8,11,14,17,20]
minute = [51,52,53,54,55,56,57,58,59]

fromaddr = 'bot@email.com'   #email you use to send the best offers - I will call it BOT email
toaddr  = 'your@email.com'  # email where you would like to receive best offers

username = "YourUsername"  # username to your BOT email
password = "YourPassword"   #password tou your BOT email

location_paths_big={
    "vinohrady":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-vinohrady&disposition%5B%5D=2-kk&disposition%5B%5D=2-1&disposition%5B%5D=3-kk&disposition%5B%5D=3-1&disposition%5B%5D=4-kk&disposition%5B%5D=4-1&disposition%5B%5D=5-kk&disposition%5B%5D=5-1&disposition%5B%5D=6-kk&disposition%5B%5D=6-1&disposition%5B%5D=7-kk&disposition%5B%5D=7-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "stare_mesto":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-stare-mesto&disposition%5B%5D=2-kk&disposition%5B%5D=2-1&disposition%5B%5D=3-kk&disposition%5B%5D=3-1&disposition%5B%5D=4-kk&disposition%5B%5D=4-1&disposition%5B%5D=5-kk&disposition%5B%5D=5-1&disposition%5B%5D=6-kk&disposition%5B%5D=6-1&disposition%5B%5D=7-kk&disposition%5B%5D=7-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "nove_mesto":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-nove-mesto&disposition%5B%5D=2-kk&disposition%5B%5D=2-1&disposition%5B%5D=3-kk&disposition%5B%5D=3-1&disposition%5B%5D=4-kk&disposition%5B%5D=4-1&disposition%5B%5D=5-kk&disposition%5B%5D=5-1&disposition%5B%5D=6-kk&disposition%5B%5D=6-1&disposition%5B%5D=7-kk&disposition%5B%5D=7-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "karlin":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-karlin&disposition%5B%5D=2-kk&disposition%5B%5D=2-1&disposition%5B%5D=3-kk&disposition%5B%5D=3-1&disposition%5B%5D=4-kk&disposition%5B%5D=4-1&disposition%5B%5D=5-kk&disposition%5B%5D=5-1&disposition%5B%5D=6-kk&disposition%5B%5D=6-1&disposition%5B%5D=7-kk&disposition%5B%5D=7-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "holesovice":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-holesovice&disposition%5B%5D=2-kk&disposition%5B%5D=2-1&disposition%5B%5D=3-kk&disposition%5B%5D=3-1&disposition%5B%5D=4-kk&disposition%5B%5D=4-1&disposition%5B%5D=5-kk&disposition%5B%5D=5-1&disposition%5B%5D=6-kk&disposition%5B%5D=6-1&disposition%5B%5D=7-kk&disposition%5B%5D=7-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "zizkov":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-zizkov&disposition%5B%5D=2-kk&disposition%5B%5D=2-1&disposition%5B%5D=3-kk&disposition%5B%5D=3-1&disposition%5B%5D=4-kk&disposition%5B%5D=4-1&disposition%5B%5D=5-kk&disposition%5B%5D=5-1&disposition%5B%5D=6-kk&disposition%5B%5D=6-1&disposition%5B%5D=7-kk&disposition%5B%5D=7-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "vysehrad":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-vysehrad&disposition%5B%5D=2-kk&disposition%5B%5D=2-1&disposition%5B%5D=3-kk&disposition%5B%5D=3-1&disposition%5B%5D=4-kk&disposition%5B%5D=4-1&disposition%5B%5D=5-kk&disposition%5B%5D=5-1&disposition%5B%5D=6-kk&disposition%5B%5D=6-1&disposition%5B%5D=7-kk&disposition%5B%5D=7-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "vrsovice":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-vrsovice&disposition%5B%5D=2-kk&disposition%5B%5D=2-1&disposition%5B%5D=3-kk&disposition%5B%5D=3-1&disposition%5B%5D=4-kk&disposition%5B%5D=4-1&disposition%5B%5D=5-kk&disposition%5B%5D=5-1&disposition%5B%5D=6-kk&disposition%5B%5D=6-1&disposition%5B%5D=7-kk&disposition%5B%5D=7-1&priceFrom=&priceTo=&order=time_order_desc&submit="}

location_paths_small={
    "vinohrady":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-vinohrady&disposition%5B%5D=garsoniera&disposition%5B%5D=1-kk&disposition%5B%5D=1-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "stare_mesto":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-stare-mesto&disposition%5B%5D=garsoniera&disposition%5B%5D=1-kk&disposition%5B%5D=1-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "nove_mesto":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-nove-mesto&disposition%5B%5D=garsoniera&disposition%5B%5D=1-kk&disposition%5B%5D=1-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "karlin":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-karlin&disposition%5B%5D=garsoniera&disposition%5B%5D=1-kk&disposition%5B%5D=1-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "holesovice":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-holesovice&disposition%5B%5D=garsoniera&disposition%5B%5D=1-kk&disposition%5B%5D=1-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "zizkov":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-zizkov&disposition%5B%5D=garsoniera&disposition%5B%5D=1-kk&disposition%5B%5D=1-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "vysehrad":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-vysehrad&disposition%5B%5D=garsoniera&disposition%5B%5D=1-kk&disposition%5B%5D=1-1&priceFrom=&priceTo=&order=time_order_desc&submit=",
    "vrsovice":"?advertoffertype=nabidka-pronajem&estatetype=byt&region=praha&county=praha-vrsovice&disposition%5B%5D=garsoniera&disposition%5B%5D=1-kk&disposition%5B%5D=1-1&priceFrom=&priceTo=&order=time_order_desc&submit="}

locations = ["vinohrady",
             "stare_mesto",
             "nove_mesto",
             "karlin",
             "holesovice",
             "zizkov",
             "vysehrad",
             "vrsovice"]

TEMPLATE_FILE = """Lokalita: {title}
Velikost: {surface}
Cena: {price}
Popis: {desc}
Odkaz na web: {web_adress}"""

TEMPLATE_EMAIL = """Subject: {title}

Lokalita: {title}
Velikost: {surface}
Cena: {price}
Popis: {desc}
Odkaz na web: {web_adress}"""

TEMPLATE_IAMALIVE = """I am still hardworking!"""

def get_url(httpcon, url):
    headers = {}
    headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    headers["Accept-Encoding"] = "gzip, deflate"
    headers["Accept-Language"] = "cs,en-us;q=0.7,en;q=0.3"
    headers["User-Agent"] = "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0"
    #headers["Cookie"]="op_cookie-test=ok;op_oddsportal=cv704mv1ek2l60ijve9iq9hn62;op_user_cookie=32830896;D_UID=739B93C4-5FB3-34AB-A4B5-0C2C36C3347E"
    res=httpcon.urlopen("GET", url, headers=headers)
    
    if res.status!=200:
        print res.status
        raise Exception("Unable to get_url "+url)
    return res.data

def create_file(path,advert_dict):
    for j in advert_dict:
        advert_output = advert_dict[j].split(';')
        with open(path+advert_output[-1]+"-"+advert_output[3]+".txt","w") as fi:
            out = TEMPLATE_FILE.format(title = advert_output[0],
                                  web_adress = advert_output[1],
                                  surface = advert_output[2],
                                  price = advert_output[3],
                                  desc = advert_output[4],
                                  id_advert = advert_output[5])
            fi.write(out)

def get_adverts_from_file(path):
    name_list = []
    for filename in os.listdir(path):
        if ".txt" in filename:
            name_list.append(filename.replace(".txt",""))
    return(name_list)

def send_email(username,password,fromaddr,toaddr,path,advert_dict):
    server = smtplib.SMTP('smtp.gmail.com:587') #if you use gmail
    server.starttls()
    server.login(username,password)
    for k in advert_dict:
        advert_output = advert_dict[k].split(';')
        msg = TEMPLATE_EMAIL.format(title = advert_output[0],
                              web_adress = advert_output[1],
                              surface = advert_output[2],
                              price = advert_output[3],
                              desc = advert_output[4],
                              id_advert = advert_output[5])
        server.sendmail(fromaddr, toaddr, msg)
    server.quit()

def get_flats_bezrealitky(httpcon,main_url,old_advert_list,price_threshold,quarter,flat_size):
    
    htmlparser=lxml.etree.HTMLParser()
    page=get_url(httpcon,main_url)
        
    p=lxml.etree.fromstring(page,htmlparser)
    pagination_xpath = p.xpath(u'body//div[@class="box-body"]/a/text()')
    pagination_list = [str(pagination_reg.findall(m)).strip("[]''""") for m in pagination_xpath]
    pagination_list = filter(None,pagination_list)
    advert_dict = {}
    id_advert_dict = {}
    count = 1
    if len(pagination_list) > 0:
        page_cnt = len(pagination_list)
    else:
        page_cnt = 1
        
    for page in range(page_cnt):
        flat_list=p.xpath(u'body//div[@class="record highlight"]')
        flat_list2=p.xpath(u'body//div[@class="record "]')
        flat_list = flat_list + flat_list2
        for r in flat_list:
            title_xpath = r.xpath('div[@class="details"]/h2/a/text()')
            web_adress_xpath = r.xpath('div[@class="details"]/p[@class="short-url"]/text()')
            surface_xpath = r.xpath('div[@class="details"]/p[@class="keys"]/text()')
            price_xpath = r.xpath('div[@class="details"]/p[@class="price"]/text()')
            desc_xpath = r.xpath('div[@class="details"]/p[@class="description"]/text()')

            title = title_xpath[1].replace(u'\xb2',"2").encode('cp1250').strip()
            web_adress = web_adress_xpath[0].encode('cp1250')
            id_advert = web_adress.split('/')[-1]
            surface_all = surface_xpath[0].replace(u'\xb2',"2").encode('cp1250')
            price = price_xpath[0].encode('cp1250')
            desc = desc_xpath[0].replace(u'\xb2',"2").replace(u'\u200b',"2").encode('cp1250','ignore').lstrip()
            price_int = price_reg.findall(price)
            price_int = [int(l.replace(".","")) for l in price_int]
            
            if sum(price_int) > price_threshold: #omezeni na vyssi celkoveho najmu
                continue        
            if id_advert+"-"+str(price) in old_advert_list:
                continue
            else:
                advert_list = title + ';' + web_adress + ';' + surface_all + ';' + str(price) + ';' + desc + ';' + id_advert
                advert_dict[count] = advert_list
                id_advert_dict[count] = id_advert
                count += 1
        url=main_url+pager_bezrealitky.format(page+2)
        page=get_url(httpcon,url)
        p=lxml.etree.fromstring(page,htmlparser)
                    
    send_email(username,password,fromaddr,toaddr,path,advert_dict)
    create_file(path+quarter+"/"+flat_size+'/',advert_dict)

       
def execute_script():
    time.sleep(random.uniform(300,640))

def i_am_alive(current_time,username,password, fromaddr,toaddr):
    if current_time.hour in hour and current_time.minute in minute:
        server = smtplib.SMTP('smtp.gmail.com:587') #if you use gmail
        server.starttls()
        server.login(username,password)
        msg = "Rent - I am still hardworking!"
        server.sendmail(fromaddr, toaddr, msg)
        server.quit()
    
while True:
    tstart=time.time()
    for i in locations:
        old_advert_list_big = get_adverts_from_file(path+i+"/"+flat_size[0]+"/")
        get_flats_bezrealitky(httpcon,http+hostname+pripona2+location_paths_big[i],old_advert_list_big,price_big,i,flat_size[0])
        old_advert_list_small = get_adverts_from_file(path+i+"/"+flat_size[1]+'/')
        get_flats_bezrealitky(httpcon,http+hostname+pripona2+location_paths_small[i],old_advert_list_small,price_small,i,flat_size[1])
    current_time = datetime.datetime.now()
    print current_time
    print "total time",time.time()-tstart
    print "bezrealitky_rent.py"
    i_am_alive(current_time,username,password, fromaddr,toaddr)
    execute_script()
