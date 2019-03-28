import scraperwiki
import sqlite3
import os
from   bs4 import BeautifulSoup
import sys
import time
import re
from datetime import datetime, timedelta
from re import sub
from decimal import Decimal
from dateutil.parser import parse
import math
import requests
import urllib.parse as urlparse
import random

import setEnvs


def parseAskingPrice(aPrice):
	try:
		value = round(Decimal(sub(r'[^\d.]', '', aPrice)))
	except:
		value = 0
	return value
	
def saveToStore(data):
	scraperwiki.sqlite.execute("CREATE TABLE IF NOT EXISTS 'zpdata' ( 'propId' TEXT, link TEXT, title TEXT, address TEXT, price BIGINT, 'displayPrice' TEXT, image1 TEXT, 'pubDate' DATETIME, 'addedOrReduced' DATE, reduced BOOLEAN, location TEXT,hashTagLocation TEXT, postContent TEXT, CHECK (reduced IN (0, 1)), PRIMARY KEY('propId'))")
	scraperwiki.sqlite.execute("CREATE UNIQUE INDEX IF NOT EXISTS 'zpdata_propId_unique' ON 'zpdata' ('propId')")
	scraperwiki.sqlite.execute("INSERT OR IGNORE INTO 'zpdata' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (data['propId'], data['link'], data['title'], data['address'], data['price'], data['displayPrice'], data['image1'], data['pubDate'], data['addedOrReduced'], data['reduced'], data['location'],data['hashTagLocation'],data['postContent']))

excludeAgents = []
if os.environ.get("MORPH_EXCLUDE_AGENTS") is not None:
	excludeAgentsString = os.environ["MORPH_EXCLUDE_AGENTS"]
	excludeAgents = excludeAgentsString.lower().split("^")

filtered_dict = {k:v for (k,v) in os.environ.items() if 'MORPH_URL' in k}
postTemplates = {k:v for (k,v) in os.environ.items() if 'ENTRYTEXT' in k}

sleepTime = 5
domain = ""

if os.environ.get("MORPH_DB_ADD_COL") is not None:
	if os.environ.get("MORPH_DB_ADD_COL") == '1':
		try:
			scraperwiki.sqlite.execute('ALTER TABLE zpdata ADD COLUMN hashTagLocation TEXT')
		except:
			print('col - hashTagLocation exists')
		try:
			scraperwiki.sqlite.execute('ALTER TABLE zpdata ADD COLUMN postContent TEXT')
		except:
			print('col - postContent exists')

if os.environ.get("MORPH_SLEEP") is not None:
	sleepTime = int(os.environ["MORPH_SLEEP"])

if os.environ.get("MORPH_DOMAIN") is not None:
	domain = os.environ["MORPH_DOMAIN"]
	
with requests.session() as s:
	s.headers['user-agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'

	for k, v in filtered_dict.items(): 
		checkURL = v
		if os.environ.get('MORPH_DEBUG') == "1":
			print(checkURL)
			
		if os.environ.get('MORPH_MAXDAYS') == "0":
			checkURL = checkURL.replace("added=24_hours&","")
			
		parsedURL = urlparse.urlparse(checkURL)
		params = urlparse.parse_qs(parsedURL.query)
		if 'page_size' in params:
			pageSize = int(params['page_size'][0])
		else:
			pageSize = 25
		
		r1 = s.get(checkURL)
		soup = BeautifulSoup(r1.content, 'html.parser')
		
		try:
			numOfResults = soup.find("span", {"class" : "listing-results-utils-count"}).text.replace(" ", "").split("of")
			numOfResults = int(numOfResults[1])
			numOfPages = math.ceil(float(numOfResults)/pageSize)
		except:
			numOfPages = 0	
		page = 0
		while page < numOfPages:
			numResults=0
			numPreFeat=0
			numNormFeat=0
			numFeat=0
			
			if page > 0: #get next page
				r1 = s.get(checkURL+"&pn="+str(page+1))
				soup = BeautifulSoup(r1.content, 'html.parser')
			
			searchResults = soup.find("ul", {"class" : "listing-results clearfix js-gtm-list"})
			matches = 0
			if searchResults is not None:		
				adverts = searchResults.findAll("li", {"id" : lambda L: L and L.startswith('listing_')})
				numResults = len(adverts)
				
				for advert in adverts:
					reduced=False
					if advert.find("div", {"class" : "listing-results-wrapper"}) is not None:
						advertMatch = {}
						postKey = random.choice(list(postTemplates))
						random.shuffle(list(postTemplates))
						agent = advert.find("p", {"class" : "top-half listing-results-marketed"}).find("span").text
						
						if any(x in agent.lower() for x in excludeAgents):
							continue;

						hashTagLocation = k.replace("MORPH_URL_","").replace("_"," ").title().replace(" ","")
						location = k.replace("MORPH_URL_","").replace("_"," ").title()
						propLink=domain+advert.find("a", {"class" : "listing-results-price text-price"}).get('href')
						propId=re.search('\d+',propLink.split("?")[0])
						if propId:
							propId=propId.group(0)
						title = advert.find("h2", {"class" : "listing-results-attr"}).text
						address = advert.find("a", {"class" : "listing-results-address"}).text
						price = parseAskingPrice(advert.find("a", {"class" : "listing-results-price text-price"}).text.strip())
						displayPrice = advert.find("a", {"class" : "listing-results-price text-price"})
						unwanted = displayPrice.find('span')
						if unwanted is not None:
							unwanted = displayPrice.find('span').extract()
							displayPrice = displayPrice.text.strip()+" "+unwanted.text.strip()
						else:
							displayPrice = displayPrice.text.strip()
						image1 = advert.find("a", {"class" : "photo-hover"}).find("img").get('src')
						addedOrReduced = advert.find("p", {"class" : "top-half listing-results-marketed"}).find("small").text.replace("Listed on","").replace("by","").strip()
						if addedOrReduced != None and addedOrReduced != "":
							addedOrReduced = parse(addedOrReduced)
						else:
							addedOrReduced = datetime.now().date()
						advertMatch['propId'] = propId
						advertMatch['link'] = propLink
						advertMatch['title'] = title.replace('Just added','').strip()
						advertMatch['address'] = address
						advertMatch['price'] = price
						advertMatch['displayPrice'] = displayPrice.replace('Just added','').strip()
						advertMatch['image1'] = image1
						advertMatch['pubDate'] = datetime.now()
						advertMatch['addedOrReduced'] = addedOrReduced
						advertMatch['reduced'] = reduced
						advertMatch['location'] = location
						advertMatch['hashTagLocation'] = hashTagLocation
						advertMatch['postContent'] = postTemplates[postKey].format(title, hashTagLocation, displayPrice)

						saveToStore(advertMatch)
						
						matches += 1
				print("Found "+str(matches)+" Matches from "+str(numResults)+" Items of which "+str(numFeat)+" are Featured")
				if matches == 0 or (numResults-numFeat-2)>matches:
					break		
			else:
				print('No Search Results\n')
			page +=1 
		time.sleep(sleepTime)
sys.exit(0)
