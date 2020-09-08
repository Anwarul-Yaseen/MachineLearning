import requests
from bs4 import BeautifulSoup
base_url = "https://www.yelp.com/search?find_desc=Restaurants&find_loc=";
loc = "New York,NY"
page = 0
url = base_url + loc +"&start="+str(page)
yelp_r = requests.get(url)
yelp_soup = BeautifulSoup(yelp_r.text,'html.parser')
for link in yelp_soup.findAll('a'):
   print(link)