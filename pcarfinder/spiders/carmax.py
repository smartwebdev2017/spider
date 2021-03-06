__author__ = 'root'

import scrapy
import sys
import urlparse
from pcarfinder.spiders import BaseProductsSpider
from pcarfinder.spiders import FormatterWithDefaults, cond_set_value
from pcarfinder.items import SiteProductItem
from scrapy.conf import settings
from pcarfinder.US_States import STATES
from scrapy.http import Request
from twisted.internet._sslverify import ClientTLSOptions
from twisted.internet.ssl import ClientContextFactory
from scrapy.core.downloader.contextfactory import ScrapyClientContextFactory
from OpenSSL import SSL
from pcarfinder.pcarfinder import PcarfinderDB
import json
import urllib2

import csv
import re
import datetime

class CustomClientContextFactory(ScrapyClientContextFactory):
    def getContext(self, hostname=None, port=None):
        ctx = ClientContextFactory.getContext(self)
        ctx.set_options(SSL.OP_ALL)
        if hostname:
            ClientTLSOptions(hostname, ctx)
        return ctx

class CarMaxSpinder(BaseProductsSpider):
    handle_httpstatus_list = [404]
    name = "carmax"
    allowed_domains = ['www.carmax.com']

    current_page = 1

    agent = "iphone_ipad': 'Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_6 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B651 Safari/9537.53",

    HOME_URL = "https://www.carmax.com"

    API_SEARCH_URL = "https://api.carmax.com/v1/api/vehicles?Distance=all&ExposedCategories=249+250+1001+1000+265+999+772&PerPage=20&SortKey=0&StartIndex={start_index}&ExposedDimensions=249+250+1001+1000+265+999+772&FreeText={search_term}&platform=carmax.com&Page={page_num}&Refinements=&Zip=01923&platform=carmax.com&apikey={apiKey}"

    SEARCH_URL = 'https://www.carmax.com/search#Distance=all&ExposedCategories=249+250+1001+1000+265+999+772&ExposedDimensions=249+250+1001+1000+265+999+772&FreeText={search_term}&Page={page_num}&PerPage=20&SortKey=15&Zip=01923'

    HEADERS={
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, sdch, br",
            "Accept-Language": "en-US,en;q=0.8",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "DNT":"1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": agent
    }

    def __init__(self, *args, **kwargs):

        self.db = PcarfinderDB()

        self.total_matches = None

        url_formatter = FormatterWithDefaults(page_num=1)
        super(CarMaxSpinder, self).__init__(url_formatter=url_formatter,
                                             site_name=self.allowed_domains[0],
                                             *args,
                                             **kwargs)

        settings.overrides['DOWNLOAD_DELAY'] = 1

    def start_requests(self):
        with open("out_car_max.csv", "a") as result:
            wr = csv.writer(result)
            #wr.writerow(['Listing_Date', 'Seller_Type', 'Sold_Status', 'Sold_Date', 'Listing_Year', 'Listing_Make', 'Listing_Model', 'Mileage', 'Condition', 'Price', 'Listing_Color', 'VIN', 'City', 'State', 'Listing_Body_Type', 'Listing_Transmission', 'Listing_Transmission_Detail', 'Drivetrain', 'Listing_Model_Detail', 'Listing_URL', 'Listing_Title', 'Listing_Description', 'VHR_Link' ])
            wr.writerow(['VIN', 'Listing_Make', 'Listing_Model', 'Listing_Trim', 'Listing_Model_Detail', 'Listing_Year', 'Mileage', 'City', 'State', 'Listing_Date', 'Price', 'Condition', 'Seller_Type', 'VHR_Link', 'Listing_Exterior_Color', 'Listing_Interior_Color', 'Listing_Transmission', 'Listing_Transmission_Detail', 'Listing_Title', 'Listing_URL', 'Listing_Engine_Size', 'Listing_Description', 'Sold_Status', 'Sold_Date', 'Listing_Body_Type', 'Listing_Drivetrain'])

        yield Request(
            self.HOME_URL,
            headers=self.HEADERS,
            callback=self._start_requests
        )
    def _start_requests(self, response):
        apiKey = re.search('"key":"(.*?)"}', response.body)

        if not self.product_url and apiKey:
            apiKey = apiKey.group(1)
            for st in self.searchterms:
                yield Request(
                    url=self.API_SEARCH_URL.format(search_term=st, start_index = (self.current_page - 1) * 20, page_num=self.current_page, apiKey=apiKey),
                    meta={
                        'search_term': st,
                        'apiKey': apiKey,
                        'remaining': sys.maxint
                    },
                    dont_filter=True,
                    headers=self.HEADERS,
                )
        elif self.product_url:
            prod = SiteProductItem()
            prod['url'] = self.product_url
            prod['search_term'] = ''

            yield Request(
                self.product_url,
                meta={
                    'product': prod,
                },
                callback=self._parse_single_product,
                headers={"User-Agent": self.agent},
            )

    def _parse_single_product(self, response):
        return self.parse_product(response)

    def parse_product(self, response):
        product = response.meta['product']

        info_content = response.xpath('//span[@data-prop="vehicleInfo"]')[0].extract()
        info = json.loads(re.search('>(.*?)</span>', info_content, re.DOTALL).group(1))
        description = ''
        cur_time = datetime.datetime.now()
        cur_str = ("%s-%s-%s, %s:%s" % (cur_time.month, cur_time.day, cur_time.year, cur_time.hour, cur_time.minute))

        product['condition'] = 'Used'
        cond_set_value(product, 'listing_date', cur_str)
        cond_set_value(product, 'seller_type', 'Dealership')
        #cond_set_value(product, 'sold_status', int(info['isSold']))
        isSold = 1
        if info['IsSold'] == False:
            isSold = 0

        cond_set_value(product, 'sold_date', cur_str)
        cond_set_value(product, 'listing_year', info['Year'])
        cond_set_value(product, 'listing_make', info['Make'])
        cond_set_value(product, 'listing_model', info['Model'])
        cond_set_value(product, 'mileage', info['Mileage'])
        cond_set_value(product, 'condition', 'Used')
        cond_set_value(product, 'price', info['Price'])
        cond_set_value(product, 'listing_color', info['ExteriorColor'])
        cond_set_value(product, 'vin', info['Vin'])

        city = self._parse_city(response)
        if city is None:
            city = self._clean_text(re.search('(=.*">)(.*?)<', response.xpath(".//div[@data-js='InfoBubble']")[0].extract(), re.DOTALL).group(2))
        cond_set_value(product, 'city', city)

        state = self._parse_state(response)
        if state is None:
            state = self._clean_text(re.search(',(.*?)<', response.xpath(".//div[@data-js='InfoBubble']")[0].extract(), re.DOTALL).group(1))
        else:
            state = state.strip()

        cond_set_value(product, 'state', state)

        cond_set_value(product, 'listing_body_type', '')
        transmission = info['Transmission']
        if transmission != 'Automatic':
            transmission = 'Manual'
        cond_set_value(product, 'listing_transmission', transmission)

        cond_set_value(product, 'listing_transmission_detail', '')
        cond_set_value(product, 'drive_train', info['DriveTrain'])
        cond_set_value(product, 'listing_model_detail', info['Description'])
        cond_set_value(product, 'listing_title', info['Description'])

        for feature in info['Features']:
            description += feature['DisplayText'] + '\r\n '
        cond_set_value(product, 'listing_description', description)

        model_detail = ''
        if info['Trim'] is None:
            model_detail = info['Model']
        else:
            model_detail = info['Model'] + ' ' + info['Trim']

        vin = self.db.check_vin_in_db(info['Vin'])
        site = self.db.get_site_id("carmax")

        if site is not None:
            if vin is None:
                bsf_data = self.db.getBSinfo(info['Vin'])
                bsf_id = self.db.insert_bsf(info['Vin'], bsf_data['msrp'], bsf_data['warranty_start'], bsf_data['model_year'], bsf_data['model_detail'], bsf_data['color'], bsf_data['production_month'], bsf_data['interior'])
                for option in bsf_data['options']:
                    self.db.insert_bsf_options(bsf_id, option['code'], option['value'])

                self.db.insert_car(site[0], info['Vin'].upper(), info['Make'], info['Model'], info['Trim'], model_detail, info['Year'], info['Mileage'], city, state, cur_str, info['Price'], 'Used', 'Dealership', 'https://www.carmax.com/car/' + str(info['StockNumber']) + '/vehicle-history', info['ExteriorColor'], info['InteriorColor'], transmission, '', info['Description'], product.get('url'), info['Size'], description,  isSold, cur_str, '', info['DriveTrain'], datetime.datetime.now(), '', bsf_id)

            else:
                self.db.update_car(info['Vin'].upper(), info['Make'], info['Model'], info['Trim'], model_detail, info['Year'], info['Mileage'], city, state, cur_str, info['Price'], 'Used', 'Dealership', 'https://www.carmax.com/car/' + str(info['StockNumber']) + '/vehicle-history', info['ExteriorColor'], info['InteriorColor'], transmission, '', info['Description'], product.get('url'), info['Size'], description,  isSold, cur_str, '', info['DriveTrain'], datetime.datetime.now())

            with open("out_car_max.csv", "a") as result:
                wr = csv.writer(result)
                #wr.writerow(['VIN', 'Listing_Make', 'Listing_Model', 'Listing_Trim', 'Listing_Model_Detail', 'Listing_Year', 'Mileage', 'City', 'State', 'Listing_Date', 'Price', 'Condition', 'Seller_Type', 'VHR_Link', 'Listing_Color', 'Listing_Interio_Color', 'Listing_Transmission', 'Listing_Transmission_Detail', 'Listing_Title', 'Listing_URL', 'Listing_Engine_Size', 'Listing_Description', 'Sold_Status', 'Sold_Date', 'Listing_Body_Type', 'Drivetrain'])
                try:
                    wr.writerow([info['Vin'].upper(), info['Make'], info['Model'], info['Trim'], model_detail, info['Year'], info['Mileage'], city, state, cur_str, '$' + str(info['Price']), 'Used', 'Dealership', 'https://www.carmax.com/car/' + str(info['StockNumber']) + '/vehicle-history', info['ExteriorColor'], info['InteriorColor'], transmission, '', info['Description'], product.get('url'), info['Size'], description,  isSold, cur_str, '', info['DriveTrain']])
                except Exception as err:
                    print(err)
        else:
            print("Carmax is not defined in db. Please check db now!")
        return product

    def _parse_location(self, response):
        location_data = response.xpath("//div[@class='info-bubble']/text()").extract()
        for data in location_data:
            if self._clean_text(data):
                return self._clean_text(data)

    def _parse_city(self, response):
        location = self._parse_location(response)
        return location.split(',')[0] if location else None

    def _parse_state(self, response):
        location = self._parse_location(response)
        return location.split(',')[1] if location else None

    def _scrape_product_links(self, response):
        product_links = []
        search_term = response.meta['search_term']
        link_domain = "https://www.carmax.com/car/"
        try:
            product_data = json.loads(response.body).get('Results', {})
            for data in product_data:
                param = str(data.get('StockNumber'))
                product_links.append(link_domain + param)

        except Exception as e:
            self.log("Error while parsing the product links {}".format(e))
        if product_links:
            for link in product_links:
                prod_item = SiteProductItem()
                req = Request(
                    url=link,
                    callback=self.parse_product,
                    meta={
                        'product': prod_item,
                        'search_term': search_term,
                        'remaining': sys.maxint,
                    },
                    dont_filter=True,
                    headers={"User-Agent": self.agent}
                )
                yield req, prod_item


    def _scrape_total_matches(self, response):
        try:
            total_matches = json.loads(response.body).get('ResultCount')
        except Exception as e:
            self.log("Error while parsing the total count".format(e))

        return total_matches if total_matches else 0

    def _scrape_next_results_page_link(self, response):
        st = response.meta['search_term']
        apiKey = response.meta['apiKey']
        total_matches = self._scrape_total_matches(response)

        if self.current_page * 20 > total_matches:
             return

        self.current_page += 1
        #count = total_matches / 20 + 1
        #if self.current_page > count:
        #    return

        next_page_link = self.API_SEARCH_URL.format(search_term=st, start_index = (self.current_page - 1) * 20, page_num=self.current_page, apiKey=apiKey)

        return Request(
            url = next_page_link,
            headers=self.HEADERS,
            meta={
                'search_term': st,
                'apiKey': apiKey,
                'remaining': sys.maxint
            },
            dont_filter=True
        )

    def _clean_text(self, text):
        return re.sub("[\r\n\t]", "", text).strip()

