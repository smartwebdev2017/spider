__author__ = 'root'

import scrapy
from pcarfinder.spiders import BaseProductsSpider
from pcarfinder.spiders import FormatterWithDefaults, cond_set_value
from pcarfinder.items import SiteProductItem
from pcarfinder.US_States import STATES
from scrapy.http import Request
import csv
import re
import datetime
from dateutil.parser import parse
class RennlistSpider(BaseProductsSpider):
    handle_httpstatus_list = [524]

    name = "rennlist"
    allowed_domains = ['rennlist.com']

    SEARCH_URL = 'https://rennlist.com/forums/marketplace/cars/search/f4-{search_term}/f7-min-max/f6-min-max/f10-1/page-{page_num}/'

    def __init__(self, *args, **kwargs):
        self.total_matches = None

        url_formatter = FormatterWithDefaults(page_num=1)
        super(RennlistSpider, self).__init__(url_formatter=url_formatter,
                                             site_name=self.allowed_domains[0],
                                             *args,
                                             **kwargs)
    def start_requests(self):
        with open("out.csv", "a") as result:
            wr = csv.writer(result)
            wr.writerow(['Listing_Date', 'Seller_Type', 'Sold_Status', 'Sold_Date', 'Listing_Year', 'Listing_Make', 'Listing_Model', 'Mileage', 'Condition', 'Price', 'Listing_Exterior_Color', 'VIN', 'City', 'State', 'Listing_Body_Type', 'Listing_Transmission', 'Listing_Transmission_Detail', 'Listing_Drivetrain', 'Listing_Model_Detail', 'Listing_URL', 'Listing_Title', 'Listing_Description' ])

        for request in super(RennlistSpider, self).start_requests():
            if request.meta.get('search_term'):
                yield request

    def after_start(self, response):
        pass
    def __parse_sing_product(self, response):
        return self.parse_product(response)
    def parse_product(self, response):
        content = response.xpath('//div[@id="posts"]//div[contains(@id, "post_message")]//p')[0].extract()

        try:
            year_str = re.search('<strong>Year:</strong>(.*?)<br>', content).group(1)
        except:
            return None

        try:
            make_str = re.search('<strong>Make:</strong>\s(.+?)<br>', content).group(1)
        except:
            return None

        try:
            model_str = re.search('<strong>Model:</strong>\s(.+?)<br>', content).group(1)
            if model_str == '0':
                model_str = ''
        except:
            model_str = ''

        try:
            price_str = re.search('<strong>Price.*?</strong>(.*?)<br>', content).group(1)
        except:
            price_str = 0

        try:
            color_str = re.search('<strong>Color:</strong>(.*?)<br>', content).group(1)
        except:
            color_str = ''

        try:
            vin_str = re.search('<strong>VIN:</strong>.*?>(.*?)</a><br>', content).group(1)
        except:
            vin_str = ''

        try:
            location_str = re.search('<strong>Location.*?</strong>(.*?)<br>', content).group(1)
        except:
            location_str = ''

        try:
            style_str = re.search('<strong>Body Style.*?</strong>(.*?)<br>', content).group(1)
        except:
            style_str = ''

        try:
            transmission_detail_str = re.search('<strong>Transmission.*?</strong>(.*?)<br>', content).group(1)
            if transmission_detail_str.strip() == 'Tiptronic' or transmission_detail_str.strip() == 'PDK':
                transmission_str = 'Auto'
            else:
                transmission_str = 'Manual'
        except:
            transmission_detail_str = ''
            transmission_str = ''

        try:
            wheel_str = re.search('<strong>2 or 4.*?</strong>(.*?)<br>', content).group(1)
            if wheel_str.strip() == '2 Wheel Drive':
                wheel_str = '2WD'
            elif wheel_str.strip() == '4 Wheel Drive':
                wheel_str = '4WD'
        except:
            wheel_str = ''

        try:
            engine_str = re.search('<strong>Engine Type.*?</strong>(.*?)<br>', content).group(1)
        except:
            engine_str = ''

        try:
            stereo_str = re.search('<strong>Stereo System.*?</strong>(.*?)<br>', content).group(1)
        except:
            stereo_str = ''

        try:
            cont_str = re.search('<strong>Cont.*?</strong>(.*?)<br>', content).group(1)
        except:
            cont_str = ''

        try:
            options_str = re.search('<strong>Options.*?</strong>(.*?)<br>', content).group(1)
        except:
            options_str = ''

        product = response.meta['product']

        try:
            dealer_ship = re.search('<b>(.*?)</b>', product.get('dealer_ship')).group(1)
        except:
            dealer_ship = 'Private Party'

        try:
            mileage_str = re.search('<strong>Mileage\s(.+?)</strong>\s(\d+)<br>', content).group(2)
        except:
            mileage_str = ''

        if mileage_str == '':
            condition = ''
        elif int(mileage_str) <=500 and dealer_ship != 'Private Party':
            condition = 'New'
        else:
            condition = 'Used'

        try:
            sold_status = re.search('<strong>(.*?)</strong>', product.get('sold_status')).group(1)
            sold_status = '1'
            cur_time = datetime.datetime.now()
            cur_str = ("%s-%s-%s, %s:%s" % (cur_time.month, cur_time.day, cur_time.year, cur_time.hour, cur_time.minute))
        except:
            sold_status = '0'
            cur_str = ''

        city_content = response.xpath('//div[@id="posts"]//div[contains(@id, "post")]//div[@class="tcell alt2"]')[0].extract()

        try:
            location = re.search('<div>Location:\s(.*?)</div>', city_content).group(1)

            try:
                city = location.split(',')[0].strip()
            except:
                city = ''

            try:
                state = location.split(',')[1].strip()
            except:
                state = ''

            if len(city) == 2:
                state = city
                city = ''
            state = ''.join([i for i in state if not i.isdigit()])
            if STATES.get(state.lower()) is not None:
                state = STATES[state.lower()]
        except:
            city = ''
            state = ''

        try:
             description = response.xpath('//div[@id="posts"]//div[contains(@id, "post_message")]')[0].extract()
             description = re.search('</p>(.*?)</div>', description, re.DOTALL).group(1)
             description = re.sub('<.*?>', '', description)
        except:
            description = ''
        try:
            date_content = response.xpath('//div[@id="posts"]//div[contains(@id, "post")]//div[@class="trow thead smallfont"]/div[@class="tcell"]')[0].extract()
            posted_date = re.search('</a>\r\n\t\t(.*)\r\n\t\t', date_content).group(1)
            date = posted_date.split(',')

            if date[0] == 'Today':
                today = datetime.datetime.now()
                date[0] = today.strftime('%m-%d-%Y')
                posted_date = date[0] + ',' + date[1]
            elif date[0] == 'Yesterday':
                yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
                date[0] = yesterday.strftime('%m-%d-%Y')
                posted_date = date[0] + ',' + date[1]
        except:
            posted_date = ''
        try:
            title = response.xpath('//div[@class="row sticky-container"]//h1')[0].extract()
            title = re.search('>\s(.*?)</h1>', title).group(1)
        except:
            title = ''
        with open("out.csv", "a") as result:
            wr = csv.writer(result)
            #wr.writerow(['Year', 'Make', 'Model', 'Mileage', 'Price', 'Color', 'VIN', 'Location' 'Style', 'Transmission', 'Wheel', 'Engine', 'Stereo', 'Cont', 'Option'])
            wr.writerow([posted_date, dealer_ship, sold_status, cur_str, year_str, make_str, model_str, mileage_str, condition, price_str, color_str, vin_str.upper(), city, state, style_str, transmission_str, transmission_detail_str, wheel_str, cont_str, product.get('url'), title, description])
        return product

    def _scrape_product_links(self, response):
        products_container = response.xpath('//div[@id="threadslist"]//div[@class="trow text-center"]//div[@class="tcell alt1 text-break text-left"]')
        products_link = response.xpath('//div[@id="threadslist"]//div[@class="trow text-center"]//div[@class="tcell alt1 text-break text-left"]//a[contains(@id, "title")]/@href').extract()
        for product in products_container:
            dealer_ship = product.xpath('.//span[@style="color:blue"]/b').extract()
            link = product.xpath('.//a[contains(@id, "title")]/@href').extract()
            sold_status = product.xpath('.//  span[@class="highlight alert"]/strong').extract()
            product_item = SiteProductItem()

            cond_set_value(product_item, 'url', 'https://rennlist.com/forums/' + link[0])

            try:
                cond_set_value(product_item, 'dealer_ship', dealer_ship[0])
            except:
                cond_set_value(product_item, 'dealer_ship', '')

            try:
                cond_set_value(product_item, 'sold_status', sold_status[0])
            except:
                cond_set_value(product_item, 'sold_status', '')
            yield 'https://rennlist.com/forums/' + link[0], product_item

    def _scrape_total_matches(self, response):
        return None

    def _scrape_results_per_page(self, response):
        products_link = response.xpath('//div[@id="threadslist"]//div[@class="trow text-center"]//div[@class="tcell alt1 text-break text-left"]//a[contains(@id, "title")]/@href').extract()
        return products_link

    def _scrape_next_results_page_link(self, response):
        meta = response.meta
        current_page = meta.get('current_page')
        if not current_page:
            current_page = 1
        #if current_page * 40 >= self.total_matches:
        #    return
        current_page += 1
        st = meta.get('search_term')
        meta['current_page'] = current_page
        next_url = self.SEARCH_URL.format(search_term=st, page_num=current_page)
        return Request(url=next_url, meta = meta)