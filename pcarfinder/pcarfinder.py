__author__ = 'root'
import mysql.connector
from bs4 import BeautifulSoup
import requests
import re

class PcarfinderDB():
    def __init__(self):
        self.conn = mysql.connector.connect(user='root', password='root', db='test1', host='localhost', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def check_vin_in_db(self, vin_code):
        sql = "select * from api_car where vin_code = '%s' "%(vin_code)
        self.cursor.execute(sql)
        vin = self.cursor.fetchone()
        return vin

    def get_site_id(self, name):
        sql = "select * from api_site where site_name = '%s' " %(name)
        self.cursor.execute(sql)
        site = self.cursor.fetchone()
        return site

    def update_site_status(self, site_id):
        sql = "UPDATE api_site SET updated = NOW() WHERE id = %s" % (site_id)
        self.cursor.execute(sql)
        site = self.cursor.fetchone()
        return site

    def insert_car(self, site, vin, listing_make, listing_model, listing_trim, listing_model_detail, listing_year, mileage, city, state, listing_date,
                   price, cond, seller_type, vhr_link, listing_exterior_color, listing_interior_color, listing_transmission, listing_transmission_detail,
                   listing_title, listing_url, listing_engine_size, listing_description, sold_state, sold_date, listing_body_type, listing_drivetrain, created, updated, bsf_id):

        sql = "INSERT INTO api_car (site_id, vin_code, listing_make, listing_model, listing_trim, listing_model_detail, listing_year, mileage, city, state, " \
                      "listing_date, price, cond, seller_type, vhr_link, listing_exterior_color, listing_interior_color, listing_transmission, listing_transmission_detail, " \
                      "listing_title, listing_url, listing_engine_size, listing_description, sold_state, sold_date, listing_body_type, listing_drivetrain, created, updated, " \
                      "pcf_id, vdf_id, vhf_id, vin_id )" \
                      "values ('%s', '%s', '%s', '%s', '%s', '%s', %s, %s, '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s) " % \
                      (site, vin, listing_make, listing_model, listing_trim, listing_model_detail, listing_year, mileage, city, state, listing_date, price, cond, seller_type, vhr_link,
                       listing_exterior_color, listing_interior_color, listing_transmission, listing_transmission_detail, listing_title, listing_url, listing_engine_size, listing_description,  sold_state,
                       sold_date, listing_body_type, listing_drivetrain, created, updated, 'NULL','NULL','NULL',bsf_id)
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            print("%s is added successfully" % vin)
        except Exception as e:
            print(e)
            self.conn.rollback()

    def update_car(self, vin, listing_make, listing_model, listing_trim, listing_model_detail, listing_year, mileage, city, state, listing_date,
                   price, cond, seller_type, vhr_link, listing_exterior_color, listing_interior_color, listing_transmission, listing_transmission_detail,
                   listing_title, listing_url, listing_engine_size, listing_description, sold_state, sold_date, listing_body_type, listing_drivetrain, updated):
        sql = "UPDATE api_car SET listing_make = '%s', listing_model = '%s', listing_trim = '%s', listing_model_detail = '%s', listing_year = %s, mileage = %s, city = '%s', state = '%s', " \
                      "listing_date = '%s', price = %s, cond = '%s', seller_type = '%s', vhr_link = '%s', listing_exterior_color = '%s', listing_interior_color = '%s', listing_transmission = '%s', listing_transmission_detail = '%s', " \
                      "listing_title = '%s', listing_url = '%s', listing_engine_size = '%s', listing_description = '%s', sold_state = %s, sold_date = '%s', listing_body_type = '%s', listing_drivetrain = '%s', updated = '%s' " \
                      "WHERE vin_code = '%s' " % (listing_make, listing_model, listing_trim, listing_model_detail, listing_year, mileage, city, state, listing_date, price, cond, seller_type, vhr_link,
                                          listing_exterior_color, listing_interior_color, listing_transmission, listing_transmission_detail, listing_title, listing_url, listing_engine_size,
                                          listing_description,  sold_state, sold_date, listing_body_type, listing_drivetrain, updated, vin)
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            print("%s is added successfully" % vin)
        except Exception as e:
            print(e)
            self.conn.rollback()

    def insert_temp_data(self, vin_id, code, value):
        sql = "INSERT INTO temp (vin_id, code, value) values (%s, '%s', '%s') " % (vin_id, code, value)

        try:
            self.cursor.execute(sql)
            self.conn.commit()
            print('%s is added successfully' %(vin_id))
        except Exception as e:
            print(e)
            self.conn.rollback()

    def insert_bsf(self, vin_id, msrp, warranty_start, model_year, model_detail, color, production_month, interior):
        sql = "SELECT vin FROM api_bsf where vin='%s' " % (vin_id)
        self.cursor.execute(sql)
        vin = self.cursor.fetchone()
        if vin is not None:
            return vin

        sql = "INSERT INTO api_bsf (vin, msrp, warranty_start, model_year, model_detail, color, production_month, interior) " \
              " values ('%s', %s, '%s', %s, '%s', '%s', '%s', '%s') " % (vin_id, msrp, warranty_start, model_year, model_detail, color, production_month, interior)

        try:
            self.cursor.execute(sql)
            self.conn.commit()
            print('%s is added successfully' %(vin_id))
            id = self.cursor.lastrowid
            return id
        except Exception as e:
            print(e)
            self.conn.rollback()

    def insert_bsf_options(self, bsf_id, code, value):
        sql = "INSERT INTO api_bsf_options (bsf_id, code, value) values (%s, '%s', '%s') " % (bsf_id, code, value)

        try:
            self.cursor.execute(sql)
            self.conn.commit()
            print('%s is added successfully' %(bsf_id))
        except Exception as e:
            print(e)
            self.conn.rollback()

    def updateBsfById(self, id, warranty_start, production_month, color, interior):
        sql = "UPDATE api_bsf SET warranty_start = '%s', production_month = '%s', color = '%s', interior='%s' WHERE id = %s " % (warranty_start, production_month, color, interior, id)

        try:
            self.cursor.execute(sql)
            self.conn.commit()
            print("%s is updated successfully" % id)
        except Exception as e:
            print(e)
            self.conn.rollback()

    def getOptionsByBsfId(self, bsf_id):
        sql = "SELECT * FROM temp WHERE vin_id = '%s' " % (bsf_id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def getBSinfo(self, vin):
        data = {}
        url = 'https://admin.porschedealer.com/reports/build_sheets/print.php?vin=%s'

        res = requests.get(url % vin)

        bs = BeautifulSoup(res.content, 'html.parser')
        title = bs.find('h1').text
        try:
            model_year = re.search('(\d{4}\s)(.*?\s)(.*?$)', title).group(1)
            model = re.search('(\d{4}\s)(.*?\s)(.*?$)', title).group(2)
            model_detail =  model + re.search('(\d{4}\s)(.*?\s)(.*?$)', title).group(3)

            data['model_year'] = model_year
            data['model'] = model
            data['model_detail'] = model_detail
            data['vin'] = vin


        except Exception as e:
            print('Parsing Error in regular expressions')

        vehicle = bs.find('div', {'class':'vehicle'})
        vehicle_labels = vehicle.findAll('div', {'class':'label'})
        vehicle_values = vehicle.findAll('div', {'class':'value'})

        print('Vehicle')
        data['production_month'] = ''
        data['msrp'] = 0
        data['color'] = ''
        data['interior'] = ''
        data['warranty_start'] = ''

        for i in range(0, len(vehicle_labels)):
            if vehicle_labels[i].text == 'Division:':
                pass
            elif vehicle_labels[i].text == 'Commission #:':
                pass
            elif vehicle_labels[i].text == 'Prod Month:':
                data['production_month'] = vehicle_values[i].text
            elif vehicle_labels[i].text == 'Price:':
                data['msrp'] = vehicle_values[i].text.replace("$", "").replace(",","")
            elif vehicle_labels[i].text == 'Exterior:':
                data['color'] = vehicle_values[i].text
            elif vehicle_labels[i].text == 'Interior:':
                data['interior'] = vehicle_values[i].text
            elif vehicle_labels[i].text == 'Warranty Start:':
                data['warranty_start'] = vehicle_values[i].text

            print('%s, %s' %(vehicle_labels[i].text, vehicle_values[i].text))

        options = bs.find('div', {'class':'options'})
        options_labels = options.findAll('div', {'class':'label'})
        options_values = options.findAll('div', {'class':'value'})


        data['options'] = []
        for i in range(0, len(options_labels)):
            option = {}
            option['code'] = options_labels[i].text
            option['value'] = options_values[i].text
            data['options'].append(option)
            print(option)

        print(data)
        return data