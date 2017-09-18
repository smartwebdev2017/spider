__author__ = 'root'
import mysql.connector

class PcarfinderDB():
    def __init__(self):
        self.conn = mysql.connector.connect(user='root', password='root', db='pfinder', host='localhost', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def check_vin_in_db(self, vin_code):
        sql = "select * from api_car where vin = '%s' "%(vin_code)
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
                   listing_title, listing_url, listing_engine_size, listing_description, sold_state, sold_date, listing_body_type, listing_drivetrain, created, updated):

        sql = "INSERT INTO api_car (site_id, vin, listing_make, listing_model, listing_trim, listing_model_detail, listing_year, mileage, city, state, " \
                      "listing_date, price, cond, seller_type, vhr_link, listing_exterior_color, listing_interior_color, listing_transmission, listing_transmission_detail, " \
                      "listing_title, listing_url, listing_engine_size, listing_description, sold_state, sold_date, listing_body_type, listing_drivetrain, created, updated) " \
                      "values ('%s', '%s', '%s', '%s', '%s', '%s', %s, %s, '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s') " % \
                      (site, vin, listing_make, listing_model, listing_trim, listing_model_detail, listing_year, mileage, city, state, listing_date, price, cond, seller_type, vhr_link,
                       listing_exterior_color, listing_interior_color, listing_transmission, listing_transmission_detail, listing_title, listing_url, listing_engine_size, listing_description,  sold_state,
                       sold_date, listing_body_type, listing_drivetrain, created, updated)
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
                      "WHERE vin = '%s' " % (listing_make, listing_model, listing_trim, listing_model_detail, listing_year, mileage, city, state, listing_date, price, cond, seller_type, vhr_link,
                                          listing_exterior_color, listing_interior_color, listing_transmission, listing_transmission_detail, listing_title, listing_url, listing_engine_size,
                                          listing_description,  sold_state, sold_date, listing_body_type, listing_drivetrain, updated, vin)
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            print("%s is added successfully" % vin)
        except Exception as e:
            print(e)
            self.conn.rollback()
