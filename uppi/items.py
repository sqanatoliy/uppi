# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class UppiItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    nav_to_visure_catastali = scrapy.Field()
    codice_fiscale = scrapy.Field()
    comune = scrapy.Field()
    tipo_catasto = scrapy.Field()
    ufficio_label = scrapy.Field()
