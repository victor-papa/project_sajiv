# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http import Request, HtmlResponse
from scrapy_splash import SplashRequest, SplashJsonResponse, SplashTextResponse
import base64
import time
import datetime
from pkgutil import get_data
from w3lib.http import basic_auth_header

class parameters:
    starturl = 'https://www.betfair.com/exchange/plus/football/competition/2005'
    #starturl = 'https://www.betfair.com/exchange/plus/football'
    baseurl = 'https://www.betfair.com/exchange/plus/'
    #input link format: http://localhost:8050/\\"football/market/1.155251684\\
    #localhost = 'http://localhost:8050/\\"'
    localhost = 'https://rvgvp6ok-splash.scrapinghub.com/\\'
    #wait = 10
    sleeptime = 0

class MainQuotesItem(scrapy.Item):
    gametag = scrapy.Field()
    timestamp = scrapy.Field()
    market_name = scrapy.Field()
    pick_1 = scrapy.Field()
    pick_2 = scrapy.Field()
    pick_X = scrapy.Field()
    back_1 = scrapy.Field()
    back_2 = scrapy.Field()
    back_X = scrapy.Field()
    back_size_1 = scrapy.Field()
    back_size_X = scrapy.Field()
    back_size_2 = scrapy.Field()
    lay_1 = scrapy.Field()
    lay_2 = scrapy.Field()
    lay_X = scrapy.Field()
    lay_size_1 = scrapy.Field()
    lay_size_X = scrapy.Field()
    lay_size_2 = scrapy.Field()
    current_gametime = scrapy.Field()
    current_score = scrapy.Field()
    volume = scrapy.Field()
    eventname = scrapy.Field()
    competition = scrapy.Field()

class CorrectScoresItem(scrapy.Item):
    gametag = scrapy.Field()
    timestamp = scrapy.Field()
    market_name = scrapy.Field()
    back_noscore = scrapy.Field()
    lay_noscore = scrapy.Field()
    back_size_noscore = scrapy.Field()
    lay_size_noscore = scrapy.Field()
    current_gametime = scrapy.Field()
    current_score = scrapy.Field()
    volume = scrapy.Field()
    eventname = scrapy.Field()

class HandicapQuotesItem(scrapy.Item):
    gametag = scrapy.Field()
    timestamp = scrapy.Field()
    market_name = scrapy.Field()
    pick_1 = scrapy.Field()
    pick_2 = scrapy.Field()
    pick_X = scrapy.Field()
    back_1 = scrapy.Field()
    back_2 = scrapy.Field()
    back_X = scrapy.Field()
    back_size_1 = scrapy.Field()
    back_size_X = scrapy.Field()
    back_size_2 = scrapy.Field()
    lay_1 = scrapy.Field()
    lay_2 = scrapy.Field()
    lay_X = scrapy.Field()
    lay_size_1 = scrapy.Field()
    lay_size_X = scrapy.Field()
    lay_size_2 = scrapy.Field()
    current_gametime = scrapy.Field()
    current_score = scrapy.Field()
    volume = scrapy.Field()
    eventname = scrapy.Field()

def addbaselink(rellink):
    abslink = parameters.baseurl + rellink
    return abslink

def saveimage(response, tag):
    imgdata = base64.b64decode(response.data['png'])
    marketid = response.url.split(".")[-1]
    marketid = marketid.replace("'\'",'')
    name = marketid + str(tag) + str(int(time.time()))
    _file2 = "{0}.png".format(name)
    with open(_file2, 'wb') as f:
        f.write(imgdata)

def creategametag(eventname):
    today = datetime.datetime.today().strftime('%Y%m%d')
    teams = eventname.split(" v ")
    gametag = today + "_" + teams[0][:3] + teams[1][:3]
    gametag = gametag.upper()
    return gametag


def getsessionid(response):
    sessionid = "create"
    # headers = response.data['headers']
    # sessionheader = next((item for item in headers if item["name"] == "X-Crawlera-Session"),dict())
    # if "value" in sessionheader:
    #     sessionid = sessionheader.get("value")
    return sessionid

class CoursecrawlerSplashSpider(CrawlSpider):
    name = '20200312_oddsscraper_WOLSHA'
    #allowed_domains = ['betfair.com']

    def __init__(self, *args, **kwargs):
        # to be able to load the Lua script on Scrapy Cloud, make sure your
        # project's setup.py file contains the "package_data" setting, similar
        # to this project's setup.py
        self.LUA_SOURCE = get_data(
            'betfair', 'scripts/crawlera.lua'
        ).decode('utf-8')
        super(CoursecrawlerSplashSpider, self).__init__(*args, **kwargs)

    #overwrite these 2 functions of CrawlSpider class to avoid non-HtmlResponses get skipped in CrawlSpider
    #see https://github.com/scrapy-plugins/scrapy-splash/issues/92
    def _requests_to_follow(self, response):
        print "entered _request_to_follow"
        seen = set()
        for n, rule in enumerate(self._rules):
            links = [lnk for lnk in rule.link_extractor.extract_links(response) if lnk not in seen]
            print links

            if links and rule.process_links:
                links = rule.process_links(links)
            for link in links:
                seen.add(link)
                r = self._build_request(n, link)
                yield rule.process_request(r)

    def _build_request(self, rule, link):
        print link.url
        #r = SplashRequest(url=link.url, endpoint='execute', callback=self._response_downloaded, args={'html': 1, 'png': 1, 'wait': parameters.wait, 'timeout': 300, 'lua_source': self.script})
        r = SplashRequest(
            url=link.url,
            endpoint='execute', 
            callback=self._response_downloaded,
            dont_filter=True,
            splash_headers={
                'Authorization': basic_auth_header(self.settings['SPLASH_APIKEY'], ''),
            },
            args={
                'lua_source': self.LUA_SOURCE,
                'crawlera_user': self.settings['CRAWLERA_APIKEY'],
                'timeout': 60,
            },
            cache_args=['lua_source'],
        )

        r.meta.update(rule=rule, link_text=link.text)
        return r

    def start_requests(self):
        print "start_requests"
        #yield SplashRequest(url=parameters.starturl, endpoint='execute', args={'html': 1, 'png': 1, 'wait': parameters.wait, 'timeout': 300, 'lua_source': self.script}, )
        yield SplashRequest(
            url=parameters.starturl,
            endpoint='execute',
            splash_headers={
                'Authorization': basic_auth_header(self.settings['SPLASH_APIKEY'], ''),
            },
            args={
                'lua_source': self.LUA_SOURCE,
                'crawlera_user': self.settings['CRAWLERA_APIKEY'],
                'timeout': 60,
            },
            # tell Splash to cache the lua script, to avoid sending it for every request
            cache_args=['lua_source'],
        )

    rules = (
        Rule(LinkExtractor(restrict_xpaths='(//li[contains(text(),"Wolfsburg")]/ancestor::a[contains(@class,"mod-link")])[1]'), 
                process_links='process_links', process_request='splash_request', callback='parse_main', follow=True),
        Rule(LinkExtractor(restrict_xpaths='//a[text()="Correct Score "]'), 
                process_links='process_links', process_request='splash_request', callback='parse_correctscore', follow=False),
        Rule(LinkExtractor(restrict_xpaths='//a[contains(text()," +1 ")]'), 
                process_links='process_links', process_request='splash_request', callback='parse_handicaps', follow=False),
        # Rule(LinkExtractor(restrict_xpaths='//span[contains(text(),"Italian Serie A")]/ancestor::div[contains(@class,"card-header")]/following-sibling::div/bf-coupon-table[1]/descendant::a[contains(@class,"mod-link")]'), 
        #         process_links='process_links', process_request='splash_request', callback='parse_main', follow=True),
    )

    def process_links(self, links):
        print "process_links"
        print "input links:"
        print links
        #http://localhost:8050/\\"football/market/1.155251684\\
        for link in links:
            if parameters.localhost in link.url:
                #[:-2] to remove the \\ in http://localhost:8050/\\"football/market/1.155251684\\
                link.url = link.url.replace(parameters.localhost, parameters.baseurl)
                link.url = link.url[:-2]
        print "output links:"
        print links
        return links

    def splash_request(self, request):
        # print "processing splash test to add waiting time"
        # print "Sleeping " + str(parameters.sleeptime) + " seconds"
        # time.sleep(parameters.sleeptime)
        return request


    def parse_main(self, response):
        print "processing parse_main"
        print response.url
        print response.headers

        print "saving image..."
        saveimage(response,"_main_")

        #scrape main data
        item = MainQuotesItem()
        eventname = response.xpath('//a[contains(@class,"EVENT   open active-node")]/text()').extract_first()
        item['eventname'] = eventname
        item['gametag'] = creategametag(eventname)
        item['competition'] = response.xpath('//a[@link-type="COMP"]/text()').extract_first()
        item['timestamp'] = int(time.time())
        item['current_gametime'] = response.xpath('//p[@class="time-elapsed inplay"]/span/text()').extract_first()
        item['current_score'] = response.xpath('//span[contains(@class,"score")]/text()').extract_first()
        item['volume'] = response.xpath('//*[contains(@class,"total-matched")]/text()')[1].extract()
        item['market_name'] = response.xpath('//h2[contains(@class,"market-type")]/text()').extract_first()

        #prices
        table = response.xpath('//table[@class="mv-runner-list"]')[1]
        rows = table.xpath('.//tr')
        back_prices = rows.xpath('.//button[contains(@class,"back-button back-selection-button")]/@price').extract()
        back_sizes = rows.xpath('.//button[contains(@class,"back-button back-selection-button")]/@size').extract()
        lay_prices = rows.xpath('.//button[contains(@class,"lay-button lay-selection-button")]/@price').extract()
        lay_sizes = rows.xpath('.//button[contains(@class,"lay-button lay-selection-button")]/@size').extract()
        picks = rows.xpath('.//h3/text()').extract()
        print "picks and prices: "
        print picks
        print back_prices
        print back_sizes
        item['back_1'] = back_prices[0]
        item['back_2'] = back_prices[1]
        item['back_X'] = back_prices[2]
        item['pick_1'] = picks[0]
        item['pick_2'] = picks[1]
        item['pick_X'] = picks[2]
        item['back_size_1'] = back_sizes[0].encode('utf-8')
        item['back_size_X'] = back_sizes[1].encode('utf-8')
        item['back_size_2'] = back_sizes[2].encode('utf-8')
        item['lay_1'] = lay_prices[0]
        item['lay_2'] = lay_prices[1]
        item['lay_X'] = lay_prices[2]
        item['lay_size_1'] = lay_sizes[0].encode('utf-8')
        item['lay_size_X'] = lay_sizes[1].encode('utf-8')
        item['lay_size_2'] = lay_sizes[2].encode('utf-8')

        #return item
        return item


    def parse_correctscore(self, response):
        print "processing parse_correctscore"
        print response.url
        print response.headers

        print "saving image..."
        saveimage(response,"_correctscore_")

        #scrape data
        item = CorrectScoresItem()
        eventname = response.xpath('//a[contains(@class,"EVENT   open active-node")]/text()').extract_first()
        item['eventname'] = eventname
        item['gametag'] = creategametag(eventname)
        item['timestamp'] = int(time.time())
        item['current_gametime'] = response.xpath('//p[@class="time-elapsed inplay"]/span/text()').extract_first()
        item['volume'] = response.xpath('//*[contains(@class,"total-matched")]/text()')[1].extract()
        item['market_name'] = response.xpath('//h2[contains(@class,"market-type")]/text()').extract_first()

        #find current score and noscore_back
        current_score = response.xpath('//span[contains(@class,"score")]/text()').extract_first()
        print current_score
        if current_score == ' ' or current_score is None:
            current_score = '0-0 '
        print current_score
        current_score_l = current_score.strip().split('-')
        current_score_s = current_score_l[0] + " - " + current_score_l[1]
        noscore_row = response.xpath('//h3[text()="' + current_score_s + '"]/ancestor::tr')
        item['current_score'] = current_score
        item['back_noscore'] = noscore_row.xpath('.//button[contains(@class,"back-button back-selection-button")]/@price').extract_first()
        item['back_size_noscore'] = noscore_row.xpath('.//button[contains(@class,"back-button back-selection-button")]/@size').extract_first().encode('utf-8')
        item['lay_noscore'] = noscore_row.xpath('.//button[contains(@class,"lay-button lay-selection-button")]/@price').extract_first()
        item['lay_size_noscore'] = noscore_row.xpath('.//button[contains(@class,"lay-button lay-selection-button")]/@size').extract_first().encode('utf-8')

        #return item
        return item

    def parse_handicaps(self, response):
        print "processing parse_handicaps"
        print response.url
        print response.headers

        print "saving image..."
        saveimage(response,"_handicap_")

        #scrape data
        item = HandicapQuotesItem()
        eventname = response.xpath('//a[contains(@class,"EVENT   open active-node")]/text()').extract_first()
        item['eventname'] = eventname
        item['gametag'] = creategametag(eventname)
        item['timestamp'] = int(time.time())
        item['current_gametime'] = response.xpath('//p[@class="time-elapsed inplay"]/span/text()').extract_first()
        item['current_score'] = response.xpath('//span[contains(@class,"score")]/text()').extract_first()
        item['volume'] = response.xpath('//*[contains(@class,"total-matched")]/text()')[1].extract()
        item['market_name'] = response.xpath('//h2[contains(@class,"market-type")]/text()').extract_first()

        #prices
        table = response.xpath('//table[@class="mv-runner-list"]')[1]
        rows = table.xpath('.//tr')
        back_prices = rows.xpath('.//button[contains(@class,"back-button back-selection-button")]/@price').extract()
        back_sizes = rows.xpath('.//button[contains(@class,"back-button back-selection-button")]/@size').extract()
        lay_prices = rows.xpath('.//button[contains(@class,"lay-button lay-selection-button")]/@price').extract()
        lay_sizes = rows.xpath('.//button[contains(@class,"lay-button lay-selection-button")]/@size').extract()
        picks = rows.xpath('.//h3/text()').extract()
        print "picks and prices: "
        print picks
        print back_prices
        print back_sizes
        item['back_1'] = back_prices[0]
        item['back_2'] = back_prices[1]
        item['back_X'] = back_prices[2]
        item['pick_1'] = picks[0]
        item['pick_2'] = picks[1]
        item['pick_X'] = picks[2]
        item['back_size_1'] = back_sizes[0].encode('utf-8')
        item['back_size_X'] = back_sizes[1].encode('utf-8')
        item['back_size_2'] = back_sizes[2].encode('utf-8')
        item['lay_1'] = lay_prices[0]
        item['lay_2'] = lay_prices[1]
        item['lay_X'] = lay_prices[2]
        item['lay_size_1'] = lay_sizes[0].encode('utf-8')
        item['lay_size_X'] = lay_sizes[1].encode('utf-8')
        item['lay_size_2'] = lay_sizes[2].encode('utf-8')

        #return item
        return item
