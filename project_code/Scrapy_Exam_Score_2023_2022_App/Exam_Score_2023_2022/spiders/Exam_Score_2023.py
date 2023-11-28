import scrapy
import json

class DiemthicrawlApiSpider(scrapy.Spider):
    name = "Exam_Score_2023"
    allowed_domains = ["api-university-2022.beecost.vn"]
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    fail_time = 1

    def start_requests(self):
        #each student has one id: 01-000002 in which 01 is board of examiner, and 000002 is ordinal number
        #there are 64 board of examiners, start at 1 and end at 65 (exclude 20)
        board_of_examiners = 1
        #this is ordinal number of student
        number = 1
        #if crawling meet 404 over 20 times, it means no more data to crawl, then it moves next board of examiner (eg: 01 -> 02) or end of crawling if board of examiner is over 64
        self.max_fail = 20

        while True:
            #set condition to increase board of exmainer
            if self.fail_time > self.max_fail:
                board_of_examiners+=1
                #due to we set to run 32 requests at the same time. In case one request reach to 20, other requests may have fail_time < 20, so it can continue increasing self.fail_time. This line helps to avoid increasing board_of_examiners multiple times
                self.max_fail += 50
                #condition to end this loop.
                if board_of_examiners > 64:
                    break
                #reset fail_time and ordinal number of student
                self.fail_time = 0
                number = 1
                continue
            #send request, if error will return 404 error. Save it to meta data
            link = f"https://api-university-2022.beecost.vn/university/lookup_examiner?id={board_of_examiners:0>2}{number:0>6}"
            yield scrapy.Request(url=link, callback=self.parse, headers={"User-Agent": self.user_agent}, meta={'handle_httpstatus_list': [404]})
            #increase student id
            number += 1

    def parse(self, response):
        #count fail time
        if response.status == 404:
            self.fail_time += 1
            print(self.fail_time, self.max_fail)
            return None
        #this will return text file in form of json file from API crawling
        html = response.body
        items = json.loads(html)['data']['scores']
        item_key = list(items.keys())

        #get data
        for i in item_key:
            if items[i] == None or i == 'subject_group_score' or items[i] == "":
                items.pop(i)
        #reset fail_time
        self.fail_time = 0
        #reduce max_fail time, from 50 to 20 
        if self.max_fail > 20:
            self.max_fail-=1      
        #retunr data
        yield items