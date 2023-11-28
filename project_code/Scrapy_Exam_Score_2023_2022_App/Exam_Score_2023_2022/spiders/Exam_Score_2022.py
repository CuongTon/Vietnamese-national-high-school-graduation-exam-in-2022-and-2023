import scrapy
import time
class ExamScore2022Spider(scrapy.Spider):
    name = "Exam_Score_2022"
    allowed_domains = ["vietnamnet.vn"]
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    request_count = 1
    max_request_each_time = 1000
    #if crawling meet 404 over 20 times, it means no more data to crawl, then it moves next board of examiner (eg: 01 -> 02) or end of crawling if board of examiner is over 64
    fail_time = 1
    max_fail_time = 20

    def start_requests(self):
        #each student has one id: 01-000002 in which 01 is board of examiner, and 000002 is ordinal number
        #there are 64 board of examiners, start at 1 and end at 65 (exclude 20)
        board_of_examiners = 1
        #this is ordinal number of student
        number = 1
    
        while True:
            #set condition to increase board of exmainer
            if self.fail_time > self.max_fail_time:
                #due to we set to run 32 requests at the same time. In case one request reach to 20, other requests may have fail_time < 20, so it can continue increasing self.fail_time. This line helps to avoid increasing board_of_examiners multiple times
                board_of_examiners += 1
                self.max_fail_time += 50
                #condition to end this loop.
                if board_of_examiners > 64:
                    break
                #reset fail_time and ordinal number of student
                self.fail_time = 0
                number = 1
                continue
            
            #This is a static crawling. To avoid banning from server due to spend large requests in a short time. Each 1000 requests, pause 5 seconds
            if self.request_count > self.max_request_each_time:
                time.sleep(5)
                self.request_count = 0
            #send request, if error will return 404 error. Save it to meta data
            yield scrapy.Request(url=f"https://vietnamnet.vn/giao-duc/diem-thi/tra-cuu-diem-thi-tot-nghiep-thpt/2022/{board_of_examiners:0>2}{number:0>6}.html",
                             callback=self.parse, headers={"User-Agent": self.user_agent}, 
                             meta={"id": f"{board_of_examiners:0>2}{number:0>6}", 'handle_httpstatus_list': [404]})
            number += 1

    def parse(self, response):
        #count fail time
        if response.status == 404:
            self.fail_time += 1
            print(self.fail_time, self.max_fail_time)
            return None
        #this will return text file in form of html file from static website crawling
        item = {}
        item.update({"id": int(response.request.meta["id"])})
        for i in response.xpath("/html/body/div/div[2]/div[3]/div[2]/table/tbody/tr"):
            item.update({i.xpath(".//td[1]/text()").get(): float(i.xpath(".//td[2]/text()").get())})
        #count request that have sent
        self.request_count += 1
        #reset fail_time
        self.fail_time = 0
        #reduce max_fail time, from 50 to 20
        if self.max_fail_time > 20:
            self.max_fail_time -= 1
        #return data
        yield item
