import requests
import time
import json

from pymysql import ProgrammingError

from thread_spider.UserAgent import get_UserAgent
from urllib import parse
from queue import Queue
from threading import Thread, Lock
import pprint
import pymysql


class TecentJobs_spider(object):
    pass

    # 初始化函数
    # 定义一二级页面URL格式
    # 定义headers
    # 定义url队列
    # 定义一个整数用于记录工作的个数，并赋予锁
    def __init__(self):
        self.index_url = "https://careers.tencent.com/tencentcareer/api/post/Query?timestamp=1582079472895&keyword={" \
                         "}&pageIndex={}&pageSize=10 "
        self.second_url = "https://careers.tencent.com/tencentcareer/api/post/ByPostId?timestamp=1582086419823&postId" \
                          "={}&language=zh-cn "
        self.headers = {
            'User-Agent': get_UserAgent(),
        }
        self.q1 = Queue()
        self.q2 = Queue()
        self.q3 = Queue()  # 存储数据库记录
        self.job_count = 0
        self.lock = Lock()

        self.conn = pymysql.connect(host='localhost', user='root', password='123456', port=3306, db='spider')
        self.cursor = self.conn.cursor()
        # sql = 'INSERT INTO students(id, name, age) values (%s, %s, %s)'

    # 功能函数：调用requests请求页面
    def get_html(self, url):
        json_data = requests.get(url=url, headers=self.headers).text
        return json_data

    # 功能函数：获取工作总页数
    def get_page_count(self, keyword):
        # keyword是工作的关键字，1代表仅请求第一页（第一页就有总的个数）
        url = self.index_url.format(keyword, 1)
        json_data = self.get_html(url)
        data = json.loads(json_data)
        count = data["Data"]['Count']
        page_count = 0
        # pageSize=10 每页有10条工作信息，除以10获取到总的页数
        if count % 10 == 0:
            page_count = count // 10
        else:
            page_count = count // 10 + 1
        return page_count

    # 功能函数：将url放入队列（一级url）
    def url_in_Queue(self, keyword):
        count = self.get_page_count(keyword)
        for i in range(1, count + 1):
            url = self.index_url.format(keyword, i)
            self.q1.put(url)

    # 核心函数：解析一级url，获取二级url并放入队列
    def parse_index_url(self):
        # 死循环用于阻止线程阻塞
        while True:
            if not self.q1.empty():
                url = self.q1.get()
                data = self.get_html(url)
                json_list = json.loads(data)
                for record in json_list['Data']['Posts']:
                    post_id = record['PostId']
                    # 格式化二级url
                    url = self.second_url.format(post_id)
                    self.q2.put(url)
                print("url=>", url)
            else:
                print('Q1 break')
                break

    def parse_second_url(self):
        # 死循环用于阻止线程阻塞
        while True:
            #
            if not self.q2.empty() or not self.q1.empty():
                url = self.q2.get()
                html = self.get_html(url)
                json_list = json.loads(html)
                items = {}
                # 封装数据
                items["RecruitPostName"] = json_list['Data']['RecruitPostName']
                items["LocationName"] = json_list['Data']['LocationName']
                items["Responsibility"] = json_list['Data']['Responsibility']
                items['LastUpdateTime'] = json_list['Data']['LastUpdateTime']
                self.q3.put(items)
                print('Q2 consuming...', end='\n')
                # 加锁（防止线程争抢资源），找到一个工作总数加1
                self.lock.acquire()
                self.job_count += 1
                self.lock.release()

            else:
                print('Q2 break')
                break

    def save_2_db(self):
        # 死循环用于阻止线程阻塞
        while True:
            if not self.q3.empty() or not self.q1.empty() or not self.q2.empty():
                record = self.q3.get()
                sql = '''INSERT INTO tencent_jobs(
                recurit_post_name, 
                location_name, 
                responsibility,
                last_update_time)
                 values ("{}", "{}", "{}","{}")''' \
                    .format(
                    record["RecruitPostName"],
                    record["LocationName"],
                    record["Responsibility"],
                    record['LastUpdateTime']
                )
                print('Q3 consuming...', end='\n')
                try:
                    self.cursor.execute(sql)
                    self.conn.commit()
                except Exception as e:
                    pass

            else:
                print('Q3 break')
                break

    # 执行函数：程序执行的入口
    def run(self):
        # 获取职位关键词
        keyword = input("enter the keyword:")
        keyword = parse.quote(keyword)
        # 调用入队函数，把一级页面都放入队列
        self.url_in_Queue(keyword)
        t1_list = []
        t2_list = []
        t3_list = []
        print(self.q1.empty())
        # 开启1个线程用于抓取一级页面的url
        for i in range(1):
            t = Thread(target=self.parse_index_url)
            t1_list.append(t)
            t.start()
            # time.sleep(1)

        # 开启2个线程用于抓取二级页面的url，缩短抓取时间
        for j in range(2):
            t = Thread(target=self.parse_second_url)
            t2_list.append(t)
            t.start()

        # 开启2个线程用于存储记录到数据库
        for k in range(2):
            t = Thread(target=self.save_2_db)
            t3_list.append(t)
            t.start()

        # 阻塞线程
        for t in t1_list:
            t.join()

        for t in t2_list:
            t.join()

        for t in t3_list:
            t.join()

        self.conn.close()
        print("=" * 40)



if __name__ == '__main__':
    start_time = time.time()
    spider = TecentJobs_spider()
    spider.run()
    end_time = time.time()
    print("耗时: %.2f" % (end_time - start_time))
    print("职位数量: %d " % spider.job_count)
