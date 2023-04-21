import csv
import os
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from datetime import datetime
from typing import List

import requests
from bs4 import BeautifulSoup
from retrying import retry


class Task:
    def __init__(self):
        self.file_name_csv = 'Task_1670_csv' #+ '_update'
        self.url = 'http://sousuo.gov.cn/column/'
        self.page_size = 10
        self.retry_times = 1  # 重试次数
        self.fieldnames = ['id', 'cate_id', 'title', 'descript', 'file_wh', 'images', 'content', 'createdate', 'status',
                           'sort', 'type',
                           'attach', 'views', 'systemdate', 'url',
                           'attach_editor', 'laiyuan']

        self.bumen = {
            'code': '30474',
            'cate_id': '71',
            'name': 'task_1670_bumen' #+ '_update',
        }
        self.meiti = {
            'code': '40048',
            'cate_id': '71',
            'name': 'task_1670_meiti' #+ '_update',
        }
        self.zhuanjia = {
            'code': '30593',
            'cate_id': '71',
            'name': 'task_1670_zhuanjia' #+ '_update',
        }
        self.tasks_list = [self.bumen, self.meiti, self.zhuanjia]

    def config_init(self, task: dict):
        task_code = task['code']
        url = f'http://sousuo.gov.cn/column/{task_code}/0.htm'
        total_page = self.get_total_page(url)
        task_name = task['name']

        cp = ConfigParser()
        cp.add_section('db')  # 创建section db
        cp.set('db', 'count', str(total_page))  # 添加 total_page 到 db下
        try:
            os.listdir('./config/')
        except:
            os.makedirs('./config/')
        cp.write(open(f"./config/{task_name}_config.ini", 'w'))  # 写入文件并保存
        # 开始爬取数据
        self.get_csv_data(task, total_page)

    def get_csv_data(self, task, page_index):

        try:
            fw = open(f'./{self.file_name_csv}/{task["name"]}.csv', 'w', encoding='UTF8', newline='')
        except:
            os.makedirs(f'./{self.file_name_csv}/')
            fw = open(f'./{self.file_name_csv}/{task["name"]}.csv', 'w', encoding='UTF8', newline='')

        # f = open('contents_add.csv', 'w', encoding='UTF-8-sig', newline='')
        writer = csv.DictWriter(fw, fieldnames=self.fieldnames)
        writer.writeheader()

        task_code = task['code']
        for i in range(int(page_index)):
            print(f'{task["name"]}正在爬取第{i}页')
            url = f'http://sousuo.gov.cn/column/{task_code}/{i}.htm'
            response = self.get_res(url)
            html = BeautifulSoup(response.text, 'lxml')

            lis = html.select('.listTxt > li')

            for li in lis:
                title = li.select('h4 > a')[0].get_text()
                date = li.select('h4 > span')[0].get_text()
                url = li.select('h4 > a')[0].attrs['href']

                response = self.get_content_res(url)
                response.encoding = response.apparent_encoding
                html = BeautifulSoup(response.text, "lxml")
                try:
                    laiyuan = self.get_laiyuan(html)
                    content = self.get_content(html)
                    rows: List[dict] = [
                        {
                            'id': '',
                            'cate_id': '71',
                            'title': title,
                            'descript': '',
                            'file_wh': '',
                            'images': '',
                            'content': content,
                            'createdate': self.get_timestamp(date),  # 创建时间，取爬取文章中的创建时间，转为时间戳格式
                            'status': 1,
                            'sort': 50,
                            'type': 1,
                            'attach': '',
                            'views': '',
                            'systemdate': '',
                            'url': url,
                            'attach_editor': '',
                            'laiyuan': laiyuan,
                        },
                    ]
                    writer.writerows(rows)
                except:
                    print(f'url:{url}')
                    pass

    # 发起请求并设置重试次数为10次，最小等待时间为5秒，最大为20秒
    @retry(stop_max_attempt_number=50, wait_random_min=10000, wait_random_max=20000)
    def get_res(self, url):
        response = requests.get(url, verify=False)
        return response

    # 发起请求并设置重试次数为10次，最小等待时间为5秒，最大为20秒
    @retry(stop_max_attempt_number=50, wait_random_min=10000, wait_random_max=20000)
    def get_content_res(self, url):
        response = requests.get(url)
        response.encoding = response.apparent_encoding

        if response.status_code != 200:
            print(f'请求出现异常，正在进行第{self.retry_times}次重试')
            self.retry_times += 1
        else:
            if self.retry_times != 1:
                print('可以正常请求了，中途休息20秒')
                time.sleep(20)
            self.retry_times = 1

        return response

    # 获取总页数
    def get_total_page(self, url):
        res = self.get_res(url)
        soup = BeautifulSoup(res.text, 'lxml')
        total_page = soup.select('#toPage > li:nth-child(2)')[0]
        total_page = re.findall(r'\d+', str(total_page))[0]
        return total_page

    # 日期转时间戳
    @staticmethod
    def get_timestamp(date):

        date_list = re.findall(r'\d+', date)
        date_list_len = len(date_list)
        if date_list_len == 1:
            dt = datetime.strptime(date_list[0], '%Y')
        elif date_list_len == 2:
            dt = datetime.strptime(date_list[0] + '-' + date_list[1], '%Y-%m')
        elif date_list_len == 3:
            dt = datetime.strptime(date_list[0] + '-' + date_list[1] + '-' + date_list[2], '%Y-%m-%d')
        try:
            utc_time = datetime.strptime("1970-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')
            met_time = dt - utc_time
            createdate = str(int(met_time.days * 24 * 3600 + met_time.seconds))
        except:
            createdate = '0' * 10

        return createdate

    @staticmethod
    def get_laiyuan(html):
        source = html.select('.article.oneColumn.pub_border > div.pages-date > span')[0].get_text()[
                 3:].strip()
        if source == '府门户网站　www.gov.cn':
            source = html.select('.article.oneColumn.pub_border > div.pages-date > span')[1].get_text()[
                     3:].strip()
        elif source == '中央政府门户网站　www.gov.cn':
            source = html.select('.pages-date > span:nth-child(2)')[1].get_text()[3:].strip()
        return source

    @staticmethod
    def get_content(html):
        content = html.select('.article.oneColumn.pub_border')[0]
        # body > div.wrap > div.main > div.mt15.details - box > div.details - main > h6 > em
        pages_print = content.find_all(class_='pages_print')
        xglist = content.find_all(class_='xg-list related')
        de_bannerS = content.find_all(class_='de_bannerS')
        div_div = content.find_all(id='div_div')
        target = content.find_all(attrs={"target": "_blank"})
        if pages_print:
            pages_print[0].decompose()
        if xglist:
            xglist[0].decompose()
        if de_bannerS:
            de_bannerS[0].decompose()
        if div_div:
            div_div[0].decompose()
        if target:
            target[0].decompose()
        content = str(content).replace(u'\xa0', u'&nbsp;')

        return content

    def to_db(self):
        import pandas as pd
        from sqlalchemy import create_engine
        engine = create_engine('mysql+pymysql://root:123456@localhost:3306/kfzmt')
        listdir = os.listdir(f'./{self.file_name_csv}')
        db_info = {
            'user': 'root',
            'password': '123456',
            'host': 'localhost',
            'port': '3306',
            'database': 'kfzmt',
            'table': 'article_221129'
            # 'table': 'article'
        }
        count = 0
        for ls in listdir:
            df = pd.read_csv(f'./{self.file_name_csv}/{ls}')
            df.to_sql(db_info["table"], con=engine, index=False, if_exists='append')
            print(f'{ls}导入数据库{db_info["database"]}下的表{db_info["table"]}成功{len(df)}条。')
            count += len(df)
        print(f'此次共导入{count}条')


if __name__ == '__main__':
    # 开启多线程
    Task = Task()
    pool = ThreadPoolExecutor(max_workers=30)
    for task in Task.tasks_list:
        pool.submit(Task.config_init, task)

    # pool = ThreadPoolExecutor(max_workers=3)
    # task_list = [pool.submit(Task.config_init, i) for i in Task.tasks_list]
    # for task in task_list:
    #     try:
    #         print(task.result())
    #     except Exception as e:
    #         pass
    #         traceback.print_exc(e)

    # Task.to_db()
