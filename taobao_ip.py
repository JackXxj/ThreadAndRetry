# coding:utf-8
__author__ = 'xxj'

import requests
import time
import sys
import os
import redis
import datetime
import Queue
from Queue import Empty
import threading
from threading import Lock
import csv
import copy
import json
import re
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError

reload(sys)
sys.setdefaultencoding('utf-8')
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
}
KEYWORD_QUEUE = Queue.Queue()    # 关键词队列
PROXY_IP_Q = Queue.Queue()    # 代理队列
THREAD_PROXY_MAP = {}    # 线程与代理对应关系
rc = redis.StrictRedis(host="172.31.10.75", port=9221)


class IpIpException(Exception):
    def __init__(self, message):
        super(IpIpException, self).__init__()
        self.message = message


class IpIp4Exception(Exception):
    def __init__(self, message):
        super(IpIp4Exception, self).__init__()
        self.message = message


def ip2int(lis):
    return int("%02x%02x%02x" % (int(lis[0]), int(lis[1]), int(lis[2])), 16)


def int2ip(num):
    hexIP = str('%06x' % num)
    # print hexIP
    return str("%i.%i.%i" % (int(hexIP[0:2], 16), int(hexIP[2:4], 16), int(hexIP[4:6], 16)))


def get_redis_proxy():
    '''
    从redis相应的key中获取代理ip
    :return:
    '''
    kuai_proxy_length = rc.scard('spider:kuai:proxy')  # 快代理
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中kuai的代理ip长度：', kuai_proxy_length
    if kuai_proxy_length == 0:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中的代理ip数量为0，等待60s'
        time.sleep(60)
        return get_redis_proxy()
    kuai_proxy_set = rc.smembers('spider:kuai:proxy')    # 快代理集合
    for i, ip in enumerate(kuai_proxy_set):
        proxies = {
            'http': "http://{ip}".format(ip=ip),
            'https': "http://{ip}".format(ip=ip)
        }
        PROXY_IP_Q.put(proxies)


def get(url, count, url_desc, proxies, lock, thread_name):
    for i in xrange(count):
        response = r(url, i, url_desc, proxies)
        if response is None:    # 异常情况
            pass
        elif response.status_code == 200:
            print '{time_} 状态码：{status_code}'.format(
                time_=time.strftime('[%Y-%m-%d %H:%M:%S]'),
                status_code=200
            )
            return response
        elif response.status_code != 200:
            print '{time_} 非200状态码：{status_code}'.format(
                time_=time.strftime('[%Y-%m-%d %H:%M:%S]'),
                status_code=response.status_code
            )
        with lock:
            THREAD_PROXY_MAP.pop(thread_name)
            if PROXY_IP_Q.empty():
                get_redis_proxy()
                print '获取到新代理队列中代理ip数量：{}'.format(PROXY_IP_Q.qsize())
            proxies = PROXY_IP_Q.get(False)
            print '新的代理IP：{}'.format(proxies)
            THREAD_PROXY_MAP[thread_name] = proxies
    return None


def r(url, i, url_desc, proxies):
    try:
        print '{time_} {url_desc}{url} 代理proxies：{proxies} count：{count}'.format(
            time_=time.strftime('[%Y-%m-%d %H:%M:%S]'),
            url_desc=url_desc,
            url=url,
            proxies=proxies,
            count=i
        )
        response = requests.get(url=url, headers=headers, proxies=proxies, timeout=5)
    except BaseException as e:
        print '{time_} BaseException：{e} url：{url}'.format(
            time_=time.strftime('[%Y-%m-%d %H:%M:%S]'),
            e=e,
            url=url
        )
        response = None
    return response


def taobao_ip(lock, fileout):
    while not KEYWORD_QUEUE.empty():
        try:
            thread_name = threading.currentThread().name  # 当前线程名
            if not THREAD_PROXY_MAP.get(thread_name):
                THREAD_PROXY_MAP[thread_name] = PROXY_IP_Q.get(False)
            proxies = THREAD_PROXY_MAP.get(thread_name)

            line = KEYWORD_QUEUE.get(False)
            if len(line) == 8:
                ip_start = line[0]    # ip起始段
                ip_end = line[1]    # ip结束段

                # con_start = taobao_req(ip_start, proxies, '起始ip')    # taobao_ip接口
                con_start = taobao_req(ip_start, proxies, lock, thread_name, '起始ip')    # taobao_ip接口
                if con_start is None:    # 针对重试10次未成功
                    continue
                # con_end = taobao_req(ip_end, proxies, '结束ip')    # taobao_ip接口
                con_end = taobao_req(ip_end, proxies, lock, thread_name, '结束ip')    # taobao_ip接口
                if con_end is None:
                    continue
                if con_start == con_end:
                    con_tmp = '\t{}'.format(con_start)
                    line.append(con_tmp)
                    content = '\t'.join(line)
                    data_write_file(fileout, lock, content)    # 写文件接口
                else:    # 起始ip和结束ip获取到的结果不一致
                    ip_start_ls = ip_start.split('.')
                    ip_end_ls = ip_end.split('.')
                    if ip_start_ls[0] != ip_end_ls[0]:
                        print '直接丢弃的ip_start：{ip_start}, ip_end：{ip_end}'.format(ip_start=ip_start, ip_end=ip_end)

                    # 方案：对ip字段的二、三段进行循环，末尾段固定为0
                    elif (ip_start_ls[1] != ip_end_ls[1]) or (ip_start_ls[2] != ip_end_ls[2]):
                        ip_start_ls = ip_start_ls[:-1]
                        # print 'ip_start_ls：', ip_start_ls
                        ip_end_ls = ip_end_ls[:-1]
                        # print 'ip_end_ls：', ip_end_ls
                        ip_ls = [int2ip(x) for x in range(ip2int(ip_start_ls), ip2int(ip_end_ls) + 1)]
                        for ip in ip_ls:
                            # print(ip)
                            line1 = copy.deepcopy(line)
                            ip = ip + '.0'
                            line1.append(ip)
                            KEYWORD_QUEUE.put(line1)

                    elif ip_start_ls[-1] != ip_end_ls[-1]:    # 对于该情况就抓取起始的那个ip即可
                        line.extend([ip_start, con_start])
                        content = '\t'.join(line)
                        data_write_file(fileout, lock, content)  # 写文件接口

            elif len(line) == 9:    # 针对起始ip和末尾ip不同的情况下
                ip = line[-1]
                # con = taobao_req(ip, proxies, '首尾不同ip')    # taobao_ip接口
                con = taobao_req(ip, proxies, lock, thread_name, '首尾不同ip')    # taobao_ip接口
                # print '首尾不同的ip,写入文件'
                if con is None:
                    continue
                line.append(con)
                content = '\t'.join(line)
                data_write_file(fileout, lock, content)  # 写文件接口

            else:
                print '异常数据源，{}'.format(line)

        except Empty:
            pass

        except BaseException as e:
            with lock:
                print '{time_} {desc} {line} meassage：{message}'.format(time_=time.strftime('[%Y-%m-%d %H:%M:%S]'),
                                                                         desc='taobao_ip索引页异常BaseException',
                                                                         line=line, message=e)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                print '新的代理IP：', proxies
                THREAD_PROXY_MAP[thread_name] = proxies


def taobao_req(ip, proxies, lock, thread_name, desc):
    """
    淘宝接口请求
    :param ip: 源数据ip
    :param proxies: 代理ip
    :param lock: 锁
    :param thread_name: 线程名
    :param desc: 请求描述
    :return:ip接口信息
    """
    url = 'http://ip.taobao.com/service/getIpInfo.php?ip={}'.format(ip)
    response = get(url, 10, '淘宝ip网{}接口url：'.format(desc), proxies, lock, thread_name)
    if response is not None:
        response_json = response.json()
        code = response_json.get('code')  # 接口返回状态码(0或1)
        # print '接口返回状态码：', code
        if code == 0:  # 0表示成功
            data = response_json.get('data')
            country = data.get('country')    # 国家
            # print '国家：', country
            region = data.get('region')  # 省份
            # print '省份：', region
            city = data.get('city')  # 城市
            # print '城市：', city
            isp = data.get('isp')  # 运营商
            # print '运营商：', isp
            ip_con = '{country} {region} {city} {isp}'.format(country=country, region=region, city=city, isp=isp)
            return ip_con

        else:  # 非0表示失败
            print '接口请求返回失败code：{code} ip：{ip}'.format(code=code, ip=ip)

    else:
        print 'ip：{} 接口请求的response is None'.format(ip)


def data_write_file(fileout, lock, content):
    with lock:
        fileout.write(content)
        fileout.write('\n')
        fileout.flush()


def main():
    lock = Lock()
    file_date = time.strftime('%Y%m')
    csv_file_dir = r'/ftp_samba/cephfs/spider/python/db_ip/db_ip_csv/'
    csv_files = os.listdir(csv_file_dir)
    csv_file = csv_files[0]  # csv文件名
    csv_file_path = os.path.join(csv_file_dir, csv_file)  # csv文件路径
    csv_file_obj = csv.reader(open(csv_file_path, 'r'))
    for index, line in enumerate(csv_file_obj):  # 行遍历
        c1 = line[0]
        c4 = line[3]
        if c4 in ['CN', 'HK', 'KR']:
            # print line
            if ':' not in c1:  # 清除ipv6的源数据
                KEYWORD_QUEUE.put(line)
    print '数据来源关键词的数量：', KEYWORD_QUEUE.qsize()

    get_redis_proxy()
    ip_num = PROXY_IP_Q.qsize()
    print '获取到的代理ip数量：', ip_num

    dest_path = '/ftp_samba/cephfs/spider/python/ip/db_ip/taobao/'  # linux上的文件目录
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'taobao_ip_' + file_date)
    tmp_file_name = os.path.join(dest_path, 'taobao_ip_' + file_date + '.tmp')
    fileout = open(tmp_file_name, 'a')

    threads = []
    for i in xrange(20):
        t = threading.Thread(target=taobao_ip, args=(lock, fileout))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    try:
        fileout.flush()
        fileout.close()
    except IOError as e:
        time.sleep(1)
        fileout.close()

    os.rename(tmp_file_name, dest_file_name)


if __name__ == '__main__':
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    main()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end'
