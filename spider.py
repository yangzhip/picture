import requests
from bs4 import BeautifulSoup
import json
import re
import codecs
import time
import os
# headers = {
#     'Cookie':'xxxxxxxx',
#     'Host':'movie.douban.com',
#     'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:69.0) Gecko/20100101 Firefox/69.0',
# }
#
# #获取页面函数
# def get_html(url):
#     try:
#         r = requests.get(url,headers = headers) #访问页面
#     except Exception as e :
#         print("失败服务器返回：%s" % e)
#         return
#     return(r.text)#返回页面文本
# #获取影片列表函数
# def get_movies(html):
#     result=json.loads(html)['data'] #数据json化
#     return(result) #返回影片列表
# #获取影片详情页函数
# def get_movie_info(movies):
#     for movie in movies: #遍历影片列表
#         html = get_html(movie['url'])   #通过每个影片的url访问页面
#         soup = BeautifulSoup(html,'lxml') #BeautifulSoup解析页面
#         movie_info = soup.find('div',attrs={'id':'info'}) #通过特定标签，过滤出影片信息
#         movie_info_text = movie_info.get_text() #获取影片信息文本
#         #过滤导演、主演、语言、上映时间、类型等数据
#         directed =    "".join([str(x) for x in re.findall('导演: (.*)',movie_info_text)]).replace(" / ", ";")
#         language =    "".join([str(x) for x in re.findall('语言: (.*)',movie_info_text)]).replace(" / ", ";")
#         releaseDate = "".join([str(x) for x in re.findall('上映日期: (.*)',movie_info_text)]).replace(" / ", ";")
#         genre =       "".join([str(x) for x in re.findall('类型: (.*)',movie_info_text)]).replace(" / ", ";")
#         #拼凑数据
#         movie = "%s|%s|%s|%s|%s|%s|%s\n" % (movie['title'],movie['rate'],genre,directed,language,releaseDate,movie['url'])
#         movie = str(movie)
#         print(movie)
#         #保存到文件中
#         file_object.write(movie)
#         time.sleep(1)
# #主函数由于豆瓣反爬机制，目前通过指定start和end值，来爬取电影数据
# def movies_down(start,end):
#     for page in range(start,end,20):#访问的url每次返回20条电影数据页面
#         url = 'https://movie.douban.com/j/new_search_subjects?sort=T&range=0,10&tags=%E7%94%B5%E5%BD%B1&start='
#         url = url+str(page) #拼凑url
#         html = get_html(url) #调用页面函数
#         movies = get_movies(html) #调用影片列表函数
#         #print(movies)
#         get_movie_info(movies) #调用详情页函数
# #定义文件句柄
# file_object = codecs.open('douban_movies.txt', 'a' ,"utf-8")
# #调用主函数，需要指定爬取的个数范围
# movies_down(1,1000)
# #关闭文件句柄
# file_object.close()
os.system("/usr/local/hadoop/bin/hdfs dfs -put /home/student/PycharmProjects/spark/test/douban_movies.txt /home/spark-test/")
