from hdfs import *
import os

client = Client("http://localhost:50070")
def ReadData():
    client = Client("http://localhost:50070")
    file_user_movie="/ml-100k/u.data"
    file_movie_info="/ml-100k/u.item"
    user_information = "/ml-100k/u.user"

    try:
        # client.download(file_user_movie,"D://VSC//vscode python//hadoop//ml-100k")
        client.download(file_user_movie,"D:\研一下课程\大数据专题\电影推荐\ml-100k")
        file_user_movie="D:/研一下课程/大数据专题/电影推荐/ml-100k/u.data"
    except:
        os.remove("D:/研一下课程/大数据专题/电影推荐/ml-100k/u.data")
        client.download(file_user_movie,"D:\研一下课程\大数据专题\电影推荐\ml-100k")
    try:
        client.download(file_movie_info,"D:\研一下课程\大数据专题\电影推荐\ml-100k")
        file_movie_info="D:/研一下课程/大数据专题/电影推荐/ml-100k/u.item"
    except:
        os.remove("D:/研一下课程/大数据专题/电影推荐/ml-100k/u.item")
        client.download(file_movie_info,"D:\研一下课程\大数据专题\电影推荐\ml-100k")
    try:
        client.download(user_information,"D:\研一下课程\大数据专题\电影推荐\ml-100k")
        user_information="D:/研一下课程/大数据专题/电影推荐/ml-100k/u.user"
    except:
        os.remove("D:/研一下课程/大数据专题/电影推荐/ml-100k/u.user")
        client.download(user_information,"D:\研一下课程\大数据专题\电影推荐\ml-100k")

if __name__ == "__main__":
        ReadData()