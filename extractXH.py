import requests
import json
import psycopg2
import schedule
from datetime import datetime
requests.packages.urllib3.disable_warnings()
def insert_data():
    request = requests.get('https://xinghe.starsee.cn/api/search/getMilitaryDynamics?pageIndex=0&pageSize=50&configId=',verify=False)

    data = json.loads(request.text)
    infos = data["result"]["data"]

    con = psycopg2.connect(database ="postgres",user ="postgres" ,password="123456",host="localhost",port ="5432")
    cur = con.cursor()
    num = 0
    for info in infos:
        id = info["id"]
        if 'spiderId' in info:
            spiderId = info["spiderId"]
        else:
            spiderId = ""
        if 'mediaType' in info:
            mediaType = info["mediaType"]
        else:
            mediaType = ""
        if 'country' in info:
            country = info["country"]
        else:
            country = ""
        if 'originalPublishTime' in info:
            originalPublishTime = info["originalPublishTime"]
        else:
            originalPublishTime = ""

        if 'userId' in info:
            userId = info["userId"]
        else:
            userId = ""
        if 'username' in info:
            username = info["username"]
        else:
            username = ""
        if 'name' in info:
            name = info["name"]
        else:
            name = ""
        if 'tweet' in info:
            tweet = info["tweet"]
        else:
            tweet = ""
        if 'zhTweet' in info:
            zhTweet = info["zhTweet"]
        else:
            zhTweet = ""
        if 'language' in info:
            language = info["language"]
        else:
            language = ""
        if 'url' in info:
            url = info["url"]
        else:
            url = ""
        if 'urlsStr' in info:
            urlsStr = info["urlsStr"]
        else:
            urlsStr = ""

        if 'photosStr' in info:
            photosStr = info["photosStr"]
        else:
            photosStr = ""
        if 'urls' in info:
            urls = info["urls"]
        else:
            urls = ""

        if 'rowKey' in info:
            rowKey = info["rowKey"]
        else:
            rowKey = ""

        if 'type' in info:
            type = info["type"]
        else:
            type = ""

        if 'score' in info:
            score = info["score"]
        else:
            score = ""

        if 'creationTime' in info:
            creationTime = info["creationTime"]
        else:
            creationTime = ""

        if 'collect' in info:
            collect = info["collect"]
        else:
            collect = False

        if 'follow' in info:
            follow = info["follow"]
        else:
            follow = False

        if 'likeNum' in info:
            likeNum = info["likeNum"]
        else:
            likeNum = ""

        if 'collectNum' in info:
            collectNum = info["collectNum"]
        else:
            collectNum = ""

        if 'commentNum' in info:
            commentNum = info["commentNum"]
        else:
            commentNum = ""
        if "innerNews" in info:
            innerNews = str(info["innerNews"])
        else:
            innerNews = ""
        if "label" in info:
            label = str(info["label"])
        else:
            label = ""
        if "topicLabel" in info:
            topicLabel = str(info["topicLabel"])
        else:
            topicLabel = ""
        if "eventLabel" in info:
            eventLabel = str(info["eventLabel"])
        else:
            eventLabel = ""
        if "zhContent" in info:
            zhcontent = info["zhContent"]
        else:
            zhcontent = ""
        if "zhtitle" in info:
            zhtitle = info["zhtitle"]
        else:
            zhtitle = ""
        try:
            sql = """INSERT INTO test (id,spiderid,mediatype,country,originalpublishtime,userid,username,name,tweet,zhtweet,zhtitle,zhcontent,language,url,urlsstr,photosstr,urls,rowkey,type,score,creationtime,collect,follow,likenum,collectnum,commentnum,innernews,label,topiclabel,eventlabel) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            params = (id,spiderId,mediaType,country,originalPublishTime,userId,username,name,tweet,zhTweet,zhtitle,zhcontent,language,url,urlsStr,photosStr,urls,rowKey,type,score,creationTime,collect,follow,likeNum,collectNum,commentNum,innerNews,label,topicLabel,eventLabel)
            cur.execute(sql,params)
            num = num + 1
            #print("insert "+ str(num))
            con.commit()
        except Exception as e:
            msg = e.args
            if "唯一约束" in msg[0] :
                con.rollback()
                continue
            else:
                print(e)

    con.commit()
    con.close()
    t2 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(t2+'\n'+"successfully "+str(num))

schedule.every(10).minutes.do(insert_data)
while True:
    schedule.run_pending()
#insert_data()