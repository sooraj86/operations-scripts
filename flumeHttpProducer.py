import random
import subprocess
import requests
import json
from datetime import datetime
url_flume = 'http://localhost:81'
headers = {'content-type': 'application/json'}

geolist=['7zzzzz','dr5red','dr5ree','dr5ref','dr5reg','dr5reu','dr5rev','dr5rey','dr5rez','dr5rgb','dr5rgc','dr5rkj','dr5rkm','dr5rkn','dr5rkz','dr5rmh','dr5rnm','dr5rsg','dr5rsh','dr5rsj','dr5rsk','dr5rsm','dr5rsn','dr5rsp','dr5rsq','dr5rsr','dr5rss','dr5rsw','dr5rsx','dr5rt8','dr5ru0','dr5ru1','dr5ru2','dr5ru3','dr5ru4','dr5ru5','dr5ru6','dr5ru7','dr5ru8','dr5ru9','dr5rud','dr5rue','dr5rug','dr5ruh','dr5ruj','dr5rum','dr5ruq','dr5rus','dr5rut','dr5ruu','dr5ruv','dr5ruw','dr5rux','dr5ruy','dr5ruz','dr5rv6','dr5rvd','dr5rvh','dr5rvj','dr5rvn','dr5rvp','dr5rvs','dr5ryy','dr5rzj','dr5x0z','dr5x1p','dr5x2c','dr72h8','dr72h9','dr72hb','dr72hc','dr72hd','dr72hf','dr72hg','dr72j0','dr72j2','dr72j3','dr72j5','dr72j6','dr72je','dr72jh','dr72m2']

msglist=[20,30,40,50,60]

while True:
  for i in random.sample(geolist, 50):
     for j in random.sample(msglist, 1):
       while j>0:
           payload = [{"headers":{"topic":"test"},"body":'{"'+str(datetime.now().strftime("%Y-%M-%d,%X"))+','+str(i)+'"}'}]
           msg=str(datetime.now().strftime("%Y-%M-%d,%X"))+","+str(i)
           with requests.Session() as session:
             session.post(url_flume, data=json.dumps(payload),headers=headers)
           j-=1

