import matplotlib.pyplot as plt
from matplotlib import font_manager as fm
import pandas as pd
import numpy as np
from  matplotlib import cm
plt.rcParams['font.sans-serif'] = ['SimHei']
from hdfs.client import Client
lines = []
lines1 = []
lines2 = []
lines3 = []
lines4 = []
client = Client("http://222.27.166.215:50070")
a= 0
b=0
c=0
d=0
e=0
f=0
g=0
h=0

############   tiaoxingtu ###############
with client.read("/home/spark-test/picture_data/part-00000", encoding='utf-8') as reader:
    for line in reader:
        line = line.replace("'","")
        line = line.replace("(", "")
        line = line.replace(")", "")
        lines.append(line.split(","))
df = pd.DataFrame(data=lines)
plt.figure(figsize=(10,15))
df[1]=df[1].astype(int)
for i in range(0, len(df)):
    if float(df.iloc[i][0]) < 4:
        g = g+df.iloc[i][1]
    if float(df.iloc[i][0])>=4 and float(df.iloc[i][0])<5:
        a=a+df.iloc[i][1]
    if float(df.iloc[i][0]) >= 5 and float(df.iloc[i][0])  < 6:
        b = b + int(df.iloc[i][1])
    if float(df.iloc[i][0]) >= 6 and float(df.iloc[i][0]) <7:
        c = c + df.iloc[i][1]
    if float(df.iloc[i][0]) >= 7 and float(df.iloc[i][0]) <8:
        h = h + df.iloc[i][1]
    if float(df.iloc[i][0]) >=8 and float(df.iloc[i][0]) <9:
        d = d + df.iloc[i][1]
    if float(df.iloc[i][0]) >=9and float(df.iloc[i][0]) <10:
        e = e + df.iloc[i][1]
lines4.append(g)
lines4.append(a)
lines4.append(b)
lines4.append(c)
lines4.append(h)
lines4.append(d)
lines4.append(e)
size= []
size.append("<4")
size.append("4-5")
size.append("5-6")
size.append("6-7")
size.append("7-8")
size.append("8-9")
size.append(">9")
# df[1]=df[1].astype(int)
lines4=np.array(lines4)
plt.bar(size,lines4)
plt.title("评分分布")
plt.xlabel("分数范围")
plt.ylabel("数量")
plt.savefig("pingfen.png")
plt.show()
############## ju ti ping fen fen bu #########################
plt.figure(figsize=(10,15))
df[1]=df[1].astype(int)
plt.barh(df[0],df[1],color="orange")
plt.xlabel("分数")
plt.ylabel("数量")
plt.savefig("tiaoxing1.png")
plt.show()
# ############   bing tu ################3
with client.read("/home/spark-test/picture_data1/part-00000", encoding='utf-8') as reader:
    for line in reader:
        line = line.replace("'","")
        line = line.replace("(", "")
        line = line.replace(")", "")
        lines1.append(line.split(","))
        # print(lines)
df1 = pd.DataFrame(data=lines1)
from matplotlib import font_manager as fm
from  matplotlib import cm

labels = df1[0]
sizes = df1[1]
explode = []
for i in range(len(sizes)):
    explode.append(0)
    if labels[i]=="剧情":
        explode[i]=0.05
    if labels[i]=="喜剧":
        explode[i]=0.05

fig, axes = plt.subplots(figsize=(12,12),ncols=2) # 设置绘图区域大小
ax1, ax2 = axes.ravel()

colors = cm.rainbow(np.arange(len(sizes))/len(sizes)) # colormaps: Paired, autumn, rainbow, gray,spring,Darks
patches, texts, autotexts = ax1.pie(sizes, labels=labels, explode=explode,autopct='%1.0f%%',
        shadow=False, startangle=170, colors=colors, labeldistance=1.2,pctdistance=1.03, radius=0.4)
# labeldistance: 控制labels显示的位置
# pctdistance: 控制百分比显示的位置
# radius: 控制切片突出的距离
ax1.axis('equal')
# 重新设置字体大小
proptease = fm.FontProperties()
proptease.set_size('small')
# font size include: ‘xx-small’,x-small’,'small’,'medium’,‘large’,‘x-large’,‘xx-large’ or number, e.g. '12'
plt.setp(autotexts, fontproperties=proptease)
plt.setp(texts, fontproperties=proptease)

ax1.set_title('各种类型电影所占比例', loc='center')

# ax2 只显示图例（legend）
ax2.axis('off')
ax2.legend(patches, labels, loc='center left')

plt.tight_layout()
# plt.savefig("pie_shape_ufo.png", bbox_inches='tight')
plt.savefig('bingtu.png')
plt.show()


