'''
Created on 18 nov. 2016

@author: Gihane G
'''
import json
import csv
from pyspark import SparkConf, SparkContext, SQLContext
from _sqlite3 import Row

# encoding=utf8

sparkConf = SparkConf().setAppName("mapreduce").setMaster("local")
sc = SparkContext(conf = sparkConf)

textFile = sc.textFile("C:\Users\Gihane G\workspace\SmartCity\Agenda_culturel_out.csv")

fProfils = "C:\Users\Gihane G\workspace\SmartCity\Profils.csv"
fileprofils = open(fProfils, "rb")
readerP = csv.reader(fileprofils, delimiter=";")

row_ghanem=[]
row_nouri=[]
row_bahssou=[]
row_ftouhi=[]


for row in readerP:
    if row[0]=="ghanem":
        row_ghanem = row
    if row[0]=="nouri":
        row_nouri = row
    if row[0]=="bahssou":
        row_bahssou = row
    if row[0]=="ftouhi":
        row_ftouhi = row
        
    


data = textFile.map(lambda line: line.split(";"))



                         
datanouri = data.filter(lambda line: row_nouri[3] in line[7]
                           or row_nouri[3] in line[6])

cpt_nouri = data.filter(lambda line: row_nouri[3] in line[7]
                           or row_nouri[3] in line[6]).count()
                           




fname1 = "C:\Users\Gihane G\workspace\SmartCity\model_nouri.csv"
file1 = open(fname1, "wb")
writer1 = csv.writer(file1, delimiter=";", quoting=csv.QUOTE_NONNUMERIC)


for i in range(0, cpt_nouri):

    writer1.writerow([datanouri.collect()[i][0], datanouri.collect()[i][1].encode("utf-8"), datanouri.collect()[i][2].encode("utf-8"), datanouri.collect()[i][3].encode("utf-8"), datanouri.collect()[i][4].encode("utf-8"),  datanouri.collect()[i][5].encode("utf-8"),   datanouri.collect()[i][6].encode("utf-8"), datanouri.collect()[i][7].encode("utf-8"),  datanouri.collect()[i][8].encode("utf-8"), datanouri.collect()[i][9].encode("utf-8"), datanouri.collect()[i][10].encode("utf-8"), 
                     datanouri.collect()[i][11].encode("utf-8"), datanouri.collect()[i][12].encode("utf-8"), 
                     datanouri.collect()[i][13].encode("utf-8"), datanouri.collect()[i][14].encode("utf-8"), 
                     datanouri.collect()[i][15].encode("utf-8"),   datanouri.collect()[i][16].encode("utf-8"), 
                     datanouri.collect()[i][17].encode("utf-8"),  datanouri.collect()[i][18].encode("utf-8"),
                     datanouri.collect()[i][19].encode("utf-8"), datanouri.collect()[i][20].encode("utf-8"),
                      datanouri.collect()[i][21].encode("utf-8"), datanouri.collect()[i][22].encode("utf-8"),
                      datanouri.collect()[i][23].encode("utf-8"), datanouri.collect()[i][24].encode("utf-8"), 
                      datanouri.collect()[i][25].encode("utf-8"),   datanouri.collect()[i][26].encode("utf-8"),
                     ])
    
    




#print datanouri.collect()

#print cpt_ghanem


