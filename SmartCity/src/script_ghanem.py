'''
Created on 15 déc. 2016

@author: Gihane G
'''
import json
import csv
from pyspark import SparkConf, SparkContext, SQLContext
from _sqlite3 import Row


sparkConf = SparkConf().setAppName("mapreduce").setMaster("local")
sc = SparkContext(conf = sparkConf)

textFile = sc.textFile("C:\Users\Gihane G\workspace\SmartCity\Agenda_culturel_out.csv")

fProfils = "C:\Users\Gihane G\workspace\SmartCity\Profils.csv"
fileprofils = open(fProfils, "rb")
readerP = csv.reader(fileprofils, delimiter=";")

row_ghanem=[]


for row in readerP:
    if row[0]=="ghanem":
        row_ghanem = row
        
    


data = textFile.map(lambda line: line.split(";"))


dataghanem = data.filter(lambda line:   
                          row_ghanem[3] in line[7]
                          or row_ghanem[3] in line[6])


cpt_ghanem = data.filter(lambda line: 
                          row_ghanem[3] in line[7]
                          or row_ghanem[3] in line[6]).count()
                
                


fname = "C:\Users\Gihane G\workspace\SmartCity\ghanem_model.csv"
file = open(fname, "wb")
writer = csv.writer(file, delimiter=";", quoting=csv.QUOTE_NONNUMERIC)



for i in range(0, cpt_ghanem):
    writer.writerow([dataghanem.collect()[i][0], dataghanem.collect()[i][1].encode("utf-8"), dataghanem.collect()[i][2].encode("utf-8"), dataghanem.collect()[i][3].encode("utf-8"), dataghanem.collect()[i][4].encode("utf-8"),  dataghanem.collect()[i][5].encode("utf-8"),   dataghanem.collect()[i][6].encode("utf-8"), dataghanem.collect()[i][7].encode("utf-8"),  dataghanem.collect()[i][8].encode("utf-8"), dataghanem.collect()[i][9].encode("utf-8"), dataghanem.collect()[i][10].encode("utf-8"), 
                     dataghanem.collect()[i][11].encode("utf-8"), dataghanem.collect()[i][12].encode("utf-8"), 
                     dataghanem.collect()[i][13].encode("utf-8"), dataghanem.collect()[i][14].encode("utf-8"), 
                     dataghanem.collect()[i][15].encode("utf-8"),   dataghanem.collect()[i][16].encode("utf-8"), 
                     dataghanem.collect()[i][17].encode("utf-8"),  dataghanem.collect()[i][18].encode("utf-8"),
                     dataghanem.collect()[i][19].encode("utf-8"), dataghanem.collect()[i][20].encode("utf-8"),
                      dataghanem.collect()[i][21].encode("utf-8"), dataghanem.collect()[i][22].encode("utf-8"),
                      dataghanem.collect()[i][23].encode("utf-8"), dataghanem.collect()[i][24].encode("utf-8"), 
                      dataghanem.collect()[i][25].encode("utf-8"),   dataghanem.collect()[i][26].encode("utf-8"),
                     ])
    