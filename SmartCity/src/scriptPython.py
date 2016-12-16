'''
Created on 18 nov. 2016

@author: Gihane G
'''

import csv

myCsvFilePath = "C:\Users\media\Downloads\Agenda_culturel.csv"
myTempOutputFile = myCsvFilePath.replace(".csv", "_out6.csv")

separator = ";"
IndexToDelete = []
ColumnsToDelete = []
ColumnsToDelete.append("Etat")
ColumnsToDelete.append("Genre")
ColumnsToDelete.append("Theme")
ColumnsToDelete.append("Ident Genre")
ColumnsToDelete.append("Jour Min")
ColumnsToDelete.append("Jour Max")
ColumnsToDelete.append("Tarif Mention Gratuit")
ColumnsToDelete.append("Photo 1 Chemin")
ColumnsToDelete.append("Photo 2 Chemin")
ColumnsToDelete.append("Photo 3 Chemin")

for columnToDeleteName in ColumnsToDelete:
    inFile = open(myCsvFilePath)        
    inCSV = csv.reader(inFile, delimiter=separator)
    # On commence par rechercher le numero de la colonne  supprimer
    firstRow = inCSV.next()
    #columnIndex = firstRow.index(columnToDeleteName)
    IndexToDelete.append(firstRow.index(columnToDeleteName))
    inFile.close()
    

inFile = open(myCsvFilePath)        
inCSV = csv.reader(inFile, delimiter=separator)

outFile = file(myTempOutputFile, 'w')        
outCSV = csv.writer(outFile, delimiter=separator, quoting=csv.QUOTE_NONNUMERIC)

for row in inCSV:
    # Ici, petite feinte pk Python copie par reference,donc on prend la valeur de la ligne et non la ligne directement
    currentRow = row[:]
    for columnIndex in IndexToDelete:
        #print columnIndex
        #print currentRow[columnIndex]
        currentRow.pop(columnIndex)
        for i in range(IndexToDelete.index(columnIndex)+1, len(IndexToDelete)):
            if(IndexToDelete[i]>columnIndex):
                IndexToDelete[i]=IndexToDelete[i]-1
    outCSV.writerow(currentRow)
inFile.close()
outFile.close()
    
#outfile=open(myTempOutputFile, 'rb')
#for row in outfile:
    #print row
        

