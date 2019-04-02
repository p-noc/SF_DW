import psycopg2
import csv
import codecs
import time
import datetime
from pathlib import Path
cntNotValidRows=0
cntValidRows=0

def createTables(cur,conn):

    cur.execute("CREATE TABLE IF NOT EXISTS test_dim_duration (idDuration smallint NOT NULL,minutes smallint NOT NULL,lessFive boolean NOT NULL DEFAULT '0',lessFifteen boolean NOT NULL DEFAULT '0',lessTwentyfive boolean NOT NULL DEFAULT '0',moreTwentyfive boolean NOT NULL DEFAULT '0')")
    cur.execute("CREATE TABLE IF NOT EXISTS test_fact(call_num integer NOT NULL,unit_id varchar(20) NOT NULL,received_date timestamp without time zone, onScene_date timestamp without time zone,duration_id smallint,declared_prior varchar(1),final_prior varchar(1))")
    conn.commit()

def getDimensionDurationRow(row, cur, tempTableDurata):
    durata=row[4]
    if (durata not in tempTableDurata.values()):
        tempTableDurata[len(tempTableDurata)] = durata

        #HO CANCELLATO IL SETTAGGIO FLAG QUI !!!!


        #cur.execute("INSERT INTO test_dim_durata VALUES (%s, %s, %s, %s, %s, %s, %s)",(dimRow[0],dimRow[1], dimRow[2], dimRow[3],dimRow[4],dimRow[5],len(tempTableDurata)-1))

    #cur.execute("INSERT INTO test_dim_durata SELECT %s, %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT * FROM test_dim_durata WHERE test_dim_durata.minuti=%s) RETURNING test_dim_durata.id_durata",(dimRow[0],dimRow[1], dimRow[2], dimRow[3],dimRow[4],dimRow[5],dimRow[0]))
    #if (cur.fetchone() is None):
    #    cur.execute("SELECT test_dim_durata.id_durata FROM test_dim_durata WHERE test_dim_durata.minuti=%s",(durata,))

    for idd,dur in tempTableDurata.items():
        if dur==durata:
            return idd

def rowManipulation(row):
    d1 = datetime.datetime.strptime(row[6], "%Y-%m-%dT%H:%M:%S")
    d2 = datetime.datetime.strptime(row[10], "%Y-%m-%dT%H:%M:%S")
    d3 = d2-d1
    d3 =(int(d3.seconds/60))

    manRow=(row[0], row[1], row[6], row[10],d3,row[21],row[23])
    '''
    for i in range(32):
        print(i,row[i])
    print("---")
    '''
    return manRow

def rowValidation(row):
    if row[31]=="None": #Colonna 31: distretto di SF, non può essere None
        return False
    if row[21]=="":     #Colonna 21: priorità, non può essere nulla
        return False
    if row[10]=="":     #Colonna 10: data intervento
        row[10]=row[6]
    if row[10]<row[6]:  #Colonna 6: data arrivo, controlla che le date non siano in conflitto
        return False
    return True

def exportDimensionDurataToCsv(dict, path):
    print(dict)
    with open(path, 'w',newline='') as fl:
        for k,v in dict.items():
            dimRow = [k,v, 0, 0, 0, 0]
            print(dimRow)
            if(v<30):
                print("12<30")

            if (v>25):
                dimRow[5] = 1
            if (v<5):
                dimRow[2] = 1
            if (v<15):
                dimRow[3] = 1
            if (v<25):
                dimRow[4] = 1

            #cur.execute("INSERT INTO test_dim_durata VALUES (%s, %s, %s, %s, %s, %s, %s)", (dimRow[0], dimRow[1], dimRow[2], dimRow[3], dimRow[4], dimRow[5], ))

            fl.write(repr(dimRow[0]) + "," +  repr(dimRow[1]) + "," + repr(dimRow[2]) + "," + repr(dimRow[3]) + "," + repr(dimRow[4]) + "," +  repr(dimRow[5]) +"\n")
    fl.close()

def exportFactToCsv(path, manRow, rowDim):
    with open(path, 'a',newline='') as f2:
        stw = (manRow[0]) + "," + repr(manRow[1]) + "," + repr(manRow[2]) + "," + repr(manRow[3]) + "," + repr(manRow[4]) + "," + ((manRow[5])) + "," + ((manRow[6])) + ("\n")
        f2.write(stw)
    f2.close()

def csvToPostgres(csvPath,tablename,cur,con):
    with open(csvPath, 'r') as f:
        cur.copy_from(f, tablename, sep=',')
    conn.commit()


postgresConnectionString = "dbname=test user=postgres password=1234 host=localhost"
inputCsvPath = Path.cwd() / 'datasource/testPython.csv' #r"\datasource\testPython.csv"
dim_durata_csvPATH = Path.cwd() / 'output/dim_durata.csv' #r"C:\Users\utente\OneDrive\Desktop\BD2\codice\datasource\dim_durata.csv"
fact_csvPATH = Path.cwd() / 'output/fact.csv' #r"C:\Users\utente\OneDrive\Desktop\BD2\codice\fact.csv"

conn = psycopg2.connect(postgresConnectionString)
cur = conn.cursor()

createTables(cur,conn)
tempTableDurata={}

start_time = time.time()
with codecs.open(inputCsvPath, 'rU', 'utf-16-le') as csv_file:
    reader = csv.reader(csv_file)
    cnt = 0
    for row in reader:
        if (cnt != 0):
            valResult=rowValidation(row)
            if valResult:
                cntValidRows=cntValidRows+1
                manRow = rowManipulation(row)
                rowDim=getDimensionDurationRow(manRow, cur, tempTableDurata)
                if rowDim is not None:
                    exportFactToCsv(fact_csvPATH, manRow, rowDim)
                    #cur.execute("INSERT INTO test_fact (call_number, unit_id, rec_date, scene_date, durata_int, or_prio, fin_prio,for_key_durata) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",(manRow[0], manRow[1], manRow[2], manRow[3],manRow[4],manRow[5],manRow[6],rowDim))
            else:
                cntNotValidRows=cntNotValidRows+1
        else:
            cnt = cnt + 1
    exportDimensionDurataToCsv(tempTableDurata, dim_durata_csvPATH)

csvToPostgres(dim_durata_csvPATH,'test_dim_duration',cur,conn)
csvToPostgres(fact_csvPATH,'test_fact',cur,conn)

print("Tempo ETL (sec): %s" % (time.time() - start_time))
print("Righe non valide: %s" % (cntNotValidRows))
print("Righe valide: %s" % (cntValidRows))

'''
class ETL_load:
    def CodeToCSV(self):
        conn = psycopg2.connect(ETL_load.postgresConnectionString)
        cur = conn.cursor()
        with open(ETL_transform.path, 'r') as f:
            cur.copy_from(f, 'test2_table', sep=',')
        conn.commit()

class ETL_transform:
# Classe per processo di trasformazione ETL: si occupa della trasformazione dei dati e
# dell'esportazione degli stessi in formato csv. L'output sarà utilizzato dalla classe ETL_load.
    path = r"D:Università\Basi\Progetto\sf-fire-data-incidents-violations-and-more\output.csv"

    def __init__(self):
        outFile = open(self.path, "w+")
        outFile.close()

    def expManipulatedRow(self,row):
        # Apre in append il file in cui sono conservate le righe manipolate
        with open(self.path, 'a',newline='') as writeFile:
            writer = csv.writer(writeFile)
            writer.writerow(row)
            writeFile.close()
'''

#SELECT test2_table.distretto, COUNT(*) AS cnt FROM test2_table GROUP BY test2_table.distretto ORDER BY 2 DESC