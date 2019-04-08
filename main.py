import psycopg2
import csv
import codecs
import time
import datetime
from pathlib import Path
cntNotValidRows=0
cntValidRows=0

def createTables(cur,conn):
    cur.execute("CREATE TABLE IF NOT EXISTS test_dim_received_date(rec_date_id integer NOT NULL,datetime timestamp without time zone, hour_f smallint, day_f smallint, month_f smallint, year_f smallint, season smallint)")
    cur.execute("CREATE TABLE IF NOT EXISTS test_dim_duration (idDuration smallint NOT NULL,minutes smallint NOT NULL,lessFive boolean NOT NULL DEFAULT '0',lessFifteen boolean NOT NULL DEFAULT '0',lessTwentyfive boolean NOT NULL DEFAULT '0',moreTwentyfive boolean NOT NULL DEFAULT '0')")
    cur.execute("CREATE TABLE IF NOT EXISTS test_fact(id_date integer, id_duration smallint, id_location smallint, call_num integer NOT NULL,unit_id varchar(20) NOT NULL,onScene_date timestamp without time zone,declared_prior varchar(1),final_prior varchar(1))")
    cur.execute("CREATE TABLE IF NOT EXISTS test_dim_location(rec_date_location smallint NOT NULL,address varchar(100),city varchar(50),zipcode integer,Neighborhooods varchar(50))")

    conn.commit()

def putDurationTableInDictionary(dict):
    cur.execute("SELECT test_dim_duration.idDuration, test_dim_duration.minutes FROM test_dim_duration")
    queryRes=cur.fetchall()
    for k in queryRes:
        dict[k[0]]=k[1]
    if len(queryRes)>0:
        return queryRes[len(queryRes)-1][0]
    else:
        return 0

def putLocationTableInDictionary(dict):
    cur.execute("SELECT * FROM test_dim_location")
    queryRes=cur.fetchall()

    for k in queryRes:
        locationString=k[1]+"@"+k[2]+"@"+str(k[3])+"@"+k[4]
        dict[locationString]=k[0]
    if len(queryRes)>0:
        return queryRes[len(queryRes)-1][0]
    else:
        return 0


def putDateTableInDictionary(dict):
    cur.execute("SELECT test_dim_received_date.rec_date_id, test_dim_received_date.datetime FROM test_dim_received_date")
    queryRes=cur.fetchall()

    for k,j in queryRes:
        dict[j.strftime("%Y-%m-%dT%H:%M:%S")]=k

    if len(queryRes) > 0:
        return queryRes[len(queryRes) - 1][0]
    else:
        return 0

def getDimensionDurationRow(duration, tempTableDurata):
    if (duration not in tempTableDurata.values()):
        tempTableDurata[len(tempTableDurata)] = duration

    for idd,dur in tempTableDurata.items():
        if dur==duration:
            return idd

def getDimensionDateRow(recDate,tempTableDate):
    res=tempTableDate.get(recDate)
    if res is None:
        tempTableDate[recDate]=len(tempTableDate)
        return len(tempTableDate)-1
    else:
        return res

def getDimensionLocationRow(address,city,zipcode,neigh,tempTableLocation):
    dimensionString=address + "@" + city + "@" + zipcode + "@" + neigh
    id=tempTableLocation.get(dimensionString)
    if (id is None):
        tempTableLocation[dimensionString] = len(tempTableLocation)
        return len(tempTableLocation)-1
    else:
        return id

    '''
    if (address not in tempTableLocation.values()):
        tempTableLocation[len(tempTableLocation)] = [address,city,zipcode,neigh]

    for idd, loc in tempTableLocation.items():
        if loc == address:
            return idd
    '''
#convert unknown priority values to known ones (2 non-emergency,3 emergency)
def mapPriority(priority):
    # Priority levels:
    # A,B,C | 2 (driving without lights/sirens)
    # D,E   | 3 (driving with lights/sirens)
    # I     | 1
    if (priority=="A") or (priority=="B" or (priority=="C") or (priority=="2")):
        return 2
    elif (priority=="D") or (priority=="E") or (priority=="3"):
        return 3
    else: #(priority=="1") or (priority=="I"):
        return 1

def rowManipulation(row):
    #Evaluate intervention duration
    d1 = datetime.datetime.strptime(row[6], "%Y-%m-%dT%H:%M:%S")
    d2 = datetime.datetime.strptime(row[10], "%Y-%m-%dT%H:%M:%S")
    durationInMinutes = d2-d1
    durationInMinutes =(int(durationInMinutes.seconds/60))

    #convert priority to a known readable format

    call_number=row[0]
    unit_id=row[1]
    incident_number=row[2]
    call_type=row[3]
    call_date=row[4]
    watch_date=row[5]
    received_dtTm=row[6]
    entry_dtTm=row[7]
    dispatch_dtTm=row[8]
    response_dtTm=row[9]
    on_scene_dtTm=row[10]
    transport_dtTm=row[11]
    hospital_dtTm=row[12]
    call_final_disposition=row[13]
    available_dtTm=row[14]
    address=row[15].replace(',','_')
    city=row[16]
    zipcode=row[17]
    battalion=row[18]
    station_area=row[19]
    box=row[20]
    origPriorityMapped=mapPriority(row[21])
    callPriorityMapped=mapPriority(row[22])
    finalPriorityMapped=mapPriority(row[23])
    als_unit=row[24]
    call_type_group=row[25]
    number_of_alarms=row[26]
    unit_type=row[27]
    unit_sequence_call_dispatch=row[28]
    fire_prevention_district=row[29]
    supervisor_district=row[30]
    neighborhood=row[31]
    location=row[32]
    rowid=row[33]


    #create the fact row
    manRow=(call_number,unit_id ,received_dtTm , on_scene_dtTm,durationInMinutes,"",origPriorityMapped,finalPriorityMapped,address,city,zipcode,neighborhood)
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
    if row[22]=="":     #Colonna 22: priorità chiamata, non può essere nulla
        return False
    if row[23]=="":     #Colonna 23: priorità finale, non può essere nulla
        return False
    if row[10]=="":     #Colonna 10: data intervento
        row[10]=row[6]
    if row[10]<row[6]:  #Colonna 6: data arrivo, controlla che le date non siano in conflitto
        return False
    if row[15]== "":    #Colonna 15: address
        return False
    if row[16]=="":     #Colonna 16: city
        return False
    if row[17]=="":     #Colonna 17: zipcode
        return False
    return True

def exportDimensionDurataToCsv(dict, path, lastID):
    #lastID: è l'ultimo id inserito prima delle operazioni di aggiornamento
    with open(path, 'w',newline='') as fl:
        for k,v in dict.items():
            if k>=lastID:
                dimRow = [k,v, 0, 0, 0, 0]
                if (v>=25):
                    dimRow[5] = 1
                if (v<=5):
                    dimRow[2] = 1
                if (v<=15):
                    dimRow[3] = 1
                if (v<=25):
                    dimRow[4] = 1

                #cur.execute("INSERT INTO test_dim_durata VALUES (%s, %s, %s, %s, %s, %s, %s)", (dimRow[0], dimRow[1], dimRow[2], dimRow[3], dimRow[4], dimRow[5], ))

                fl.write(repr(dimRow[0]) + "," +  repr(dimRow[1]) + "," + repr(dimRow[2]) + "," + repr(dimRow[3]) + "," + repr(dimRow[4]) + "," +  repr(dimRow[5]) +"\n")
    fl.close()

def exportDimensionDateToCsv(dict,path,lastID):
    with open(path,'w',newline='') as fl:
        for k,v in dict.items():
            if v>=lastID:
                dt=datetime.datetime.strptime(k,"%Y-%m-%dT%H:%M:%S")
                fl.write(repr(v) + "," + k + "," + repr(dt.hour)+ "," + repr(dt.day)+ "," + repr(dt.month)+ "," + repr(dt.year)+ "," + repr(dt.year)+ "\n")
    fl.close()

def exportDimensionLocationToCsv(dict, path, lastID):
    # lastID: è l'ultimo id inserito prima delle operazioni di aggiornamento
    #tokenize value string

    with open(path, 'w', newline='') as fl:
        for k, v in dict.items():
            if v >=lastID:
                fieldsList=k.split("@")
                fl.write(repr(v) + "," + fieldsList[0] + "," + fieldsList[1] + "," + fieldsList[2] + "," + fieldsList[3] + "\n")
    fl.close()


def exportFactToCsv(f, manRow, idDuration, idDate, idLocation):
        stw = (repr(idDate)+ "," + repr(idDuration) + "," + repr(idLocation) + "," + manRow[0] + "," + repr(manRow[1])+ "," + repr(manRow[3])+ "," +  repr(manRow[6])+ "," +  repr(manRow[7])+"\n" )
        f.write(stw)

def csvToPostgres(csvPath,tablename,cur,conn):
    with open(csvPath, 'r') as f:
        cur.copy_from(f, tablename, sep=',')
        conn.commit()


postgresConnectionString = "dbname=test user=postgres password=1234 host=localhost"
inputCsvPath = Path.cwd() / 'datasource/fire-department-calls-for-service-1250-1500.csv' #r"\datasource\testPython.csv"
#inputCsvPath = Path.cwd() / 'datasource/testPython.csv'
dimDurationCSVPath = Path.cwd() / 'output/dim_duration.csv' #r"C:\Users\utente\OneDrive\Desktop\BD2\codice\datasource\dim_durata.csv"
dimDateCSVPath= Path.cwd() / 'output/dim_date.csv'
dimLocationCSVPath= Path.cwd() / 'output/dim_location.csv'
fact_csvPATH = Path.cwd() / 'output/fact.csv' #r"C:\Users\utente\OneDrive\Desktop\BD2\codice\fact.csv"

conn = psycopg2.connect(postgresConnectionString)
cur = conn.cursor()

createTables(cur,conn)

# Dictionaries
tempTableDurata={}
tempTableLocation={}
tempTableDate={}

# Fill dictionaries and fetch latest id
lastIDDuration=putDurationTableInDictionary(tempTableDurata)
lastIDLocation=putLocationTableInDictionary(tempTableLocation)
lastIDDate=putDateTableInDictionary(tempTableDate)


open(fact_csvPATH, 'w').close()
f=open(fact_csvPATH, 'a', newline='')

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

                idDuration=getDimensionDurationRow(manRow[4], tempTableDurata)
                idDate=getDimensionDateRow(manRow[2],tempTableDate)
                idLocation=getDimensionLocationRow(manRow[8],manRow[9],manRow[10],manRow[11],tempTableLocation)

                if idDuration is not None:
                    exportFactToCsv(f, manRow, idDuration, idDate, idLocation)
                    #cur.execute("INSERT INTO test_fact (call_number, unit_id, rec_date, scene_date, durata_int, or_prio, fin_prio,for_key_durata) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",(manRow[0], manRow[1], manRow[2], manRow[3],manRow[4],manRow[5],manRow[6],rowDim))
            else:
                cntNotValidRows=cntNotValidRows+1
        else:
            cnt = cnt + 1
    exportDimensionDurataToCsv(tempTableDurata, dimDurationCSVPath, lastIDDuration)
    exportDimensionDateToCsv(tempTableDate,dimDateCSVPath,lastIDDate)
    exportDimensionLocationToCsv(tempTableLocation,dimLocationCSVPath,lastIDLocation)


csvToPostgres(dimDurationCSVPath, 'test_dim_duration', cur, conn)
csvToPostgres(fact_csvPATH,'test_fact',cur,conn)
csvToPostgres(dimLocationCSVPath,'test_dim_location',cur,conn)
csvToPostgres(dimDateCSVPath,'test_dim_received_date',cur,conn)

f.close()

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

#SCRIVO la
#SELECT test2_table.distretto, COUNT(*) AS cnt FROM test2_table GROUP BY test2_table.distretto ORDER BY 2 DESC