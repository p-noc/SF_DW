import psycopg2
import csv
import codecs
import time
import datetime
import ast
from pathlib import Path
import random
import sys

cntNotValidRows=0
cntValidRows=0

class FragFile:
    def __init__(self,year,cur):
        path = 'output/factFrag' + year + '.csv'
        self.filePath=Path.cwd() / path
        self.fileDesc=open(self.filePath, 'a', newline='')
        self.postgresTableName='dispatch911_frag_'+year
        query = 'CREATE TABLE IF NOT EXISTS ' + self.postgresTableName + '(id_received_date integer,id_geo_place integer,id_duration smallint,id_responsibility integer,id_call_type smallint,call_number varchar(20),unit_id varchar(10),incident_number varchar(10),call_date timestamp without time zone, watch_date timestamp without time zone,entry_DtTm timestamp without time zone,dispatch_DtTm timestamp without time zone,response_DtTm timestamp without time zone,on_scene_DtTm timestamp without time zone,transport_DtTm timestamp without time zone,hospital_DtTm timestamp without time zone,call_final_disposition varchar(30),available_DtTm timestamp without time zone,original_priority varchar(1),priority varchar(1),final_priority varchar(1),ALS_unit bool,number_of_alarms smallint,unit_type varchar(20),unit_sequence_in_call_dispatch smallint,fire_prevenction_district varchar(10),supervisor_district varchar(20),location_f point,rowid varchar(50))'
        cur.execute(query)

# Class for query testing
class QueryTester:
    csvQueryResults=None
    resultsCsvWriter=None
    queryArray=[]
    queryIterations = 6
    csvQueryResultsPath = Path.cwd() / 'output/queryResults.csv'

    def __init__(self):
        self.csvQueryResults = open(self.csvQueryResultsPath, 'w', newline='')
        self.resultsCsvWriter = csv.writer(self.csvQueryResults, lineterminator='\n', delimiter=';')

        # Query 1A (Dimensions) #1
        self.queryArray.append("SELECT dat.year_f, geo.neighborhooods,count(*) FROM dispatch911_dimensions as dis INNER JOIN dim_geo_place as geo ON dis.id_geo_place=geo.id_geo_place INNER JOIN dim_call_type as callt ON callt.id_call_type=dis.id_call_type INNER JOIN dim_received_date as dat ON dat.id_received_date=dis.id_received_date WHERE callt.call_type='HazMat' GROUP BY  dat.year_f, geo.neighborhooods")
        # Query 1A (Original)   #2
        self.queryArray.append("SELECT date_part('year', received_dttm), neighborhood_district, count(*) FROM dispatch911_original WHERE call_type='HazMat' GROUP BY date_part('year', received_dttm), neighborhood_district")
        # Query 1B (Dimensions) #3
        self.queryArray.append("SELECT geo.neighborhooods, COUNT( distinct call_number) FROM dispatch911_dimensions as dis INNER JOIN dim_geo_place as geo ON dis.id_geo_place=geo.id_geo_place INNER JOIN dim_call_type as callt ON callt.id_call_type=dis.id_call_type INNER JOIN dim_received_date as dat ON dat.id_received_date=dis.id_received_date WHERE callt.call_type='HazMat' AND dat.year_f='2016' GROUP BY  geo.neighborhooods")
        # Query 1B (Frag)       #4
        self.queryArray.append("select neighborhooods, count(distinct q1.call_number) from (SELECT * FROM dispatch911_frag_2000 as frag ) as q1 inner join (SELECT id_call_type,call_type from dim_call_type as calltype WHERE calltype.call_type='HazMat' ) as q2 on q1.id_call_type = q2.id_call_type inner join ( SELECT id_geo_place,neighborhooods FROM dim_geo_place )as q3 on q1.id_geo_place = q3.id_geo_place GROUP BY  q3.neighborhooods")

        # Query 2 (No vista)    #5
        self.queryArray.append("select dayquery.call_type, (minutes_day), (minutes_night) from (select  call_type, avg(minutes) as minutes_day from dispatch911_dimensions  fact INNER JOIN dim_duration  dur on (fact.id_duration = dur.id_duration) INNER JOIN dim_call_type as emergency on fact.id_call_type = emergency.id_call_type INNER JOIN dim_received_date as recdate on fact.id_received_date = recdate.id_received_date where recdate.hour_f in (1,2,3,4,5,6,7,8,9,10,11,12) group by call_type ) AS dayquery inner join (select  call_type, avg(minutes) as minutes_night from dispatch911_dimensions  fact INNER JOIN dim_duration  dur on (fact.id_duration = dur.id_duration) INNER JOIN dim_call_type as emergency on fact.id_call_type = emergency.id_call_type INNER JOIN dim_received_date as recdate on fact.id_received_date = recdate.id_received_date where recdate.hour_f in (13,14,15,16,17,18,19,20,21,22,23,24) group by call_type ) AS nightquery on dayquery.call_type = nightquery.call_type")
        # Query 2 (Vista)       #6
        self.queryArray.append("select dayquery.call_type, dayquery.avgminutes as dayavg, nightquery.avgminutes as nightavg from (select  call_type,  avg(minutes) as avgminutes from intervention_daytime group by call_type )AS dayquery inner join (select  call_type,  avg(minutes) as avgminutes from intervention_nighttime group by call_type )AS nightquery on dayquery.call_type = nightquery.call_type")

        # Query 3 (Original)    #7
        self.queryArray.append("(SELECT '1' as season,neighborhood_district, count(*) FROM dispatch911_original WHERE date_part('month',received_dttm) IN (12,1,2) AND call_type_group='Fire' GROUP BY neighborhood_district) union (SELECT '2' as season,neighborhood_district, count(*) FROM dispatch911_original WHERE date_part('month',received_dttm) IN (3,4,5) AND call_type_group='Fire' GROUP BY neighborhood_district) union (SELECT '3' as season,neighborhood_district, count(*) FROM dispatch911_original WHERE date_part('month',received_dttm) IN (6,7,8) AND call_type_group='Fire' GROUP BY neighborhood_district) union (SELECT '4' as season,neighborhood_district, count(*) FROM dispatch911_original WHERE date_part('month',received_dttm) IN (9,10,11) AND call_type_group='Fire' GROUP BY neighborhood_district)	 order by neighborhood_district, 1")
        # Query 3 (Dimensions)  #8
        self.queryArray.append("SELECT dat.season, geo.neighborhooods, count(*) FROM dispatch911_dimensions as dis INNER JOIN dim_geo_place as geo ON dis.id_geo_place=geo.id_geo_place INNER JOIN dim_call_type as callt ON callt.id_call_type=dis.id_call_type INNER JOIN dim_received_date as dat ON dat.id_received_date=dis.id_received_date WHERE callt.call_type_group='Fire' GROUP BY dat.season, geo.neighborhooods order by 2,1")

        # Query 4 (Dimensions)  #9
        self.queryArray.append("select q2.original_priority,q2.call_type, avg(number_of_unit_dispatched) from (Select call_number, count(*) as number_of_unit_dispatched from dispatch911_dimensions group by call_number )as q1 inner join (Select distinct call_number, original_priority, call_type from dispatch911_dimensions as fact inner join dim_call_type as emer on fact.id_call_type = emer.id_call_type )as q2 on q1.call_number = q2.call_number group by q2.original_priority,q2.call_type order by 3")
        # Query 4 (Original)    #10
        self.queryArray.append("select q2.original_priority,q2.call_type, avg(number_of_unit_dispatched) from (Select call_number, count(*) as number_of_unit_dispatched from dispatch911_original group by call_number)as q1 inner join (Select distinct call_number, original_priority, call_type from dispatch911_original as fact )as q2 on q1.call_number = q2.call_number group by q2.original_priority,q2.call_type order by 3")

        # Query 5 (Vista)    #11
        self.queryArray.append("(select dayq.call_type, daycount, nightcount from (select call_type, count(*) as daycount from intervention_daytime where final_priority < original_priority group by call_type) as dayq inner join (select call_type, count(*) as nightcount from intervention_nighttime where final_priority < original_priority group by call_type) nightq on  dayq.call_type = nightq.call_type)")
        # Query 5 (NoVista)       #12
        self.queryArray.append("(select dayq.call_type, daycount, nightcount from 	(select call_type, count(*) as daycount	from dispatch911_dimensions  fact	INNER JOIN dim_duration  dur on (fact.id_duration = dur.id_duration) INNER JOIN dim_call_type as emergency on fact.id_call_type = emergency.id_call_type	INNER JOIN dim_received_date as recdate on fact.id_received_date = recdate.id_received_date where recdate.hour_f in (1,2,3,4,5,6,7,8,9,10,11,12) and final_priority < original_priority group by call_type) as dayq	inner join (select call_type, count(*) as nightcount  from dispatch911_dimensions  fact INNER JOIN dim_duration  dur on (fact.id_duration = dur.id_duration) INNER JOIN dim_call_type as emergency on fact.id_call_type = emergency.id_call_type INNER JOIN dim_received_date as recdate on fact.id_received_date = recdate.id_received_date where recdate.hour_f in (13,14,15,16,17,18,19,20,21,22,23,24) and final_priority < original_priority group by call_type) nightq on  dayq.call_type = nightq.call_type)")

        # Query 6 (Dimensions)  #13
        self.queryArray.append("select fact.original_priority, count(*) from dispatch911_dimensions as fact inner join dim_call_type as emergency on fact.id_call_type = emergency.id_call_type inner join dim_duration as dur on fact.id_duration = dur.id_duration where dur.lessfive=true group by fact.original_priority ")
        # Query 6 (Original)    #14
        self.queryArray.append("select fact.original_priority, count(*) from dispatch911_original as fact where fact.durationminutes <= 5 group by fact.original_priority")

        # Query 7 (Original)    #15
        self.queryArray.append("select fact.call_type, count(*), avg(fact.durationminutes) from dispatch911_original as fact where fact.battalion = 'B01' group by fact.call_type")
        # Query 7 (Dimension)   #16
        self.queryArray.append("select emergency.call_type, count(*), avg(dur.minutes) from dispatch911_dimensions as fact inner join dim_call_type as emergency on fact.id_call_type = emergency.id_call_type inner join dim_duration as dur on fact.id_duration = dur.id_duration inner join dim_responsibility as resp on fact.id_responsibility = resp.id_responsibility where resp.battalion = 'B01' group by emergency.call_type")

        # Query 8 (Dimension)   #17
        self.queryArray.append("select box, count (distinct call_number) as number_of_calls from (	 (select * from dispatch911_dimensions as fact) as q1 inner join (select id_geo_place from dim_geo_place as geo where geo.city='SAN FRANCISCO') as q2 on q1.id_geo_place = q2.id_geo_place inner join (select recdate.id_received_date from dim_received_date as recdate where recdate.year_f ='2015' )as q2bis on q1.id_received_date = q2bis.id_received_date ) inner join ( select id_responsibility, box from dim_responsibility as resp ) as q3 on q1.id_responsibility = q3.id_responsibility group by box order by 2 desc")
        # Query 8 (Frag)        #18
        self.queryArray.append("select box, count (distinct call_number) as number_of_calls from  ((select * from dispatch911_frag_2000 as fact) as q1 inner join (select id_geo_place from dim_geo_place as geo where geo.city='SAN FRANCISCO') as q2 on q1.id_geo_place = q2.id_geo_place ) inner join (  select id_responsibility, box from dim_responsibility as resp ) as q3 on q1.id_responsibility = q3.id_responsibility group by box order by 2 desc")
        # Query 8 (Original)    #19
        self.queryArray.append("select box, count (distinct call_number) as number_of_calls from dispatch911_original as fact where city='SAN FRANCISCO' and date_part('year', received_dttm) = '2015' group by box order by 2 desc")

    def computeAndWriteAvgs(self, block, usingIndex):
        queryIndex=1
        for q in self.queryArray:
            queryTime = 0
            for i in range(0,self.queryIterations):
                query_start_time=time.time()
                cur.execute(q)
                if (i!=0):
                    queryTime=queryTime+(time.time()-query_start_time)
                #print(queryTime/(i+1))

            outResultRow=[block, queryIndex, usingIndex, queryTime / self.queryIterations * 1000]
            self.resultsCsvWriter.writerow(outResultRow)
            queryIndex=queryIndex+1
            print(outResultRow)

    def createIndex(self):
        cur.execute("CREATE INDEX  dispatch_date_ind on dispatch911_dimensions (id_received_date);")
        cur.execute("CREATE INDEX  dispatch_date_geo on dispatch911_dimensions (id_geo_place);")
        cur.execute("CREATE INDEX  dispatch_date_dur on dispatch911_dimensions (id_duration);")
        cur.execute("CREATE INDEX  dispatch_date_resp on dispatch911_dimensions (id_responsibility);")
        cur.execute("CREATE INDEX  dispatch_date_calltype on dispatch911_dimensions (id_call_type);")

    def dropIndex(self):
        cur.execute("DROP INDEX dispatch_date_ind;")
        cur.execute("DROP INDEX dispatch_date_geo;")
        cur.execute("DROP INDEX dispatch_date_dur;")
        cur.execute("DROP INDEX dispatch_date_resp;")
        cur.execute("DROP INDEX dispatch_date_calltype;")

def createTables(cur,conn):
    #cur.execute("CREATE TYPE enum_call_type AS ENUM ('Administrative','Aircraft Emergency','Alarms','Assist Police','Citizen Assist / Service Call','Confined Space / Structure Collapse','Electrical Hazard','Elevator / Escalator Rescue','Explosion','Extrication / Entrapped (Machinery  Vehicle)','Fuel Spill','Gas Leak (Natural and LP Gases)','HazMat','High Angle Rescue','Industrial Accidents','Lightning Strike (Investigation)','Marine Fire','Medical Incident','Mutual Aid / Assist Outside Agency','Odor (Strange / Unknown)','Oil Spill','Other','Outside Fire','Smoke Investigation (Outside)','Structure Fire','Suspicious Package','Traffic Collision','Train / Rail Fire','Train / Rail Incident','Transfer','Vehicle Fire','Water Rescue','Watercraft in Distress')")
    #cur.execute("CREATE TYPE enum_call_type_group AS ENUM ('Fire','Potentially Life-Threatening','Non Life-threatening','Alarm','NotAssigned')")

    #cur.execute("DROP TABLE IF NOT EXISTS dispatch911_original,dim_received_date,dim_duration,dim_geo_place,dim_responsibility,dim_call_type,dispatch911_dimensions")
    cur.execute("CREATE TABLE IF NOT EXISTS dim_received_date(id_received_date integer NOT NULL,received_DtTm timestamp without time zone, hour_f smallint, day_f smallint, month_f smallint, year_f smallint, season smallint)")
    cur.execute("CREATE TABLE IF NOT EXISTS dim_duration (id_duration smallint NOT NULL,minutes smallint NOT NULL,lessFive boolean NOT NULL DEFAULT '0',lessFifteen boolean NOT NULL DEFAULT '0',lessTwentyfive boolean NOT NULL DEFAULT '0',moreTwentyfive boolean NOT NULL DEFAULT '0')")
    cur.execute("CREATE TABLE IF NOT EXISTS dim_geo_place(id_geo_place Integer NOT NULL,address varchar(100),city varchar(50),zipcode integer,Neighborhooods varchar(50))")
    cur.execute("CREATE TABLE IF NOT EXISTS dim_responsibility(id_responsibility integer, box varchar,station_area varchar, battalion varchar(5))")
    cur.execute("CREATE TABLE IF NOT EXISTS dim_call_type(id_call_type smallint, call_type enum_call_type, call_type_group enum_call_type_group)")
    cur.execute("CREATE TABLE IF NOT EXISTS dispatch911_original(call_number varchar(20),unit_id varchar(10),incident_number varchar(10),call_type varchar(50),call_date timestamp without time zone, watch_date timestamp without time zone,received_DtTm timestamp without time zone,entry_DtTm timestamp without time zone,dispatch_DtTm timestamp without time zone,response_DtTm timestamp without time zone,on_scene_DtTm timestamp without time zone,transport_DtTm timestamp without time zone,hospital_DtTm timestamp without time zone,call_final_disposition varchar(30),available_DtTm timestamp without time zone,address varchar(50),city varchar(30),zipcode_of_incident varchar(10),battalion varchar(10),station_area varchar(20),box varchar(10),original_priority varchar(1),priority varchar(1),final_priority varchar(1),ALS_unit bool,call_type_group varchar(35),number_of_alarms smallint,unit_type varchar(20),unit_sequence_in_call_dispatch smallint,fire_prevenction_district varchar(10),supervisor_district varchar(20),neighborhood_district varchar(50),location_f varchar(50),rowid varchar(50),durationMinutes smallint)")
    cur.execute("CREATE TABLE IF NOT EXISTS dispatch911_dimensions(id_received_date integer,id_geo_place integer,id_duration smallint,id_responsibility integer,id_call_type smallint,call_number varchar(20),unit_id varchar(10),incident_number varchar(10),call_date timestamp without time zone, watch_date timestamp without time zone,entry_DtTm timestamp without time zone,dispatch_DtTm timestamp without time zone,response_DtTm timestamp without time zone,on_scene_DtTm timestamp without time zone,transport_DtTm timestamp without time zone,hospital_DtTm timestamp without time zone,call_final_disposition varchar(30),available_DtTm timestamp without time zone,original_priority varchar(1),priority varchar(1),final_priority varchar(1),ALS_unit bool,number_of_alarms smallint,unit_type varchar(20),unit_sequence_in_call_dispatch smallint,fire_prevenction_district varchar(10),supervisor_district varchar(20),location_f point,rowid varchar(50))")
    cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS intervention_daytime AS ((select rowid, minutes, call_type, original_priority, final_priority from dispatch911_dimensions  fact INNER JOIN dim_duration  dur on (fact.id_duration = dur.id_duration)INNER JOIN dim_call_type as emergency on fact.id_call_type = emergency.id_call_type	INNER JOIN dim_received_date as recdate on fact.id_received_date = recdate.id_received_date  where recdate.hour_f in (1,2,3,4,5,6,7,8,9,10,11,12)))")
    cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS intervention_nighttime AS ((select rowid, minutes, call_type, original_priority, final_priority from dispatch911_dimensions  fact INNER JOIN dim_duration  dur on (fact.id_duration = dur.id_duration) INNER JOIN dim_call_type as emergency on fact.id_call_type = emergency.id_call_type INNER JOIN dim_received_date as recdate on fact.id_received_date = recdate.id_received_date	where recdate.hour_f in (13,14,15,16,17,18,19,20,21,22,23,24)))")

    conn.commit()

def putDurationTableInDictionary(dict):
    cur.execute("SELECT dim_duration.id_duration, dim_duration.minutes FROM dim_duration")
    queryRes=cur.fetchall()
    for k in queryRes:
        dict[k[1]]=k[0]
    if len(queryRes)>0:
        return queryRes[len(queryRes)-1][0]
    else:
        return 0

def putGeoPlaceTableInDictionary(dictLoc):
    cur.execute("SELECT * FROM dim_geo_place")
    queryRes=cur.fetchall()

    for k in queryRes:
        geoPlaceString=k[1]+"@"+k[2]+"@"+str(k[3])+"@"+k[4]
        dictLoc[geoPlaceString]=k[0]
    if len(queryRes)>0:
        return queryRes[len(queryRes)-1][0]
    else:
        return 0


def putDateTableInDictionary(dict):
    cur.execute("SELECT id_received_date, received_DtTm FROM dim_received_date")
    queryRes=cur.fetchall()

    for k,j in queryRes:
        dict[j.strftime("%Y-%m-%dT%H:%M:%S")]=k

    if len(queryRes) > 0:
        return queryRes[len(queryRes) - 1][0]
    else:
        return 0

def putResponsibilityTableInDictionary(dictResp):
    cur.execute("SELECT * FROM dim_responsibility")
    queryRes=cur.fetchall()
    for k in queryRes:
        responsString=k[1]+"@"+k[2]+"@"+(k[3])
        dictResp[responsString]=k[0]
    if len(queryRes)>0:
        return queryRes[len(queryRes)-1][0]
    else:
        return 0


def putCallTypeTableInDictionary(dictCallType):
    cur.execute("SELECT * FROM dim_call_type")
    queryRes=cur.fetchall()
    for k in queryRes:
        calltypeString=k[1]+"@"+k[2]
        dictCallType[calltypeString]=k[0]
    if len(queryRes)>0:
        return queryRes[len(queryRes)-1][0]
    else:
        return 0

def getDimensionDurationRow(duration, tempTableDurata):
    '''
    if (duration not in tempTableDurata.values()):
        tempTableDurata[len(tempTableDurata)] = duration

    for idd,dur in tempTableDurata.items():
        if dur==duration:
            return idd
    '''
    res=tempTableDurata.get(duration)
    if res is None:
        tempTableDurata[duration] = len(tempTableDurata)
        return len(tempTableDurata)-1
    else:
        return res

def getDimensionDateRow(recDate,tempTableDate):
    res=tempTableDate.get(recDate)
    if res is None:
        tempTableDate[recDate]=len(tempTableDate)
        return len(tempTableDate)-1
    else:
        return res

def getDimensionGeoPlaceRow(address, city, zipcode, neigh, tempTableGeoPlace):
    dimensionString=address + "@" + city + "@" + zipcode + "@" + neigh
    id=tempTableGeoPlace.get(dimensionString)
    if (id is None):
        tempTableGeoPlace[dimensionString] = len(tempTableGeoPlace)
        return len(tempTableGeoPlace) - 1
    else:
        return id

def getDimensionResponsibilityRow(box,station_area,battalion,tempTableResponsibility):
    responsibility=box+"@"+station_area+"@"+battalion
    id = tempTableResponsibility.get(responsibility)
    if (id is None):
        tempTableResponsibility[responsibility] = len(tempTableResponsibility)
        return len(tempTableResponsibility)-1
    else:
        return id

def getDimensionCallTypeRow(call_type,call_type_group,tempTableCallType):
    calltype=call_type+"@"+call_type_group
    id = tempTableCallType.get(calltype)
    if (id is None):
        tempTableCallType[calltype]=len(tempTableCallType)
        return len(tempTableCallType)-1
    else:
        return id

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

def rowManipulation(row,cur):

    call_number=row[0]
    unit_id=row[1]
    incident_number=row[2]
    call_type=row[3]
    call_date=row[5]
    watch_date=row[4]
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

    # Evaluate intervention duration
    d1 = datetime.datetime.strptime(received_dtTm, "%Y-%m-%dT%H:%M:%S")
    d2 = datetime.datetime.strptime(on_scene_dtTm, "%Y-%m-%dT%H:%M:%S")
    durationInMinutes = d2-d1
    durationInMinutes =(int(durationInMinutes.seconds/60))

    #
    if (call_type_group==''):
        call_type_group=callTypeGroupDictionary.get(random.randint(0,3))

    call_type=call_type.replace(","," ")

    # als_unit (bool) in postgres
    if als_unit=='True':
        als_unit=1
    else:
        als_unit=0

    if(number_of_alarms is not 'None'):
        number_of_alarms=int(number_of_alarms)
    if (unit_sequence_call_dispatch is not 'None'):
        unit_sequence_call_dispatch=int(unit_sequence_call_dispatch)

    lat_lon='None'
    dictTest=ast.literal_eval(location)
    if(dictTest is not None):
        longitude=dictTest.get('longitude')
        latitude=dictTest.get('latitude')
        lat_lon='('+latitude+","+longitude+')'

    manRow=(call_number,                    #0
            unit_id,                        #1
            incident_number,                #2
            call_type,                      #3
            call_date,                      #4
            watch_date,                     #5
            received_dtTm,                  #6
            entry_dtTm,                     #7
            dispatch_dtTm,                  #8
            response_dtTm,                  #9
            on_scene_dtTm,                  #10
            transport_dtTm,                 #11
            hospital_dtTm,                  #12
            call_final_disposition,         #13
            available_dtTm,                 #14
            address,                        #15
            city,                           #16
            zipcode,                        #17
            battalion,                      #18
            station_area,                   #19
            box,                            #20
            origPriorityMapped,             #21
            callPriorityMapped,             #22
            finalPriorityMapped,            #23
            als_unit,                       #24
            call_type_group,                #25
            number_of_alarms,               #26
            unit_type,                      #27
            unit_sequence_call_dispatch,    #28
            fire_prevention_district,       #29
            supervisor_district,            #30
            neighborhood,                   #31
            lat_lon,                        #32
            rowid,                          #33
            durationInMinutes)              #34

    # ++++++++++++++++++++++++++++++++++++++++++++++++
    # +++ Not logically linked to row manipulation +++
    dt = datetime.datetime.strptime(manRow[6], "%Y-%m-%dT%H:%M:%S")
    if fragTablesPath.get(dt.year) is None:
        newFragFilePath(dt.year, fragTablesPath,cur)
    # +++ ---------------------------------------- +++
    # ++++++++++++++++++++++++++++++++++++++++++++++++
    '''
    for i in range(35):
        print(i,row[i])
    print("---")
    '''
    return manRow

import string
def randomStr(size=6, chars=string.ascii_uppercase + string.digits+string.ascii_lowercase):
    return ''.join(random.choice(chars) for x in range(size))

def generateConsistentFakeRows(tableDurata, tableGeoPlace, tableDate, tableResponsibility, tableCallType, numOfRows=100):
    fakeStr = 'FAKE'
    legalPriorities = [2, 3]

    if (not tableGeoPlace or not tableResponsibility):
        print("empty dimensions, fake rows will not be inserted")


    outputFakeRows = Path.cwd() / 'datasource/fakeRows.csv'

    f = codecs.open(outputFakeRows, 'w', encoding='utf-16-le')
    # f = open(outputFakeRows, 'a', newline='')
    writer = csv.writer(f, lineterminator='\n', delimiter=',')

    for i in range(numOfRows):
        fakeRow = [None] * 35
        fakeRow[0] = fakeStr + randomStr(15)  # call_number varchar(20)
        fakeRow[1] = fakeStr + randomStr(5)  # unit_id varchar(10)
        fakeRow[21] = random.choice(legalPriorities)
        fakeRow[22] = random.choice(legalPriorities)
        fakeRow[23] = random.choice(legalPriorities)
        fakeRow[25] = 'Alarm'
        fakeRow[3] = 'Other'

        geoTuple=random.choice(list(tableGeoPlace.keys()))
        geoFields = geoTuple.split("@")
        fakeRow[31]=geoFields[3]
        fakeRow[15]=geoFields[0]
        fakeRow[16] = geoFields[1]
        fakeRow[17] = geoFields[2]

        respTuple=random.choice(list(tableResponsibility.keys()))
        respFields= respTuple.split("@")
        fakeRow[19] = respFields[1]
        fakeRow[18] = respFields[2]
        fakeRow[20] = respFields[0]

        writer.writerow(fakeRow)
    f.close()

def createCallTypeDictionary():
    dictCallType={'Administrative':'Fire','Aircraft Emergency':'Alarm','Alarms':'Alarm','Assist Police':'Alarm','Citizen Assist / Service Call':'Alarm','Confined Space / Structure Collapse':'Fire','Electrical Hazard':'Alarm','Elevator / Escalator Rescue':'Alarm','Explosion':'Fire','Extrication / Entrapped (Machinery  Vehicle)':'Fire','Fuel Spill':'Alarm','Gas Leak (Natural and LP Gases)':'Alarm','HazMat':'Alarm','HazMat':'Fire','High Angle Rescue':'Fire','Industrial Accidents':'Fire','Marine Fire':'Fire','Medical Incident':'Alarm','Medical Incident':'Non Life-threatening','Medical Incident':'Potentially Life-Threatening','Mutual Aid / Assist Outside Agency':'Fire','Odor (Strange / Unknown)':'Alarm','Odor (Strange / Unknown)':'Fire','Oil Spill':'Alarm','Other':'Alarm','Other':'Non Life-threatening','Other':'Potentially Life-Threatening','Outside Fire':'Alarm','Outside Fire':'Fire','Smoke Investigation (Outside)':'Alarm','Structure Fire':'Alarm','Structure Fire':'Fire','Structure Fire':'Potentially Life-Threatening','Suspicious Package':'Fire','Traffic Collision':'Non Life-threatening','Traffic Collision':'Potentially Life-Threatening','Train / Rail Fire':'Fire','Train / Rail Incident':'Fire','Vehicle Fire':'Alarm','Vehicle Fire':'Fire','Water Rescue':'Fire','Water Rescue':'Potentially Life-Threatening','Watercraft in Distress':'Alarm','Watercraft in Distress':'Fire','Extrication / Entrapped (Machinery, Vehicle)':'Alarm'}

    return dictCallType;

def cityValidation(cityName):
    cityName = cityName.upper()

    if cityName == "TI":
        return "TREASURE"
    elif cityName == "BN":
        return "BRISBANE"
    elif cityName == "DC":
        return "DALY CITY"
    elif cityName == "FM":
        return "FORT MASON"
    elif cityName == "HP":
        return "HUNTERS POINT"
    elif cityName == "PR":
        return "PRESIDIO"
    elif cityName == "SF":
        return "SAN FRANCISCO"
    elif cityName == "YB":
        return "YERBA BUENA"
    elif cityName == "TI":
        return "TREASURE ISLAND"
    elif cityName == "TREASURE ISLA":
        return "TREASURE ISLAND"
    return cityName;



def rowValidation(row,dictCallType):

    if row[25] == '' or row[25] == 'None' or row[25] is None:
        if(dictCallType.get(row[3] is not None)):
            row[25] = dictCallType.get(row[3])
        else:
            row[25]= 'NotAssigned'
    if row[31] == '' or row[31] == 'None' or row[31] is None: #Colonna 31: quartiere di SF, non può essere None
        return False
    if row[21]=="":     #Colonna 21: priorità, non può essere nulla
        return False
    if row[22]=="":     #Colonna 22: priorità chiamata, non può essere nulla
        return False
    if row[23]=="":     #Colonna 23: priorità finale, non può essere nulla
        return False
    if row[6]=="":     #Colonna DATA chiamata, se assente usa data corrente come placeholder
        yearRandom=random.randint(2020,2040)
        row[6]= datetime.datetime.strftime(datetime.datetime.now().replace(year=yearRandom), "%Y-%m-%dT%H:%M:%S")
    if row[10]=="" or (row[10]==row[6] and row[6!=""])or row[10]<row[6]:     #Colonna 10: data intervento, genera data con un ritardo in minuti casuale per la data d'arrivo sul sito se assente o invalida
        row[10]=row[6]
        onSiteDate=datetime.datetime.strptime(row[10], "%Y-%m-%dT%H:%M:%S")
        minutesOffset=random.randint(10,40)
        onSiteDate=onSiteDate+datetime.timedelta(minutes=minutesOffset)
        row[10]=datetime.datetime.strftime(onSiteDate, "%Y-%m-%dT%H:%M:%S")
    if row[15]== "":    #Colonna 15: address
        return False
    if row[16]=="":     #Colonna 16: city
        return False
    else:
        row[16]=cityValidation(row[16])
    if row[17]=="":     #Colonna 17: zipcode
        return False
    if row[19]=="":     #Colonna 19: station area
        return False
    if row[18] == "":  # Colonna 18: battalion
        return False
    if row[20] == "":  # Colonna 20: box
        return False

    for i in range(len(row)):
        if row[i] == "":
            row[i] = 'None'
    return True

def exportDimensionDurataToCsv(dict, path, lastID):
    with open(path, 'w',newline='') as fl:
        for v,k in dict.items():
            if k> lastID or lastID==0:
                dimRow = [k,v, 0, 0, 0, 0]
                if (v>=25):
                    dimRow[5] = 1
                if (v<=5):
                    dimRow[2] = 1
                if (v<=15):
                    dimRow[3] = 1
                if (v<=25):
                    dimRow[4] = 1

                fl.write(repr(dimRow[0]) + ";" +  repr(dimRow[1]) + ";" + repr(dimRow[2]) + ";" + repr(dimRow[3]) + ";" + repr(dimRow[4]) + ";" +  repr(dimRow[5]) +"\n")
    fl.close()

def exportDimensionDateToCsv(dict,path,lastID):
    with open(path,'w',newline='') as fl:
        for k,v in dict.items():
            if v> lastID or lastID==0:
                dt=datetime.datetime.strptime(k,"%Y-%m-%dT%H:%M:%S")
                if (dt.month==12) or (dt.month==1) or (dt.month==2):
                    season=1
                elif (dt.month==3) or (dt.month==4) or (dt.month==5):
                    season=2
                elif (dt.month==6) or (dt.month==7) or (dt.month==8):
                    season=3
                elif (dt.month==9) or (dt.month==10) or (dt.month==11):
                    season=4
                fl.write(repr(v) + ";" + k + ";" + repr(dt.hour)+ ";" + repr(dt.day)+ ";" + repr(dt.month)+ ";" + repr(dt.year)+ ";" + repr(season)+ "\n")
    fl.close()

def exportDimensionGeoPlaceToCsv(dict, path, lastID):
    #tokenize value string
    with open(path, 'w', newline='') as fl:
        for k, v in dict.items():
            if v > lastID or lastID==0:
                fieldsList=k.split("@")
                fl.write(repr(v) + ";" + fieldsList[0] + ";" + fieldsList[1] + ";" + fieldsList[2] + ";" + fieldsList[3] + "\n")
    fl.close()

def exportDimensionResponsibilityToCsv(dict,path,lastID):
    with open(path, 'w', newline='') as fl:
        for k, v in dict.items():
            if v > lastID or lastID==0:
                fieldsList = k.split("@")
                fl.write(repr(v) + ";" + fieldsList[0] + ";" + fieldsList[1] + ";" + fieldsList[2]+ "\n")
    fl.close()

def exportDimensionCallTypeToCsv(dict,path,lastID):
    with open(path, 'w', newline='') as fl:
        for k, v in dict.items():
            if v > lastID or lastID==0:
                fieldList=k.split("@")
                fl.write(repr(v)+";"+fieldList[0]+";"+fieldList[1]+"\n")
    fl.close()


def exportFactOriginalToCsv(f, manRow):
    writer = csv.writer(f,lineterminator='\n', delimiter=';')
    writer.writerow(manRow)

def exportFactDimToCsv(f, manRow, idDuration, idDate, idGeoPlace, idResponsibility, idCallType):
    #stw = (repr(idDate) + ";" + repr(idGeoPlace) + ";" + repr(idDuration) + ";" + repr(idResponsibility) + ";" + repr(idCallType) + ";" + manRow[0] + ";" + repr(manRow[1]) + ";" + repr(manRow[2]) + ";" + repr(manRow[4]) + ";" + repr(manRow[5]) +  ";" + repr(manRow[5]) +  ";" + repr(manRow[7]) + ";" + repr(manRow[8])+";" + repr(manRow[9]) +";" + repr(manRow[10]) +";" + repr(manRow[12]) +";" + repr(manRow[11]) +";" + repr(manRow[13]) +";" + repr(manRow[14]) +";" + repr(manRow[21]) +";" + repr(manRow[22]) +";" + repr(manRow[23]) +";" + repr(manRow[24]) +";" + repr(manRow[26]) +";" + repr(manRow[27]) +";" + repr(manRow[28]) +";" + repr(manRow[29]) +";" + repr(manRow[30]) +";" + repr(manRow[32]) +";" + repr(manRow[33]) +"\n")
    stw = [(idDate),(idGeoPlace),(idDuration),(idResponsibility),(idCallType),manRow[0],(manRow[1]),(manRow[2]),(manRow[4]),(manRow[5]),(manRow[7]),(manRow[8]), (manRow[9]) , (manRow[10]) , (manRow[11]) , (manRow[12]) , (manRow[13]) , (manRow[14]) , (manRow[21]) , (manRow[22]) , (manRow[23]) , (manRow[24]) , (manRow[26]) , (manRow[27]) , (manRow[28]) , (manRow[29]) , (manRow[30]) , (manRow[32]) , (manRow[33])]

    writer = csv.writer(f,lineterminator='\n', delimiter=';')
    writer.writerow(stw)

def csvToPostgres(csvPath,tablename,cur,conn):
    with open(csvPath, 'r') as f:
        try:
            cur.copy_from(f, tablename, sep=';',null='None')
        except psycopg2.OperationalError as e:
            print(e)

def newFragFilePath(year,fragTablesPath,cur):
    fragFile=FragFile(repr(year),cur)
    fragTablesPath[year]= fragFile

def exportFactToFragCSV(fragTablesPath ,manRow, idDuration, idDate, idGeoPlace, idResponsibility, idCallType):
#def exportFactToFragCSV(row,fragTablesPath):
    dt=datetime.datetime.strptime(manRow[6],"%Y-%m-%dT%H:%M:%S")
    stw = [(idDate),(idGeoPlace),(idDuration),(idResponsibility),(idCallType),manRow[0],(manRow[1]),(manRow[2]),(manRow[4]),(manRow[5]),(manRow[7]),(manRow[8]), (manRow[9]) , (manRow[10]) , (manRow[11]) , (manRow[12]) , (manRow[13]) , (manRow[14]) , (manRow[21]) , (manRow[22]) , (manRow[23]) , (manRow[24]) , (manRow[26]) , (manRow[27]) , (manRow[28]) , (manRow[29]) , (manRow[30]) , (manRow[32]) , (manRow[33])]
    writer = csv.writer(fragTablesPath.get(dt.year).fileDesc, lineterminator='\n', delimiter=';')
    writer.writerow(stw)

def closeFragmentationFiles (fragTablesPath):
    for year, fragFile in fragTablesPath.items():
        fragFile.fileDesc.close()

def openFragmentationFiles (fragTablesPath):
    for year, fragFile in fragTablesPath.items():
        fragFile.fileDesc = open(fragFile.filePath, 'w', newline='')


postgresConnectionString = "dbname=test user=postgres password=1234 host=localhost"

inputCsvPath1 = Path.cwd() / 'datasource/01-fire-department-calls-for-service.csv'
inputCsvPath2 = Path.cwd() / 'datasource/02-fire-department-calls-for-service.csv'
inputCsvPath3 = Path.cwd() / 'datasource/03-fire-department-calls-for-service.csv'
inputCsvPath4 = Path.cwd() / 'datasource/04-fire-department-calls-for-service.csv'
inputCsvPath5 = Path.cwd() / 'datasource/05-fire-department-calls-for-service.csv'

inputCsvPath6 = Path.cwd() / 'datasource/06-fire-department-calls-for-service.csv'
inputCsvPath7 = Path.cwd() / 'datasource/07-fire-department-calls-for-service.csv'
inputCsvPath8 = Path.cwd() / 'datasource/08-fire-department-calls-for-service.csv'
inputCsvPath9 = Path.cwd() / 'datasource/09-fire-department-calls-for-service.csv'
inputCsvPath10 = Path.cwd() / 'datasource/10-fire-department-calls-for-service.csv'

inputCsvPath11 = Path.cwd() / 'datasource/11-fire-department-calls-for-service.csv'
inputCsvPath12 = Path.cwd() / 'datasource/12-fire-department-calls-for-service.csv'
inputCsvPath13 = Path.cwd() / 'datasource/13-fire-department-calls-for-service.csv'
inputCsvPath14 = Path.cwd() / 'datasource/14-fire-department-calls-for-service.csv'
inputCsvPath15 = Path.cwd() / 'datasource/15-fire-department-calls-for-service.csv'

inputCsvPath16 = Path.cwd() / 'datasource/16-fire-department-calls-for-service.csv'
inputCsvPath17 = Path.cwd() / 'datasource/17-fire-department-calls-for-service.csv'
inputCsvPath18 = Path.cwd() / 'datasource/18-fire-department-calls-for-service.csv'
inputCsvPath19 = Path.cwd() / 'datasource/19-fire-department-calls-for-service.csv'

inputCsvPathFAKE = Path.cwd() / 'datasource/fakeRows.csv'
inputCsvPathTEST = Path.cwd() / 'datasource/testPython.csv'

inputList = []
inputList.append(inputCsvPath1)
inputList.append(inputCsvPath2)
inputList.append(inputCsvPath3)
inputList.append(inputCsvPath4)
inputList.append(inputCsvPath5)
''' 
inputList.append(inputCsvPath6)
inputList.append(inputCsvPath7)
inputList.append(inputCsvPath8)
inputList.append(inputCsvPath9)
inputList.append(inputCsvPath10)
inputList.append(inputCsvPath11)
inputList.append(inputCsvPath12)
inputList.append(inputCsvPath13)
inputList.append(inputCsvPath14)
inputList.append(inputCsvPath15)
inputList.append(inputCsvPath16)
inputList.append(inputCsvPath17)
inputList.append(inputCsvPath18)
inputList.append(inputCsvPath19)
inputList.append(inputCsvPathFAKE)
inputList.append(inputCsvPathTEST)
'''

dimDurationCSVPath = Path.cwd() / 'output/dim_duration.csv'
dimDateCSVPath= Path.cwd() / 'output/dim_date.csv'
dimGeoPlaceCSVPath= Path.cwd() / 'output/dim_geo_place.csv'
dimResponsibilityCSVPath= Path.cwd() / 'output/dim_responsibility.csv'
dimCallTypeCSVPath= Path.cwd() / 'output/dim_call_type.csv'
factOriginal_csvPATH = Path.cwd() / 'output/factOriginal.csv'
factDimensions_csvPATH = Path.cwd() / 'output/factDimensions.csv'

clockTimeExtraction=0
clockTimeTransformation=0
clockTimeLoading=0
clockTimeMatView=0
clockTimeOther=0
clockTimeIndex=0

elapsedTimeExtraction=0
elapsedTimeTransformation=0
elapsedTimeLoading=0
elapsedTimeMatView=0
elapsedTimeOther=0
elapsedTimeIndex=0

conn = psycopg2.connect(postgresConnectionString)
cur = conn.cursor()

createTables(cur,conn)

# Dictionaries
callTypeGroupDictionary={
                            0:'Fire',
                            1:'Potentially Life-Threatening',
                            2:'Non Life-threatening',
                            3:'Alarm',
                            4:'NotAssigned'
    }

tempTableDurata={}
tempTableGeoPlace={}
tempTableDate={}
tempTableResponsibility={}
tempTableCallType={}
fragTablesPath={}


dictCallType=createCallTypeDictionary()

# Last event date
# lastEventDate=lastEventDate[0][0].strftime("%Y-%m-%dT%H:%M:%S")

queryTester = QueryTester()

start_global_time = time.time()

csvIteration=0
for currentCSV in inputList:
    csvIteration=csvIteration+1
    # Fill dictionaries and fetch latest id
    lastIDDuration = putDurationTableInDictionary(tempTableDurata)
    lastIDGeoPlace = putGeoPlaceTableInDictionary(tempTableGeoPlace)
    lastIDDate = putDateTableInDictionary(tempTableDate)
    lastIDResponsibility = putResponsibilityTableInDictionary(tempTableResponsibility)
    lastIDCallType = putCallTypeTableInDictionary(tempTableCallType)

    if currentCSV==inputCsvPathFAKE:
        generateConsistentFakeRows(tempTableDurata, tempTableGeoPlace, tempTableDate, tempTableResponsibility,
                                   tempTableCallType, 5000)

    start_local_time=time.time()    #TODO se mettiamo i clock per ogni evento tipo Ext, Transf, Load, questo ci vuole?
    clockTimeExtraction=time.time()    # Start (Extraction phase)

    f=open(factOriginal_csvPATH, 'w', newline='')
    g=open(factDimensions_csvPATH, 'w', newline='')
    openFragmentationFiles(fragTablesPath)
    with codecs.open(currentCSV, 'rU', 'utf-16-le') as csv_file:
        reader = csv.reader(csv_file)
        cnt = 0
        for row in reader:
            if cnt != 0:
                valResult=rowValidation(row,dictCallType)
                if valResult:
                    cntValidRows=cntValidRows+1

                    elapsedTimeExtraction=elapsedTimeExtraction+(time.time()-clockTimeExtraction)
                    # Pause (Extraction phase)

                    # Start (Transformation phase)
                    clockTimeTransformation=time.time()
                    manRow = rowManipulation(row,cur)
                    elapsedTimeTransformation=elapsedTimeTransformation+(time.time()-clockTimeTransformation)
                    # End (Transformation phase)

                    # Start (OTHER)
                    clockTimeOther = time.time()

                    idDuration = getDimensionDurationRow(manRow[34], tempTableDurata)
                    #if (idDuration == 0):
                    #    idDuration += 1;
                    idDate = getDimensionDateRow(manRow[6], tempTableDate)
                    #if (idDate == 0):
                    #    idDate += 1;
                    idGeoPlace = getDimensionGeoPlaceRow(manRow[15], manRow[16], manRow[17], manRow[31],
                                                         tempTableGeoPlace)
                    #if (idGeoPlace == 0):
                    #    idGeoPlace += 1;
                    idResponsibility = getDimensionResponsibilityRow(manRow[20], manRow[19], manRow[18],
                                                                     tempTableResponsibility)
                    #if (idResponsibility == 0):
                    #    idResponsibility += 1;
                    idCallType = getDimensionCallTypeRow(manRow[3], manRow[25], tempTableCallType)
                    #if (idCallType == 0):
                    #    idCallType += 1;
                    elapsedTimeOther = elapsedTimeOther + (time.time() - clockTimeOther)
                    # End (Transformation phase)

                    # Start (Loading)
                    clockTimeLoading=time.time()

                    exportFactOriginalToCsv(f, manRow)
                    exportFactToFragCSV(fragTablesPath,manRow,idDuration,idDate,idGeoPlace,idResponsibility,idCallType)
                    exportFactDimToCsv(g,manRow,idDuration,idDate,idGeoPlace,idResponsibility,idCallType)

                    # Pause (Loading)
                    elapsedTimeLoading=elapsedTimeLoading+(time.time()-clockTimeLoading)
                else:
                    cntNotValidRows=cntNotValidRows+1
                # Re-Start (Extraction phase)
                clockTimeExtraction = time.time()
            else:
                cnt = cnt + 1


        # Re-Start (Loading)
        clockTimeLoading = time.time()

        exportDimensionDurataToCsv(tempTableDurata, dimDurationCSVPath, lastIDDuration)
        exportDimensionDateToCsv(tempTableDate,dimDateCSVPath,lastIDDate)
        exportDimensionGeoPlaceToCsv(tempTableGeoPlace, dimGeoPlaceCSVPath, lastIDGeoPlace)
        exportDimensionResponsibilityToCsv(tempTableResponsibility,dimResponsibilityCSVPath,lastIDResponsibility)
        exportDimensionCallTypeToCsv(tempTableCallType,dimCallTypeCSVPath,lastIDCallType)

        # Pause (Loading)
        elapsedTimeLoading = elapsedTimeLoading + (time.time() - clockTimeLoading)

    f.close()
    g.close()
    closeFragmentationFiles(fragTablesPath)
    #print("Fine fase estrazione e trasformazione (sec): %s" % (time.time() - start_local_time))

    # Re-Start (Loading)
    clockTimeLoading = time.time()

    csvToPostgres(dimDurationCSVPath, 'dim_duration', cur, conn)
    csvToPostgres(dimGeoPlaceCSVPath, 'dim_geo_place', cur, conn)
    csvToPostgres(dimDateCSVPath,'dim_received_date',cur,conn)
    csvToPostgres(dimResponsibilityCSVPath,'dim_responsibility',cur,conn)
    csvToPostgres(dimCallTypeCSVPath,'dim_call_type',cur,conn)
    csvToPostgres(factOriginal_csvPATH, 'dispatch911_original', cur, conn)
    csvToPostgres(factDimensions_csvPATH, 'dispatch911_dimensions', cur, conn)
    for y,fragFile in fragTablesPath.items():
        csvToPostgres(fragFile.filePath,fragFile.postgresTableName,cur,conn)

    open(dimDurationCSVPath, 'w').close()
    open(dimDateCSVPath, 'w').close()
    open(dimGeoPlaceCSVPath, 'w').close()
    open(dimResponsibilityCSVPath, 'w').close()
    open(dimCallTypeCSVPath, 'w').close()
    open(factOriginal_csvPATH, 'w').close()
    open(factDimensions_csvPATH, 'w').close()
    open(inputCsvPathFAKE,'w').close()
    for year, fragFile in fragTablesPath.items():
        open(fragFile.filePath,'w').close()

    # Start (Refresh materialized view)
    clockTimeMatView = time.time()

    cur.execute("REFRESH MATERIALIZED VIEW intervention_daytime")
    cur.execute("REFRESH MATERIALIZED VIEW intervention_nighttime")

    # End (Refresh materialized view)
    elapsedTimeMatView=elapsedTimeMatView + (time.time() - clockTimeMatView)

    conn.commit()

    # End (Loading)
    elapsedTimeLoading = elapsedTimeLoading + (time.time() - clockTimeLoading)
    print("Fine ETL (sec): %s" % (time.time() - start_local_time))

    queryTester.computeAndWriteAvgs(csvIteration,'NoIndex')

    #Creating index (Start)
    clockTimeIndex=time.time()
    queryTester.createIndex()
    elapsedTimeIndex=time.time() - clockTimeIndex

    queryTester.computeAndWriteAvgs(csvIteration,'Index')
    queryTester.dropIndex()

    print("+ elapsedTimeExtraction: %s" % elapsedTimeExtraction)
    print("+ elapsedTimeTransformation: %s" % elapsedTimeTransformation)
    print("+ elapsedTimeLoading: %s" % elapsedTimeLoading)
    print("+ elapsedTimeOther: %s" % elapsedTimeOther)
    print("+ elapsedTimeMatView: %s" % elapsedTimeMatView)
    print("+ elapsedTimeIndex: %s" % elapsedTimeIndex)

    print("Righe non valide: %s" % (cntNotValidRows))
    print("Righe valide: %s" % (cntValidRows))

    elapsedTimeExtraction = 0
    elapsedTimeTransformation = 0
    elapsedTimeLoading = 0
    elapsedTimeOther = 0
    elapsedTimeMatView = 0
    cntNotValidRows = 0
    cntValidRows = 0
    elapsedTimeIndex = 0

    print("+++")

queryTester.csvQueryResults.close()

print("Tempo totale per tutti i file (sec): %s" % (time.time() - start_global_time))

