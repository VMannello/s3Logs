import boto3
import re
import sqlite3
from multiprocessing.dummy import Pool as ThreadPool
from datetime import datetime
import requests
import ipaddress
from collections import defaultdict
from os.path import isfile, getsize

# This file will build / update the sqlite database that is used to respond to queries.

# Things to do:
# Build front end and gnuicorn api
# Need to add key for ipinfo.io if user has one
# Need to add update timeframe to refresh IP database
# Currently the threaded processes use the same number of threads, maybe adjust depending on length of list, have variable be more like "max threads"
# Add a database to sqlite that has all the computational meterics, threads used, execution speed, etc etc

tcount = 44  # Thread variable, 44 seems to work well

start = datetime.now().timestamp()

s3 = boto3.resource('s3')
bucket = s3.Bucket('forgelog')
cols = re.compile(r'(?P<owner>\S+) (?P<bucket>\S+) (?P<time>\[[^]]*\]) (?P<ip>\S+) (?P<requester>\S+) (?P<reqid>\S+) (?P<operation>\S+) (?P<key>\S+) (?P<request>"[^"]*") (?P<status>\S+) (?P<error>\S+) (?P<bytes>\S+) (?P<size>\S+) (?P<totaltime>\S+) (?P<turnaround>\S+) (?P<referrer>"[^"]*") (?P<useragent>"[^"]*") (?P<version>\S)')


def isSQLite3(filename):
    #Check if database exists
    if not isfile(filename):
        return False
    if getsize(filename) < 100:
        return False
    with open(filename, 'rb') as fd:
        header = fd.read(100)
    return header[0:16] == b'SQLite format 3\x00'


def chunks(l, n=8):
    #Yield n-sized chunks from l
    for i in range(0, len(l), n):
        yield l[i:i+n]


def str2dict(s, f):
    x = cols.match(s)
    if x is not None:
        x = x.groupdict()
        x.update({'logfile': f})
        return x
    else:
        return


def getContents(key):
    fileObj = s3.Object(bucket.name, key)
    strs = str(fileObj.get()["Body"].read()).split('\\n')
    group = []
    for oneLine in strs:
        if (len(oneLine) > 3):
            group.append(str2dict(oneLine, key))
    return group


def ipGeo(ip):
    thisObj = defaultdict(lambda: None)
    thisObj['ipString'] = ip
    if (ipaddress.ip_address(ip).is_private):
        thisObj['lastUpdate'] = datetime.now().timestamp()
        thisObj['hostname'] = 'Private'
    else:
        resp = requests.get('http://ipinfo.io/' + thisObj['ipString'] + '/json')
        thisObj.update(resp.json())
    thisObj['ip'] = int(ipaddress.IPv4Address(ip))
    return thisObj


#Create/Connect Database
#Need to get last update
mark = ()

if isSQLite3('s3LogDB.db'):
    conn = sqlite3.connect('s3LogDB.db')
    cur = conn.cursor()
    cur.execute('''SELECT KEY FROM MARKER ORDER BY TIMESTAMP DESC LIMIT 1''')
    mark = cur.fetchone()

else:
    conn = sqlite3.connect('s3LogDB.db')
    cur = conn.cursor()
    cur.execute('''CREATE TABLE log(id INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT, bucket TEXT, time TEXT, ip TEXT, requester TEXT, reqid TEXT, operation TEXT, key TEXT, request TEXT, status INT, error TEXT, bytes INT, size INT, totaltime INT, turnaround INT, referrer TEXT, useragent TEXT, version TEXT, logfile TEXT)''')
    cur.execute('''CREATE TABLE filelist(id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT)''')
    cur.execute('''CREATE TABLE marker(id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT, timestamp NUMERIC)''')
    cur.execute('''CREATE TABLE ipInfo(ip INTEGER PRIMARY KEY, ipString TEXT, hostname TEXT, city TEXT, region TEXT, country TEXT, loc TEXT, org TEXT, postal TEXT, lastUpdate NUMERIC)''')
    conn.commit()

if type(mark) is tuple and len(mark) > 0:
    listOfKeys = [item.key for item in bucket.objects.filter(Marker=mark[0])]
else:
    listOfKeys = [item.key for item in bucket.objects.all()]

rows2add = len(listOfKeys)

if rows2add > 0:
    cur.executemany('INSERT INTO filelist VALUES(null, ?)', list(zip(listOfKeys, )))
    cur.execute('INSERT INTO marker VALUES(null, :key, :timestamp)', {'key': listOfKeys[-1], 'timestamp': datetime.now().timestamp()})
    conn.commit()
    p = ThreadPool(tcount)
    compiledLog = []
    compiledLog.extend(p.map(getContents, listOfKeys))
    p.close()
    compiledLog = [item for sublist in compiledLog for item in sublist]
    compiledLog = filter(None, compiledLog)

    cur.executemany('INSERT INTO log VALUES (null, :owner, :bucket, :time, :ip, :requester, :reqid, :operation, :key, :request, :status, :error, :bytes, :size, :totaltime, :turnaround, :referrer, :useragent, :version, :logfile)', compiledLog)

    conn.commit()

print("Added : " + str(rows2add) + " Records")
print("Elapsed Time : " + str(datetime.now().timestamp()-start))

#Compare ips from logs with ips we've already cataloged, eventually update from timestamp if older than ??
cur.execute('''SELECT DISTINCT ip FROM log''')
temp1 = [item[0] for item in iter(cur.fetchall())]
cur.execute('''SELECT DISTINCT ipString FROM ipInfo''')
temp2 = [item[0] for item in iter(cur.fetchall())]
listOfIps = list(set(temp1) - set(temp2))

if (len(listOfIps) > 0):
    p = ThreadPool(tcount)
    compiledIPs = p.map(ipGeo, listOfIps)
    p.close()
    cur.executemany('''INSERT OR IGNORE INTO ipInfo VALUES (:ip, :ipString, :hostname, :city, :region, :country, :loc, :org, :postal, :lastUpdate)''', compiledIPs)
    conn.commit()

conn.close()

print("Added : " + str(len(listOfIps)) + " new IPs")
print("Elapsed Time : " + str(datetime.now().timestamp()-start))
