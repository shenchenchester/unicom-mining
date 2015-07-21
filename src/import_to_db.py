import arrow
import urlparse
import pymongo
import csv

db = pymongo.MongoClient('mongodb://localhost:27017')['unicom']
filename = '/Users/chester/Downloads/20150629.csv'
f = open(filename, 'r')

csv_reader = csv.reader(f)
csv_reader.next() # skip headline
n = 0
docs = []
id_set = set()
missing_n = 0
for row in csv_reader:
    n+=1
    if n%100000==0:
        print n
        db.unicom.insert(docs)
        docs = []
#     id_set.add(row[1])
    if not (row[0] and row[1] and row[2] and row[3]):
        missing_n += 1
        continue
    hn = urlparse.urlsplit(row[2]).hostname
    dt = arrow.get(row[3]+'+08:00')
    suffix = hn and hn.split('.')[-1] or ''
    docs.append({'id':row[1],
                 'url':row[2],
                 'time':dt.datetime,
                 'hostname':hn,
                 'is_ip':suffix.isdigit(),
                 'is_domain_name':suffix.isalpha()})
    
        
# print len(id_set)
