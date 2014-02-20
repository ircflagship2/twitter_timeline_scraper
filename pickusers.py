import simplejson as json
import fileinput
import sys
import subprocess
from sets import Set

def runProcess(exe):    
    p = subprocess.Popen(exe, stdout=subprocess.PIPE, shell=True)
    while(True):
        retcode = p.poll() #returns None while subprocess is running
        line = p.stdout.readline()
        yield line
        if(retcode is not None):
            print "DONE"
            break

files = sys.argv[1:]

users = Set()

for file in files:    
    print file
    for line in runProcess(['./streamusers.sh ' + file]):
        try:
            twitterJson = json.loads("[" + line[1:] + "]")
            for tweet in twitterJson:                            
                location = '_'.join(file.split('_')[1:]).replace('.lzo','')
                userid = tweet['user']['id']

                if userid not in users:
                    users.add(userid)
                    print str(userid) + "," + location
        except:
            print line