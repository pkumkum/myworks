import sys
from operator import add
from pyspark import SparkContext
from datetime import datetime, timedelta
import datetime

def getrulesfrompackage(pkg):
    packages = {'asthma':[[11,['23'],'=','string','|'],[27,['2001-01-19'],'>=','datetime','|']]}
    return packages.get(pkg)

def evaluate(values):
    '''values example :-[index of col in data passed,[list of values to be checked],operator to be used,data type,separator in csv,csv line data]'''
    if values == None:
        return False
    else:        
        flag = False
        lstvalue = values[1]
        strvalcol = values[0]
        separator = values [4]
        data = values[5]
        if values[3] == 'datetime':
            strkey =  datetime.datetime.strptime(data.split(separator)[strvalcol], '%Y-%m-%d').date()
            strstdtval = datetime.datetime.strptime(lstvalue[0], '%Y-%m-%d').date()            
            if values[2] == '>':
                if strkey > strstdtval:
                    flag = True
            elif values[2] == '<':
                if strkey < strstdtval:
                    flag = True
            elif values[2] == '>=':
                if strkey >= strstdtval:
                    flag = True
            elif values[2] == '<=':
                if strkey <= strstdtval:
                    flag = True
            elif values[2] == '>=<=':
                strenddtval = datetime.datetime.strptime(lstvalue[1], '%Y-%m-%d').date()
                if strkey >= strstdtval and strkey <= strenddtval:
                    flag =True
        elif values[3] == 'string':          
            if values[2] == '=':
                if data.split(separator)[strvalcol]== lstvalue[0]:
                    flag = True
            elif values[2] == 'in':
                if data.split(separator)[strvalcol] in lstvalue:
                    flag = True
            elif data.split(separator)[strvalcol] == 'not in':
                if values[0] not in lstvalue:
                    flag = True            
        elif values[3] == 'int':
            strintkey = int(data.split(separator)[strvalcol])
            strintval = int(value[0])
            strintendval = int(value[1])
            if values[2] == '=':
                if strintkey == strintval:
                    flag = True
            elif values[2] == '>':
                if strintkey > strintval:
                    flag = True
            elif values[2] == '<':
                if strintkey < strintval:
                    flag = True
            elif values[2] == '>=':
                if strintkey >= strintval:
                    flag = True
            elif values[2] == '<=':
                if strintkey <= strintval:
                    flag = True
            elif values[2] == '>=<=':
                if strintkey >= strintval and strintkey <= strintendval:
                    flag =True
        return flag                

def getval(values,pkg):    
    lst = getrulesfrompackage(pkg)
    data = []
    for items in lst: 
        items.append(values)        
        data.append(items)    
    return all([evaluate(x) for x in data])

if __name__ == "__main__":    
    sc = SparkContext(appName="claimCount")
    lines = sc.textFile(sys.argv[1], 1)    
    counts = lines.map(lambda x:x.split('|')[1] if getval(x,'asthma') else None) \
            .map(lambda x: (x, 1)) \
            .reduceByKey(add) 
    output = counts.collect()
    ##filter for members with count >2
    a = filter(lambda x: x if x[0] != None and x[1] > 2 else None,output)
    for (word, count) in a:
        print "%s: %i" % (word, count)
