import sys
from operator import add
from pyspark import SparkContext
from datetime import datetime, timedelta
import datetime

def getseparator():
    return '|'
def getrulesfrompackage(pkg):
    '''values example :-[index of col in data passed,[list of values to be checked],operator to be used,data type]'''
    '''below example checks for place of svc cd,date of svc begin,diag code,diag position,proc cd'''
    packages = {'asthma':[[13,['23'],'=','string'],[29,['2014-05-01'],'>=','datetime'],[75,['4423'],'in','string'],[73,['1'],'=','string'],[47,['A9579'],'in','string']], 'diabetes':[[]] }
    return packages.get(pkg)
def get_claimcount_per_mbr_per_pkg(pkg):
    '''this is used to check the max claim count per mbr'''
    clmcnt = {'asthma':1,'diabetes':2}
    return clmcnt.get(pkg)
def evaluate(values):
    '''expects date to be in yyyy-mm-dd format always without time,empty dates condition not handled'''
    ''''''
    if values == None:
        return False
    else:        
        flag = False
        lstvalue = values[1]
        strvalcol = values[0]
        separator = getseparator()
        data = values[4]
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
    a = filter(lambda x: x if x[0] != None and x[1] >= get_claimcount_per_mbr_per_pkg('asthma')  else None,output)
    for (mbr,clmcount) in a:
        print "%s: %i" % (mbr, clmcount)

