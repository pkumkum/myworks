import os
import pyodbc
import xlsxwriter

def returnquery(table,columns):  
    return 'select ' + ",".join([str(x) for x in columns]) +' from ' + table + ' with(nolock)'
if __name__== '__main__':
    try:
        cnxn1 = pyodbc.connect('DSN=sqlpy1;UID=kramya;PWD=krishna')
        cnxn2 = pyodbc.connect('DSN=sqlpy2;UID=kramya;PWD=krishna')
        cursor1 = cnxn1.cursor()
        cursor2 = cnxn2.cursor()
        tables = []
        values = []
        cursor1.execute("select tab_name,col_name,data_type from config_tab c WITH(NOLOCK)  \
                        inner join config_col cc WITH(NOLOCK) on cc.config_tab_idn = c.config_tab_idn \
                        where col_name not in ('source','crt_dt','upd_dt','user_idn','is_default','is_required') \
                        and tab_name not in ('code_diag','code_proc') order by tab_name")
        rows = cursor1.fetchall()
        for row in rows:        
            tables.append([x.split(',')[0] for x in row])    
        tablelist = list(set([k[0] for k in tables]))
        tablelist.sort()
        tables = []
        workbook = xlsxwriter.Workbook('code_table_data_diff.xlsx')    
        for table in tablelist:
            try:
                tables = []
                data1 = []
                data2 = []
                dl1 = []
                for row in rows:        
                    tables.append([x.split(',')[0] for x in row])  
                columns = [k[1] for k in tables if k[0] == table] 
                s = returnquery(table,columns)     
                cursor1.execute(s)
                rowset1 = cursor1.fetchall()             
                cursor2.execute(s)
                rowset2 = cursor2.fetchall()         
                for row in rowset1:
                    tmp = []
                    tmp.append([str(x) for x in row])
                    data1.append(tmp)           
                for r in rowset2:
                    tmp = []
                    tmp.append([str(x) for x in r])
                    data2.append(tmp)
                #------------------------------------
                worksheet = workbook.add_worksheet(table)        
                j = 0 
                cnt = 1
                m = 0
                map((lambda x: worksheet.write(0, columns.index(x), x)),columns)        
                for x in data1:
                    k = 0           
                    if x not in data2:
                        for item in x:                                          
                            for p in item:
                                worksheet.write(cnt, k, p,)
                                k = k+1  
                                m = k
                        cnt =cnt + 1     
                #---------------------target-----       
                map((lambda x: worksheet.write(0, columns.index(x)+m + 1, x)),columns)
                cnt = 1       
                for x in data2:               
                    if x not in data1:
                        for item in x:  
                            k = m + 1                    
                            for p in item:    
                                worksheet.write(cnt, k, p)
                                k = k+1
                        cnt = cnt + 1        
                print table + ' data successfully exported'
                #-----------------------------------      
            except Exception ,e:
                print str(e) 
                pass
        workbook.close()    
    except:
        print "error "+str(IOError)
                        
        