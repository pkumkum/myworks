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
        cursor1.execute("select tab_name,col_name,data_type from config_tab c WITH(NOLOCK)  \
                        inner join config_col cc WITH(NOLOCK) on cc.config_tab_idn = c.config_tab_idn \
                        where col_name not in ('source','crt_dt','upd_dt','user_idn','is_default','is_required') \
                        and tab_name not in ('code_cpt_modifier','code_diag','code_proc','user_type','config_note_type')\
                        and c.entity_active = 'Y' \
                        and cc.entity_active ='Y' order by tab_name")
        rows = cursor1.fetchall()
        for row in rows:        
            tables.append([x.split(',')[0] for x in row])    
        tablelist = list(set([k[0] for k in tables]))
        tablelist.sort()
        tables = []
        workbook = xlsxwriter.Workbook('code_table_data_diff.xlsx')    
        for table in tablelist:
            try:
                print 'processing table : '+table+'..'
                tables = []
                data1 = []
                data2 = []
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
                for row in rowset2:
                    tmp = []
                    tmp.append([str(x) for x in row])
                    data2.append(tmp)
                #------------------------------------
                tmplist1 = [y[0] for y in[x for x in data1 if x not in data2]]
                tmplist2=  [y[0] for y in[x for x in data2 if x not in data1]]
                if len(tmplist1)>0 or len(tmplist2)>0:                
                    worksheet = workbook.add_worksheet(table)        
                    map((lambda x: worksheet.write(0, columns.index(x), x)),columns)    
                    for item in tmplist1:                                          
                        for p in item:
                            worksheet.write(tmplist1.index(item)+1, item.index(p), p.encode('utf-8','ignore'),)                        
                    #---------------------target-----       
                    map((lambda x: worksheet.write(0, columns.index(x)+ len(tmplist1[0])+ 1, x)),columns)
                    for item in tmplist2:               
                        for p in item:    
                            worksheet.write(tmplist2.index(item)+1, len(tmplist1[0])+item.index(p)+1, p.encode('utf-8','ignore'))   
                    print table + ' data successfully exported' 
            except Exception ,e:
                print str(e) 
                pass
        workbook.close()    
    except Exception ,e:
        print str(e) 
                        
        