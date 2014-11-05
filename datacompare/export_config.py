import os
import pyodbc
import xlsxwriter

def returnquery(table,columns):  
    return 'select ' + ",".join([str(x) for x in columns]) +' from ' + table + ' with(nolock)'
if __name__== '__main__':
    try:
        cnxn = pyodbc.connect('DSN=sqlpy40;UID=sa;PWD=Dallas1!')      
        cursor1 = cnxn.cursor()
        tables = []        
        cursor1.execute("select tab_name,col_name,tab_title,col_title from config_tab c WITH(NOLOCK)  \
                        inner join config_col cc WITH(NOLOCK) on cc.config_tab_idn = c.config_tab_idn \
                        where col_name not in ('source','crt_dt','upd_dt','user_idn','is_default','is_required') \
                        and tab_name not in ('code_cpt_modifier','code_diag','code_proc','user_type','config_note_type')\
                        and c.entity_active = 'Y'  \
                        and cc.entity_active ='Y' order by tab_name")
        rows = cursor1.fetchall()
        for row in rows:        
            tables.append([x.split(',')[0] for x in row])    
        tablelist = list(set([k[0] for k in tables]))
        tabtitlelist = list(set([k[2] for k in tables]))
        tablelist.sort()
        workbook = xlsxwriter.Workbook('code_table_data.xlsx')    
        for table in tablelist:
            try:
                print 'processing table : '+table+'..'
                tables = []
                data = []
                for row in rows:        
                    tables.append([x.split(',')[0] for x in row])  
                columns = [k[1] for k in tables if k[0] == table] 
                column_list = [k[3] for k in tables if k[0] == table]                 
                s = returnquery(table,columns)     
                cursor1.execute(s)
                rowset = cursor1.fetchall()             
                for row in rowset:
                    tmp = []
                    tmp.append([str(x) for x in row])
                    data.append(tmp)                           
                tmplist = [y[0] for y in[x for x in data]]                
                worksheet = workbook.add_worksheet(table[:31])        
                map((lambda x: worksheet.write(0, column_list.index(x), x[:31])),column_list)    
                for item in tmplist:                                          
                    for p in item:
                        worksheet.write(tmplist.index(item)+1, item.index(p), p.encode('utf-8','ignore'),)                           
                print '['+str(tablelist.index(table))+ '] '+table+' data successfully exported'                       
            except Exception ,e:
                print str(e) 
                pass
        workbook.close()    
    except Exception ,e:
        print str(e) 
                        
        
