import pymysql
import os 

def insert_db(table ,data):
    db = pymysql.connect('localhost', 'root', 'root', 'IoMafelt')
    cusor = db.cursor()
    sql_exports = """INSERT INTO `exports` (`exportid`,`sampleid`,`name`) VALUES (%s,%s,%s)"""
    sql_imports = """INSERT INTO `imports` (`importid`,`sampleid`,`library`,`name`) VALUES (%s, %s ,%s, %s)"""
    sql_segments = """INSERT INTO `segments` (`segid`,`sampleid`,`segname`) VALUES (%s, %s, %s)"""
    sql_syscalls = """INSERT INTO `syscalls` (`syscallid`,`sampleid`,`name`) VALUES (%s,%s,%s)"""
    if(table == 'exports'):
        try: 
            cusor.executemany(sql_exports,data) 
            db.commit()
        except Exception as e: 
            print(e)
    elif(table == 'imports'):
        try:
            cusor.executemany(sql_imports,data) 
            db.commit()
        except Exception as e: 
            print(e)
    elif(table == 'syscalls'):
        try:
            cusor.executemany(sql_syscalls,data) 
            db.commit() 
        except Exception as e: 
            print(e)
    elif(table == 'segments'):
        try:
            cusor.executemany(sql_segments,data) 
            db.commit() 
        except Exception as e: 
            print(e)
    else:
        print('Unknown table name!')
    db.close()

def deal_exports(path):
    data = []
    sampleid = 1
    for root_dir_path, dir_names, file_names in os.walk(path):
        for single_file in file_names:
            exportid = single_file[:-4]
            path_exports = os.path.join(root_dir_path, single_file)
            if(os.path.getsize(path_exports)!=0):
                file = open(path_exports)
                for line in file:
                    list = []
                    list.append(sampleid)
                    list.append(exportid)
                    line = line.strip('\n')
                    if(len(line)!=0):
                        list.append(line)
                        sampleid += 1
                        data.append(list)
                file.close()
    return data

def deal_segments(path):
    data = []
    sampleid = 1
    for root_dir_path, dir_names, file_names in os.walk(path):
        for single_file in file_names:
            segid = single_file[:-4]
            path_segment = os.path.join(root_dir_path, single_file)
            if(os.path.getsize(path_segment)!=0):
                file = open(path_segment)
                for line in file:
                    list = []
                    list.append(sampleid)
                    list.append(segid)
                    line = line.strip('\n')
                    if(len(line)!=0):
                        list.append(line)
                        data.append(list)
                        sampleid += 1
                file.close()
    return data

def deal_imports(path):
    data = []
    sampleid = 1
    for root_dir_path, dir_names, file_names in os.walk(path):
        for single_file in file_names:
            segid = single_file[:-4]
            path_segment = os.path.join(root_dir_path, single_file)
            if(os.path.getsize(path_segment)!=0):
                file = open(path_segment)
                for line in file:
                    list = []
                    list.append(sampleid)
                    list.append(segid)
                    line = line.strip('\n')
                    if(len(line)!=0):
                        for i in line.split(' ',1):
                            list.append(i)
                        sampleid += 1
                        data.append(list)
                file.close()
    return data

def base_tables(table):
    path = "/home/whr/IoMafelt/data/result/Info"
    if(table == 'exports'):
        data_export = deal_exports(path+'/Exports')
        insert_db('exports', data_export)
    elif(table == 'imports'):
        data_import = deal_imports(path+'/Imports')
        insert_db('imports', data_import)
    elif(table == 'segments'):
        data_segment = deal_segments(path+'/Segment')
        insert_db('segments', data_segment)
    elif(table == 'syscalls'):
        data_export = deal_exports(path+'/Syscall')
        insert_db('syscalls', data_export)
    else:
        print('Unknown table name!')

def get_importname():
    #db = pymysql.connect('localhost', 'root', '', 'excel')
    db = pymysql.connect(host='localhost', user='root', passwd='root', db='IoMafelt')
    cusor = db.cursor()
    sql = "select name,library,count(*) from imports group by name"
    try:
        cusor.execute(sql)
    except Exception as e:
        print(e)
    db.close()
    return cusor.fetchall()

def get_libname():
    db = pymysql.connect(host='localhost', user='root', passwd='root', db='IoMafelt')
    cusor = db.cursor()
    sql = "select library,count(*) from imports group by library"
    try:
        cusor.execute(sql)
    except Exception as e:
        print(e)
    db.close()
    return cusor.fetchall()

def get_segname():
    #db = pymysql.connect('localhost', 'root', '', 'excel')
    db = pymysql.connect(host='localhost', user='root', passwd='root', db='IoMafelt')
    cusor = db.cursor()
    sql = "select segname,count(*) from segments group by segname"
    try:
        cusor.execute(sql)
    except Exception as e:
        print(e)
    db.close()
    return cusor.fetchall()

def get_exportname():
    #db = pymysql.connect('localhost', 'root', '', 'excel')
    db = pymysql.connect(host='localhost', user='root', passwd='root', db='IoMafelt')
    cusor = db.cursor()
    sql = "select name,count(*) from exports group by name"
    try:
        cusor.execute(sql)
    except Exception as e:
        print(e)
    db.close()
    return cusor.fetchall()

def insert_db_importcount(data):
    #db = pymysql.connect('localhost', 'root', '', 'excel')
    db = pymysql.connect(host='localhost', user='root', passwd='root', db='IoMafelt')
    cusor = db.cursor()
    sql = """INSERT INTO `import_count` (`importname`,`LuaBot`,`ExploitKit`,`mrblack`,`aesddos`,`Spike`,`benign`,`Elknot`,`Masuta`,`Backdoor.Linux.Mirai.b`,`Persirai`,`Backdoor.Linux.Mirai.c`,`Mirai.AT`,`Backdoor.Linux.Mirai.a`,`Mirai.x`,`Satori`,`Mirai.AT!TR`,`IoTroop`,`OMG`,`Hajime`,`Backdoor.Linux.Mirai.r`,`Backdoor.Linux.Mirai.n`,`GoScanSSH`,`Stuxnet`,`Qbot`,`VPNFilter`,`Xor`,`RBOT`,`Aidra`,`Psybot`,`Amnesia`,`Kaiten`,`total`) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""    
    try:
        cusor.execute(sql,data)
        db.commit() 
    except Exception as e:
        print(e)
    db.close()

def insert_db_libcount(data):
    db = pymysql.connect(host='localhost', user='root', passwd='root', db='IoMafelt')
    cusor = db.cursor()
    sql = """INSERT INTO `lib_count` (`lib_name`,`LuaBot`,`ExploitKit`,`mrblack`,`aesddos`,`Spike`,`benign`,`Elknot`,`Masuta`,`Backdoor.Linux.Mirai.b`,`Persirai`,`Backdoor.Linux.Mirai.c`,`Mirai.AT`,`Backdoor.Linux.Mirai.a`,`Mirai.x`,`Satori`,`Mirai.AT!TR`,`IoTroop`,`OMG`,`Hajime`,`Backdoor.Linux.Mirai.r`,`Backdoor.Linux.Mirai.n`,`GoScanSSH`,`Stuxnet`,`Qbot`,`VPNFilter`,`Xor`,`RBOT`,`Aidra`,`Psybot`,`Amnesia`,`Kaiten`,`total`) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""    
    try:
        cusor.execute(sql,data)
        db.commit() 
    except Exception as e:
        print(e)
    db.close()
    
def insert_db_segcount(data):
    #db = pymysql.connect('localhost', 'root', '', 'excel')
    db = pymysql.connect(host='localhost', user='root', passwd='root', db='IoMafelt')
    cusor = db.cursor()
    sql = """INSERT INTO `seg_count` (`segname`,`LuaBot`,`ExploitKit`,`mrblack`,`aesddos`,`Spike`,`benign`,`Elknot`,`Masuta`,`Backdoor.Linux.Mirai.b`,`Persirai`,`Backdoor.Linux.Mirai.c`,`Mirai.AT`,`Backdoor.Linux.Mirai.a`,`Mirai.x`,`Satori`,`Mirai.AT!TR`,`IoTroop`,`OMG`,`Hajime`,`Backdoor.Linux.Mirai.r`,`Backdoor.Linux.Mirai.n`,`GoScanSSH`,`Stuxnet`,`Qbot`,`VPNFilter`,`Xor`,`RBOT`,`Aidra`,`Psybot`,`Amnesia`,`Kaiten`,`total`) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    try:
        cusor.execute(sql,data)
        db.commit()
    except Exception as e:
        print(e)
    db.close()
    
def insert_db_exportcount(data):
    #db = pymysql.connect('localhost', 'root', '', 'excel')
    db = pymysql.connect(host='localhost', user='root', passwd='root', db='IoMafelt')
    cusor = db.cursor()
    sql = """INSERT INTO `export_count` (`exportname`,`LuaBot`,`ExploitKit`,`mrblack`,`aesddos`,`Spike`,`benign`,`Elknot`,`Masuta`,`Backdoor.Linux.Mirai.b`,`Persirai`,`Backdoor.Linux.Mirai.c`,`Mirai.AT`,`Backdoor.Linux.Mirai.a`,`Mirai.x`,`Satori`,`Mirai.AT!TR`,`IoTroop`,`OMG`,`Hajime`,`Backdoor.Linux.Mirai.r`,`Backdoor.Linux.Mirai.n`,`GoScanSSH`,`Stuxnet`,`Qbot`,`VPNFilter`,`Xor`,`RBOT`,`Aidra`,`Psybot`,`Amnesia`,`Kaiten`,`total`) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""    
    try:
        cusor.execute(sql,data)
        db.commit() 
    except Exception as e:
        print(e)
    db.close()

def get_importcount(import_list):
    global error
    #db = pymysql.connect('localhost', 'root', '', 'excel')
    db = pymysql.connect(host='localhost', user='root', passwd='root', db='IoMafelt')
    cusor = db.cursor()
    data = []
    for name in import_list:
        sql = "select familyid,count(*) from (select imports.importid,imports.name,samples.familyid from imports inner join samples on samples.sampleid = imports.sampleid) as newtable where newtable.name = '%s' group by familyid" %(name[0],)
        #print(sql)
        list = []
        list.append(name[0])
        pre = 1
        try:
            cusor.execute(sql)
            family_count = cusor.fetchall()
            #print(family_count)
            for one in family_count:
                now = one[0]
                for i in range(pre,now):
                    list.append(0)
                list.append(one[1])
                pre = now + 1
            if(pre<32):
                for i in range(pre,32):
                    list.append(0)
            list.append(name[1])
            #print(list) 
            if(len(list)!=33):
                print(list)  
            data.append(list)
        except Exception as e:
            print(e)
    return data 
    
def get_libcount(lib_list):
    global error
    db = pymysql.connect(host='localhost', user='root', passwd='root', db='IoMafelt')
    cusor = db.cursor()
    data = []
    for name in lib_list:
        sql = "select familyid,count(*) from (select imports.importid,imports.library,samples.familyid from imports inner join samples on samples.sampleid = imports.sampleid) as newtable where newtable.name = '%s' group by familyid" %(name[0],)
        list = []
        list.append(name[0])
        pre = 1
        try:
            cusor.execute(sql)
            family_count = cusor.fetchall()
            #print(family_count)
            for one in family_count:
                now = one[0]
                for i in range(pre,now):
                    list.append(0)
                list.append(one[1])
                pre = now + 1
            if(pre<32):
                for i in range(pre,32):
                    list.append(0)
            list.append(name[1])
            print(list) 
            if(len(list)!=33):
                print(list)  
            data.append(list)
        except Exception as e:
            print(e)
    return data  

def get_segcount(seg_list):
    global error
    #db = pymysql.connect('localhost', 'root', '', 'excel')
    db = pymysql.connect(host='localhost', user='root', passwd='root', db='IoMafelt')
    cusor = db.cursor()
    data = []
    for name in seg_list:
        sql = "select familyid,count(*) from (select segments.segid,segments.segname,samples.familyid from segments inner join samples on samples.sampleid = segments.sampleid) as newtable where newtable.segname = '%s' group by familyid" %(name[0],)
        #print(sql)
        list = []
        list.append(name[0])
        pre = 1
        try:
            cusor.execute(sql)
            family_count = cusor.fetchall()
            #print(family_count)
            for one in family_count:
                now = one[0]
                for i in range(pre,now):
                    list.append(0)
                list.append(one[1])
                pre = now + 1
            if(pre<32):
                for i in range(pre,32):
                    list.append(0)
            list.append(name[1])
            #print(list) 
            if(len(list)!=33):
                print(list)
            data.append(list)
        except Exception as e:
            print(e)
    return data
    
def get_exportcount(export_list):
    global error
    #db = pymysql.connect('localhost', 'root', '', 'excel')
    db = pymysql.connect(host='localhost', user='root', passwd='root', db='IoMafelt')
    cusor = db.cursor()
    data = []
    for name in export_list:
        sql = "select familyid,count(*) from (select exports.exportid,exports.name,samples.familyid from exports inner join samples on samples.sampleid = exports.sampleid) as newtable where newtable.name = '%s' group by familyid" %(name[0],)
        #print(sql)
        list = []
        list.append(name[0])
        pre = 1
        try:
            cusor.execute(sql)
            family_count = cusor.fetchall()
            #print(family_count)
            for one in family_count:
                now = one[0]
                for i in range(pre,now):
                    list.append(0)
                list.append(one[1])
                pre = now + 1
            if(pre<32):
                for i in range(pre,32):
                    list.append(0)
            list.append(name[1])
            #print(list) 
            if(len(list)!=33):
                print(list)  
            data.append(list)
        except Exception as e:
            print(e)
    return data  

def count_tables(table):
    if(table == 'export_count'):
        export_list = get_exportname()
        exportcount_data = get_exportcount(export_list)
        for d in exportcount_data:
            insert_db_exportcount(d)
    elif(table == 'import_count'):
        import_list = get_importname()
        importcount_data = get_importcount(import_list)
        for d in importcount_data:
            insert_db_importcount(d)
    elif(table == 'lib_count'):
        lib_list = get_libname()
        lib_data = get_libcount(lib_list)
        for d in lib_data:
            insert_db_libcount(d)
    elif(table == 'seg_count'):
        seg_list = get_segname()
        seg_data = get_segcount(seg_list)
        for d in seg_data:
            insert_db_segcount(d)
            
def other_segments(path):
    data = []
    sampleid = 437693
    file = open(path)
    for line in file:
        list = []
        list.append(sampleid)
        list.append(line.strip('\n'))
        list.append('NOBODY')
        data.append(list)
        sampleid += 1
    file.close
    return data
    
if __name__ == '__main__':
    print('1')