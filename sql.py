import MySQLdb
import codecs
import re
import sys

db_host = "147.46.15.66"
db_user = "qmopla"
db_pw = "bde1234"
db_name = "qmopla"
connect = MySQLdb.connect(db_host, db_user, db_pw, db_name)
connect.set_character_set('utf8')
connect.autocommit = True
c = connect.cursor()



def read_log():
    f = codecs.open("recovery.log", "r", "utf-8")
    lines = f.readlines()
    #"""checkpoint가 있는 줄을 읽기위한 정규표현식을 아래와 같이 나타냄"""
    ex = re.compile("([a-zA-Z]{10})\s+([\W+\w+\W+]+)")
    #"""모든 라인을 하나씩 읽어들이면서 정규표현식에 부합하지 않는 None이 나오면 계속 넘어가고 표현식에 맞으면 해당하는 checkpoint를 리스트에넣음"""
    for line in lines:
        ex1 = ex.search(line)
        if ex1 == None:
            continue
        elif ex1.group(1) == 'checkpoint':
            checkpoint = ex1.group(2).replace(" ","").strip().split(",")
            #"""checkpoint의 트랜잭션만을 뽑아서 리스트로 저장시켰다. checkpoint는 리스트로 저장되어있음 각각의 트랜잭션은 스트링으로"""
            redo(checkpoint)
    #print(checkpoint)


def redo(li):
    checkpoint = li
    f = codecs.open("recovery.log", "r", "utf-8")
    lines = f.readlines()
    redo_lines = []
    for idx,line in enumerate(lines): #체크 포인트가있는 인덱스를 뽑아내기위해 enumerate해서 인덱스를 뽑고 그 이후의 라인들을 redo라인으로 설정해주자.
        if line.strip().startswith("checkpoint"):
            for new_line in lines[idx+1:]:
                redo_lines.append(new_line.strip().replace(".",",")) #redo라인을 만들면서 \t\n과같은 내용은 없애주고 .도 ,로 바꾸어서 나중에 split을 용이하게하자.
    #print(redo_lines)
    ex = re.compile("(\W+\w+\W+)\s([\w+\W]+)")
    for redo_line in redo_lines: #모든 redo라인을 순차적으로 돌리자.
        ex1 = ex.search(redo_line)
        transaction = ex1.group(1) #이건 하나하나redo라인의 transaction 번호
        sql_need = ex1.group(2).split(",") #이건 그 redo라인에서 수행하야 하는 sql내용
        if (transaction in checkpoint) and len(sql_need) != 1 :
            if len(sql_need) == 5: #"""recovey log중 oldvalue를 가진경우 처리"""
                primary_sql = "show fields from %s where `Key` = 'PRI'"%(sql_need[0])
                c.execute(primary_sql)
                primary_key = c.fetchone()
                #print(primary_key[0])
                sql = ("update %s set %s = '%s' where %s = '%s'"%(sql_need[0], sql_need[2], sql_need[4], primary_key[0], sql_need[1]))
                c.execute(sql)
            elif len(sql_need) == 4: #"""recovey log중 oldvalue가 없는경우 처리"""
                primary_sql = "show fields from %s where `Key` = 'PRI'" % (sql_need[0])
                c.execute(primary_sql)
                primary_key = c.fetchone()
                #print(primary_key[0])
                sql = ("update %s set %s = '%s' where %s = '%s'"%(sql_need[0], sql_need[2], sql_need[3], primary_key[0], sql_need[1]))
                c.execute(sql)
            else:
                print("recovey log length is something wrong plz check")

        elif (transaction in checkpoint) and len(sql_need) == 1: #commit이나 abort를 만나면 checkpoint리스트에서 삭제해주자
            if sql_need[0] == 'commit' or sql_need[0] == 'abort' :
                checkpoint.remove(transaction)
            else : continue
        elif (transaction not in checkpoint): #start를 만나면 checkpoint리스트에 넣어주자
            if sql_need[0] == 'start':
                checkpoint.append(transaction)

    if not checkpoint:
        end()

    else:
        undo(checkpoint)

def undo(li):
    checkpoint = li
    f = codecs.open("recovery.log", "r", "utf-8")
    lines_redo = f.readlines()
    undo_lines = []
    for line in lines_redo[::-1]: #처음부터 끝까지의 recovery log를 뒤집어서 하나씩 뽑아내자.
        undo_lines.append(line.strip().replace(".",",")) #\t\n과같은 내용은 없애주고 .도 ,로 바꾸어서 나중에 split을 용이하게하자.
    #print(undo_lines)
    ex = re.compile("(\W+\w+\W+)\s([\w+\W]+)")
    for undo_line in undo_lines:
        ex1 = ex.search(undo_line)
        transaction = ex1.group(1)
        sql_need = ex1.group(2).split(",")
        if transaction not in checkpoint:
            continue
        elif (transaction in checkpoint) and len(sql_need) != 1 :
            if len(sql_need) == 5: #"""recovey log중 oldvalue를 가진경우 처리"""
                primary_sql = "show fields from %s where `Key` = 'PRI'"%(sql_need[0])
                c.execute(primary_sql)
                primary_key = c.fetchone()
                #print(primary_key[0])
                sql = ("update %s set %s = '%s' where %s = '%s'"%(sql_need[0], sql_need[2], sql_need[4], primary_key[0], sql_need[1]))
                c.execute(sql)
            elif len(sql_need) == 4: #"""recovey log중 oldvalue가 없는경우 처리"""
                primary_sql = "show fields from %s where `Key` = 'PRI'" % (sql_need[0])
                c.execute(primary_sql)
                primary_key = c.fetchone()
                #print(primary_key[0])
                sql = ("update %s set %s = '%s' where %s = '%s'"%(sql_need[0], sql_need[2], sql_need[3], primary_key[0], sql_need[1]))
                c.execute(sql)
        elif (transaction in checkpoint) and len(sql_need) == 1 :
            if sql_need[0] == 'start':
                checkpoint.remove(transaction)

    if not checkpoint:
        end()
    else: print("Something is Wrong!! Check Code or recovery log.")


def end():
    print("All recovery log is done. system end.")
    sys.exit()



read_log()




