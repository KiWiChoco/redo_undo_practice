import MySQLdb
import codecs
import re

db_host = "147.46.15.66"
db_user = "qmopla"
db_pw = "bde1234"
db_name = "qmopla"
connect = MySQLdb.connect(db_host, db_user, db_pw, db_name)
c = connect.cursor()


def read_log():
    f = codecs.open("recovery.log", "r", "utf-8")

    lines = f.readlines()
    #checkpoint가 있는 줄을 읽기위한 정규표현식을 아래와 같이 나타냄
    ex = re.compile("([a-zA-Z]{10})\s+([\W+\w+\W+]+)")
    #모든 라인을 하나씩 읽어들이면서 정규표현식에 부합하지 않는 None이 나오면 계속 넘어가고 표현식에 맞으면 해당하는 checkpoint를 리스트에넣음
    for line in lines:
        ex1 = ex.search(line)
        if ex1 == None:
            continue
        elif ex1.group(1) == 'checkpoint':
            checkpoint = ex1.group(2).replace(" ","").strip().split(",")
            #checkpoint의 트랜잭션만을 뽑아서 리스트로 저장시켰다. checkpoint는 리스트로 저장되어있음 각각의 트랜잭션은 스트링으로
            redo(checkpoint)
    #print(checkpoint)




def redo(li):
    print(li)


read_log()




