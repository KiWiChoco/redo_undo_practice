import MySQLdb
import codecs



def read_log():
    f = codecs.open("recovery.log", "r", "utf-8")

    lines = f.readlines()
    checkpoint = []
    for idx,line in enumerate(lines):
        print("log:", line)
        if line.startswith("checkpoint"):
            checkpoints = line[11:].strip().split(",")
            checkpoint.extend(checkpoints)
            print("DO IT: checkpoint")
        else:
            t = line.split()[0]
            oper = line.split()[1]
            if oper == "start":
                print("DO IT: start")
            elif oper == "commit":
                print("DO IT: commit")
            elif oper == "abort":
                print("DO IT: abort")
            else:
                print("DO IT: update SQL")
    print(checkpoint)
read_log()


def read():
    db_host = "147.46.15.66"
    db_user = "qmopla"
    db_pw = "bde1234"
    db_name = "qmopla"
    connect = MySQLdb.connect(db_host, db_user, db_pw, db_name)
    c = connect.cursor()

    c.execute("select stu_id from student")

    data = c.fetchall()
    print(data)

#read()





