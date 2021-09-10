#7.2만 row 가공해 86만 row insert
#1000row 10분 -> 1.2만 row 4.8초 개선

# mysql db connection
import pymysql

host = 'mealgen-db-real-cluster.cluster-ch1v7vb7kdnw.ap-northeast-2.rds.amazonaws.com'
port = 3306
db_name = 'mealgen'
conn = pymysql.connect(host=host, port=int(port), user='mealgen', password='', db='mealgen', charset='utf8')
curs = conn.cursor()

#insert 하나 당 query 1개 + commit : memory overflow 말고... db부하로 connection 끊김
import pandas as pd
import time

start = time.time()

data = pd.read_csv('/Users/happytoseeyou/Desktop/FL20.csv')


for j in range(1000,1100):
    if type(data['TAG_CODE'][j]) == str:
        gender = data['GENDER_TYPE'][j]
        age = data['AGE_GROUP'][j]
        std_code = data['STD_MENU_CODE'][j]
        theme_code = 'TM%s000' % data['TAG_CODE'][j][2]
        tag_code = data['TAG_CODE'][j]

        q = "INSERT INTO STD_MENU_LABEL (STD_MENU_CODE, AGE_GROUP, GENDER_TYPE, TAG_CODE, THEME_CODE) VALUES('%s', %s, '%s', '%s', '%s');" % (std_code, age, gender, tag_code, theme_code)
        curs.execute(q)

    for i in range(1, 40):
    
        if type(data['TAG_CODE.%s' %i][j]) == str:
            gender = data['GENDER_TYPE'][j]
            age = data['AGE_GROUP'][j]
            std_code = data['STD_MENU_CODE'][j]
            theme_code = 'TM%s000' % data['TAG_CODE.%s' % i][j][2]
            tag_code = data['TAG_CODE.%s' % i][j]

            q = "INSERT INTO STD_MENU_LABEL (STD_MENU_CODE, AGE_GROUP, GENDER_TYPE, TAG_CODE, THEME_CODE) VALUES('%s', %s, '%s', '%s', '%s');" % (std_code, age, gender, tag_code, theme_code)
            curs.execute(q)
            conn.commit()
print(time.time()-start)

#insert 하나 당 query 1개 + execute staging 모아서 + commit : 1000 row 당 10분
import pandas as pd
import time

start = time.time()

data = pd.read_csv('/Users/happytoseeyou/Desktop/FL20.csv')


for j in range(1000,1100):
    if type(data['TAG_CODE'][j]) == str:
        gender = data['GENDER_TYPE'][j]
        age = data['AGE_GROUP'][j]
        std_code = data['STD_MENU_CODE'][j]
        theme_code = 'TM%s000' % data['TAG_CODE'][j][2]
        tag_code = data['TAG_CODE'][j]

        q = "INSERT INTO STD_MENU_LABEL (STD_MENU_CODE, AGE_GROUP, GENDER_TYPE, TAG_CODE, THEME_CODE) VALUES('%s', %s, '%s', '%s', '%s');" % (std_code, age, gender, tag_code, theme_code)
        curs.execute(q)

    for i in range(1, 40):
    
        if type(data['TAG_CODE.%s' %i][j]) == str:
            gender = data['GENDER_TYPE'][j]
            age = data['AGE_GROUP'][j]
            std_code = data['STD_MENU_CODE'][j]
            theme_code = 'TM%s000' % data['TAG_CODE.%s' % i][j][2]
            tag_code = data['TAG_CODE.%s' % i][j]

            q = "INSERT INTO STD_MENU_LABEL (STD_MENU_CODE, AGE_GROUP, GENDER_TYPE, TAG_CODE, THEME_CODE) VALUES('%s', %s, '%s', '%s', '%s');" % (std_code, age, gender, tag_code, theme_code)
            curs.execute(q)
conn.commit()
print(time.time()-start)

#bulkinsert 40개 기준 1해당하는 것들 모아서 query 1개 + commit : 1000row 당 49초
import pandas as pd
import time

start = time.time()

data = pd.read_csv('/Users/happytoseeyou/Desktop/FL20.csv')


for j in range(1000,2000):
    if type(data['TAG_CODE'][j]) == str:
        gender = data['GENDER_TYPE'][j]
        age = data['AGE_GROUP'][j]
        std_code = data['STD_MENU_CODE'][j]
        theme_code = 'TM%s000' % data['TAG_CODE'][j][2]
        tag_code = data['TAG_CODE'][j]

        q = "INSERT INTO STD_MENU_LABEL (STD_MENU_CODE, AGE_GROUP, GENDER_TYPE, TAG_CODE, THEME_CODE) VALUES('%s', %s, '%s', '%s', '%s');" % (std_code, age, gender, tag_code, theme_code)
        curs.execute(q)

    temp=[]
    for i in range(1, 40):
        if type(data['TAG_CODE.%s' %i][j]) == str:
            gender = data['GENDER_TYPE'][j]
            age = data['AGE_GROUP'][j]
            std_code = data['STD_MENU_CODE'][j]
            theme_code = 'TM%s000' % data['TAG_CODE.%s' % i][j][2]
            tag_code = data['TAG_CODE.%s' % i][j]
            value ="('%s', %s, '%s', '%s', '%s')"%(std_code, age, gender, tag_code, theme_code)
            temp.append(value)
    value = ','.join(temp)
    q = "INSERT INTO STD_MENU_LABEL (STD_MENU_CODE, AGE_GROUP, GENDER_TYPE, TAG_CODE, THEME_CODE) VALUES%s;" % (value)
    curs.execute(q)
conn.commit()
print(time.time()-start)

#bulkinsert 1해당하는 거 전부 모아서 query 1개(executemany(query, list) 이용) + commit : 1000row 당 4초, 12400row 20초(16만개)
#데이터 range 벗어나서 에러발생 시  executemany + commit 하도록 구성
import pandas as pd
import time

start = time.time()

data = pd.read_csv('/Users/happytoseeyou/Desktop/FL40.csv')
temp = []

try: 
    for j in range(0,15000):

        if type(data['TAG_CODE'][j]) == str:
            gender = data['GENDER_TYPE'][j]
            age = data['AGE_GROUP'][j]
            std_code = data['STD_MENU_CODE'][j]
            theme_code = 'TM%s000' % data['TAG_CODE'][j][2]
            tag_code = data['TAG_CODE'][j]
            temp.append((std_code, age, gender, tag_code, theme_code,))

        for i in range(1, 40):
            if type(data['TAG_CODE.%s' % i][j]) == str:
                gender = data['GENDER_TYPE'][j]
                age = data['AGE_GROUP'][j]
                std_code = data['STD_MENU_CODE'][j]
                theme_code = 'TM%s000' % data['TAG_CODE.%s' % i][j][2]
                tag_code = data['TAG_CODE.%s' % i][j]
                temp.append((std_code, age, gender, tag_code, theme_code,))
except:
    q = "INSERT INTO STD_MENU_LABEL (STD_MENU_CODE, AGE_GROUP, GENDER_TYPE, TAG_CODE, THEME_CODE) VALUES(%s, %s, %s, %s, %s)"
    curs.executemany(q, temp)
    conn.commit()
    print(time.time() - start)

#connection close로 db부하 연결없이 하자
conn.close()