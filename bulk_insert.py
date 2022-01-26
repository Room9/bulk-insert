#7.2만 row 가공해 288만 row insert
#40000row 10분 -> 4.8초 개선

# mysql db connection
import pymysql

host = 'db host'
port = 3306
db_name = 'db name'
conn = pymysql.connect(host=host, port=int(port), user='dbuser', password='', db='mealgen', charset='utf8')
curs = conn.cursor()

# 1. [개별 insert] data 1row 당 execute + commit 진행
# db부하로 connection 끊김

import pandas as pd
import time

start = time.time()

data = pd.read_csv('/Users/happytoseeyou/Desktop/FL20.csv')

for t in range(0,len(data),1000):
    for j in range(t-1000,t):
        if type(data['TAG_CODE'][j]) == str:
            gender = data['GENDER_TYPE'][j]
            age = data['AGE_GROUP'][j]
            std_code = data['STD_MENU_CODE'][j]
            theme_code = 'TM%s000' % data['TAG_CODE'][j][2]
            tag_code = data['TAG_CODE'][j]

            q = "INSERT INTO STD_MENU_LABEL (STD_MENU_CODE, AGE_GROUP, GENDER_TYPE, TAG_CODE, THEME_CODE) VALUES('%s', %s, '%s', '%s', '%s');" % (std_code, age, gender, tag_code, theme_code)
            curs.execute(q)
            conn.commit()

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

# 2. [개별 insert] data 1row 당 execute 진행 -> execute staging 40000개 모아서 일괄 commit
# 40000 row 당 10분

import pandas as pd
import time

start = time.time()

data = pd.read_csv('/Users/happytoseeyou/Desktop/FL20.csv')

for t in range(0,len(data),1000):
    for j in range(t-1000,t):
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

# 3. [bulkinsert] data 40row 당 execute 진행 -> execute staging 1000개 모아서 일괄 commit
# 40000row 당 49초

import pandas as pd
import time

start = time.time()

data = pd.read_csv('/Users/happytoseeyou/Desktop/FL20.csv')

for t in range(0,len(data),1000):
    for j in range(t-1000,t):
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

# 4. [bulkinsert] data 40000개 당 executemany + commit 진행
# 40000row 당 4초
import pandas as pd
import time

start = time.time()

data = pd.read_csv('/Users/happytoseeyou/Desktop/FL40.csv')
temp = []

for t in range(0,len(data),1000):
    for j in range(t-1000,t):

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
            
        q = "INSERT INTO STD_MENU_LABEL (STD_MENU_CODE, AGE_GROUP, GENDER_TYPE, TAG_CODE, THEME_CODE) VALUES(%s, %s, %s, %s, %s)"
        curs.executemany(q, temp)
        conn.commit()
        print(time.time() - start)

#connection close로 db부하 연결없이 하자
conn.close()