# bulk_insert


<!-- ABOUT THE PROJECT -->
## About The Project

> **bulk_insert_case.py**

data 7.2만 row의 csv를 전처리하여 288만 row data insert \
40000만 row 씩 data insert 시 case 별 소요시간 확인 \
pandas dataframe 이용한 데이터 처리

1. [개별 insert] data 1row 당 execute + commit 진행 \
db부하로 connection 끊김

2. [개별 insert] data 1row 당 execute 진행 -> execute staging 40000개 모아서 일괄 commit \
40000 row 당 10분

3. [bulkinsert] data 40row 당 execute 진행 -> execute staging 1000개 모아서 일괄 commit \
40000row 당 49초

4. [bulkinsert] data 40000개 당 executemany + commit 진행 \
40000row 당 4.8초

(결론) 40000row 10분 -> 4.8초 개선
commit은 lazy하게 동작하는 것이 좋다
대용량일수록 execute보다 executemany를 사용하는것이 좋다

> **bulk_insert_example.py**

가장 실행속도가 빠른 executemany + commit로 데이터 처리 예시


### Built With

- Python3.9
- Aurora MYSQL
- Pandas
- Jupyter Notebook
