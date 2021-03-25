import math
import pymysql
import paramiko
import pandas as pd
import os.path
import datetime as dt
from paramiko import SSHClient
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
import xlsxwriter
">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>enter connection`s form"
`````````sory corporative etique  :D 1```````````````
">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>query enter set"
###
"""pd.set_option('max_rows', 5)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', '{:.2f}'.format)"""
###
intDateStart='20210101'
intDateFin=(dt.datetime.now()-dt.timedelta(days=1)).strftime("%Y%m%d")


query ="""SELECT 
    cr.id 'ІД кредиту',
    cr.user_id 'ІД клієнта',
    u.inn 'ІНН клієнта',
    ROUND(cr.amount) 'Тіло кредиту',
    start_date 'Дата початку',
    IFNULL(fact_return_date,
            'Не закритий') 'Дата повернення',
    CASE
        WHEN is_prolong = 0 THEN 'Пролонгації не було'
        WHEN is_prolong = 1 THEN 'Пролонгація була'
    END AS 'Пролонгація',
    man.manager 'Хто видавав',
    CASE
        WHEN cr.status = 1 THEN 'Активний'
        WHEN cr.status = 2 THEN 'Закритий'
        WHEN cr.status = 3 THEN 'Просрочка'
        WHEN cr.status = 4 THEN 'Пролонгований'
    END AS 'Поточний статус',
    DATE(com.created_at) 'Дата коментарю',
    TIME(com.created_at) 'Час коментарю',
    REPLACE(REPLACE(com.comment,
            '
            ',
            ' '),
        '	',
        ' ') 'Зміст коментарю',
    comment_author 'Автор коментарю',
    coment_url.record_url AS 'Запис'
FROM
    credits cr
        JOIN
    comments AS com ON com.user_id = cr.user_id
        JOIN
    users AS u ON u.id = cr.user_id
        JOIN
    (SELECT 
        cr.id AS credit_id_man,
            CONCAT(man.lastname, ' ', man.firstname, ' ', man.middlename) AS manager
    FROM
        creditone.credits cr
    LEFT JOIN (SELECT 
        *
    FROM
        creditone.comments com
    JOIN (SELECT 
        u.id AS user_id2, MAX(com.id) AS maxId
    FROM
        creditone.users u
    JOIN creditone.comments com ON com.user_id = u.id
    WHERE
        com.manager_id IN (30 , 31, 32, 14123)
    GROUP BY u.id) AS lastCom ON lastCom.maxId = com.id
        AND com.user_id = lastCom.user_id2) AS lastCom ON lastCom.user_id = cr.user_id
    LEFT JOIN creditone.users man ON man.id = lastCom.manager_id
    JOIN creditone.users clients ON clients.id = cr.user_id
    WHERE
        cr.start_date > '20200531') AS man ON man.credit_id_man = cr.id
        JOIN
    (SELECT 
        id AS manager_id,
            CONCAT(usman.lastname, ' ', usman.firstname, ' ', usman.middlename) AS comment_author
    FROM
        users usman) AS com_man ON com_man.manager_id = com.manager_id
        LEFT JOIN
    (SELECT 
        record_url AS record_url,
            caller_user_id AS caller_user_id,
            created_at AS created_at,
            user_id AS user_id_call
    FROM
        phone_callbacks
    WHERE
        record_url IS NOT NULL
            AND type = 'OUTBOUND'
            AND duration > 45) AS coment_url ON coment_url.caller_user_id = com.manager_id
        AND DATE(com.created_at) = DATE(coment_url.created_at)
        AND (TIMESTAMPDIFF(SECOND,
        coment_url.created_at,
        com.created_at)) <= 240
        AND u.id = user_id_call
WHERE
    com.created_at BETWEEN cr.start_date AND IFNULL(fact_return_date, CURDATE())
        AND DATE(com.created_at) between CURDATE()-1 and CURDATE()-1"""


def zapyt(query, db):
	with SSHTunnelForwarder(
		#підключення для запиту з основної
	        (ssh_host, ssh_port),
	        ssh_username=ssh_user,
	        ssh_password='H',
	        remote_bind_address=(sql_hostname, sql_port)) as tunnel:

	    conn = pymysql.connect(host='1', user=sql_username, passwd=sql_password, db=db, port=tunnel.local_bind_port)

	    data = pd.read_sql_query(query, conn)

	    conn.close()

	return(data)


data=zapyt(query, 'creditone')


dataset=data

pd_writer = pd.ExcelWriter(f"C:\\Python\\звіти окк\\ОКК-{str(intDateFin)}.xlsx",'xlsxwriter')


dataset.to_excel(pd_writer, index=False, sheet_name='Table 1')
workbook = pd_writer.book
worksheet = pd_writer.sheets['Table 1']
worksheet.freeze_panes(1,0)
pd_writer.save()
