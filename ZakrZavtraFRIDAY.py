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
sql_hostname = '127.0.0.1'
sql_username = 'VLegoida'
sql_password = 'HNtSWOi60zz!'
sql_main_database = 'creditone'
sql_reserve_database = 'creditone_1'
sql_port = 3306
ssh_host = '10.1.32.75'
ssh_user = 'VLegoida'
ssh_password = 'HNtSWOi60zz!'
ssh_port = 22
host = '127.0.0.1'

">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>query enter set"
###
"""pd.set_option('max_rows', 5)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', '{:.2f}'.format)"""
###
intDateStart = '20210101'
intDateFin = (dt.datetime.now()-dt.timedelta(days=-1)).strftime("%Y%m%d")+'-'+(dt.datetime.now()-dt.timedelta(days=-3)).strftime("%Y%m%d")

query = """select  ifnull(monthname(c.start_date),'заявка відхилена') as 'Місяць кредиту',
		concat(u1.lastname, ' ', u1.firstname, ' ', u1.middlename) as 'ПІБ клієнта', u1.digits_phone 'Мобільний',
		u1.inn as 'ІПН клієнта', 
		ifnull(cr.credit_id,'заявка відхилена') as 'Id кредита', 
        replace(ifnull(c.amount,0),'.',',') as 'Тіло кредиту, грн', 
                case when c.id is null then 'заявка відхилена' when c.fact_return_date is not null then 'кредит закритий'
			 when c.is_prolong = 0 and curdate() <= c.return_date then 'в роботі'
			 when c.is_prolong = 1 and curdate() <= c.prolong_date then 'пролонгований'
             when (c.is_prolong = 0 and datediff(curdate(), c.return_date) <= 10) or (c.is_prolong = 1 and datediff(curdate(), c.prolong_date) <= 10) then 'просрочка 1-10 днів'
             when (c.is_prolong = 0 and datediff(curdate(), c.return_date) <= 30) or (c.is_prolong = 1 and datediff(curdate(), c.prolong_date) <= 30) then 'просрочка 11-30 днів'
             when (c.is_prolong = 0 and datediff(curdate(), c.return_date) <= 60) or (c.is_prolong = 1 and datediff(curdate(), c.prolong_date) <= 60) then 'просрочка 31-60 днів'
             when (c.is_prolong = 0 and datediff(curdate(), c.return_date) <= 90) or (c.is_prolong = 1 and datediff(curdate(), c.prolong_date) <= 90) then 'просрочка 61-90 днів'
             else 'просрочка 90+ днів'
			 end as 'Статус кредиту',

        man.manager "Менеджер",
		date(if(c.is_prolong = 0, c.return_date, c.prolong_date)) as 'Платіжна дата'
from creditone.credit_requests cr
join creditone.users u1 on u1.id = cr.user_id
join creditone.statuses s on s.id = cr.status
left join creditone.credits c on c.id = cr.credit_id
left join creditone.users u2 on u2.id = cr.checked_by
left join creditone.users u3 on u3.id = cr.approved_by
left join creditone.users u4 on u4.id = cr.rejected_by
left join creditone.rejection_reasons rr on rr.id = cr.rejection_reason_id
left join creditone.users u5 on u5.id = cr.money_sent_by
left join creditone.dictionary_values dv on dv.id = u1.status_id
left join (select cr1.id, @row_number1:=case when @requestId=cr1.user_id THEN @row_number1+1 ELSE 1 END AS row_number, 
			@requestId:=cr1.user_id AS UserRequest
			FROM creditone.credit_requests cr1 order by cr1.user_id) as RequestRN on RequestRN.id = cr.id
left join (select cr2.credit_id, @row_number2:=case when @creditId=cr2.user_id then @row_number2+1 else 1 end as row_number,
			@creditId:=cr2.user_id as UserCredit
            from creditone.credit_requests cr2 where cr2.credit_id is not null order by cr2.user_id) as CreditRN on CreditRN.credit_id = cr.credit_id
left join (select distinct cp1.user_id, cp1.minCrDate, cm.name
			from 
				(select cp1.user_id, min(cp1.created_at) as minCrDate from creditone.cpa cp1 group by cp1.user_id) as cp1
			join creditone.cpa cp2 on cp1.user_id = cp2.user_id and cp1.minCrDate = cp2.created_at
            join creditone.cpa_model cm on cm.id = cp2.cpa_id) as cpaType on cpaType.user_id = u1.id
/*left join   (select credit_id, count(credit_id) as 'prlCount', sum(current_payment) as 'prlSum'
			from creditone.credits_logs cl
			where cl.message like '%Оплата комиссии за пролонгацию%' or cl.message like '%Дней 5%' or cl.message like '%Дней 10%' or cl.message like '%Дней 20%' or cl.message like '%Дней 30%'
			group by credit_id) as prlTbl on prlTbl.credit_id = cr.credit_id*/
left join 	(select p.credit_id, count(p.credit_id) as 'prlCount', sum(p.amount) as 'prlSum'
			from creditone.payments p
            where p.is_prolong = '1'
            group by p.credit_id) as prlTbl on prlTbl.credit_id = cr.credit_id
left join 	(select cr.id, cr.user_id, max(intRes.sumPay) as repSum
			from creditone.credits cr
			left join	(select crInt.id as id, @paidSum:=case when @counterPay=crInt.user_id then @paidSum+clInt.current_payment else 0 end as sumPay,
						@counterPay:=crInt.user_id as UserId
						from creditone.credits crInt
						left join creditone.credits_logs clInt on crInt.id = clInt.credit_id) as intRes on cr.user_id = intRes.UserId
			where intRes.id < cr.id
			group by cr.id
			order by user_id) as repSum on repSum.id = cr.credit_id
left join   (select cr.id, cr.user_id, max(amRes.paidAm) as amSum
			from creditone.credits cr
			left join	(select crInt.id as id, @paidAm:=case when @counterAm=crInt.user_id then @paidAm+crInt.amount else crInt.amount end as paidAm,
						@counterAm:=crInt.user_id as UserId
						from creditone.credits crInt order by userId) as amRes on cr.user_id = amRes.UserId 
			where amRes.id < cr.id
			group by cr.id
			order by UserId, cr.id) as repAm on repAm.id = cr.credit_id
left join (select u.id, case when c.comment like '%кспери%' or c.comment like '%кспере%' then 'експериментальний' else 'стандартна видача' end as expCom
		   from creditone.users u
		   join creditone.comments c on c.user_id = u.id
           where (c.comment like '%кспери%' or c.comment like '%кспере%')
           group by u.id, case when c.comment like '%кспери%'  or c.comment like '%кспере%' then 'експериментальний' else 'стандартна видача' end) as expCom on u1.id = expCom.id 
join 	  (select u.id, maxReturn.maxDate, minReturn.minDate, s.status_name, 
		   case when maxReturn.maxDate is null then 'кредит боржнику не видавався'
				when maxReturn.maxDate = 0 then 'отримав один кредит і його не повернув'
				when maxReturn.maxDate > 0 and minReturn.minDate = 0 then 'повторний клієнт з кредитом на руках'
				when maxReturn.maxDate > 0 and minReturn.minDate > 0 then 'повторний клієнт без кредиту на руках'
				else '?' end as curCredit
			from creditone.users u
			join creditone.statuses s on s.id = u.status_id
			left join (select u.id as id, max(ifnull(cr.fact_return_date, 0)) as maxDate
					  from creditone.users u
					  join creditone.credits cr on cr.user_id = u.id
					  group by u.id) as maxReturn on maxReturn.id = u.id
			left join (select u.id as id, min(ifnull(cr.fact_return_date, 0)) as minDate
					  from creditone.users u
					  join creditone.credits cr on cr.user_id = u.id
					  group by u.id) as minReturn on minReturn.id = u.id
			) as curStat on curStat.id = u1.id

left join (select cr.id as CR_ID, concat(clients.lastname, ' ', clients.firstname, ' ', clients.middlename) as 'ПІБ клієнта', 
			   cr.start_date as 'Дата видачі кредиту', concat(man.lastname, ' ', man.firstname, ' ', man.middlename) as manager
		from creditone.credits cr

		left join (select *
			  from creditone.comments com
			  join (select u.id as user_id2, max(com.id) as maxId
			  from creditone.users u
			  join creditone.comments com on com.user_id = u.id
			  where com.manager_id in (30, 31, 32, 14123) or (com.manager_id=34497 and date(com.created_at)>='20210302')
			  group by u.id) as lastCom on lastCom.maxId = com.id and com.user_id = lastCom.user_id2) as lastCom on lastCom.user_id = cr.user_id
		left join creditone.users man on man.id = lastCom.manager_id
		join creditone.users clients on clients.id = cr.user_id
		where cr.id > 20 and cr.user_id > 44) as man on man.CR_ID=cr.credit_id
where cr.id > 20 and cr.user_id > 44 and cr.credit_id is not null
and fact_return_date is null and date(if(c.is_prolong = 0, c.return_date, c.prolong_date)) between CURDATE()+1 and CURDATE()+3"""


def zapyt(query, db):
    with SSHTunnelForwarder(
            # підключення для запиту з основної
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_password='HNtSWOi60zz!',
            remote_bind_address=(sql_hostname, sql_port)) as tunnel:
        conn = pymysql.connect(host='127.0.0.1', user=sql_username, passwd=sql_password, db=db,
                               port=tunnel.local_bind_port)

        data = pd.read_sql_query(query, conn)

        conn.close()

    return (data)


data = zapyt(query, 'creditone')

dataset = data

pd_writer = pd.ExcelWriter(f"C:\\Python\\звіти окк\\Мають закритись-{str(intDateFin)}.xlsx",'xlsxwriter')


dataset.to_excel(pd_writer, index=False, sheet_name='Table 1')
workbook = pd_writer.book
worksheet = pd_writer.sheets['Table 1']
worksheet.freeze_panes(1,0)
pd_writer.save()