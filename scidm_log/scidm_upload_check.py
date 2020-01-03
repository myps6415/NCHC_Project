from ckanapi import RemoteCKAN
import time
import smtplib
from email.mime.text import MIMEText

scidm = RemoteCKAN('https://scidm.nchc.org.tw', apikey='API_KEY')
today = time.strftime("%Y%m%d", time.localtime())

gmail_user = 'GMAIL'
gmail_password = 'PASSWORD' # your gmail password

msg = MIMEText('親愛的收信者您好：\n\n收到此封信代表例行的 SCIDM 自動上傳失敗，請趕緊處理！\n\nJohn_robot 敬上')
msg['Subject'] = 'SCIDM 未成功上傳自動通知信'
msg['From'] = gmail_user
msg['To'] = 'Email'
msg['CC'] = 'Email'

upload_data_date_list = []
for data in scidm.call_action('package_show', {'id': 'nspo-usage-statistic'})['resources']:
    upload_data_date_list.append(data['description'].split('_')[0])

if today not in upload_data_date_list:
    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.ehlo()
    server.login(gmail_user, gmail_password)
    server.send_message(msg)
    server.quit()
    print('Email sent!')
else:
    print('有檢查到')
