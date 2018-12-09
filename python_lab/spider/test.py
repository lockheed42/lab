#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'''
   spider
'''

__author__ = 'lockheed'

import requests
import http.cookiejar


# 模拟登陆
base_login = "https://www.elegomall.com/site/login.html"
# 请求报头
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
    "X-CSRF-Token": "RnVkc1pkMjEXOCkhNRFwUnc5PAsALAt8IzpQJ3cwVGsgEiVEEAFIdQ==",
    "Cookie": "age=qualified;_ga=GA1.2.834221224.1517461768;__lc.visitor_id.8854324=S1517461769.107aed3dda;__cfduid=d7e816258e17995ad55310e1b663972dd1529888335;_csrf=c8c098b0a73a3dfa09177fac167febb8b7586274721e8388973315d53f171f38s%3A32%3A%22QMMRouBc1LXxZH9MeO4T-TfZfgA7JezD%22%3B;_gid=GA1.2.1190735462.1543281423;autoinvite_callback=true;autoinvite_callback=true;currencyCode=USD;currency=dd37fdf39f2b0330dfed5e02c5e2c1c80f060e8b1c20b31e2c011150ae0726d6s%3A3%3A%22USD%22%3B;age=qualified;lc_invitation_opened=opened;lc_sso8854324=1543369475826;productHistory=5f137add19678183251c5806b326e1777ac666ff3fe744fa053a26bb943b4010s%3A29%3A%2218803%2C18731%2C17932%2C18550%2C16825%22%3B;lc_window_state=minimized;PHPSESSID=k244seasj5t9s99u8rifveagv3",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": "https://www.elegomall.com/"
}
home_url = "https://www.elegomall.com/product/smok-r-kiss-200w-kit-with-tfv-mini-v2-tank.html"

# 创建data表单数据
data = {
    "FrontendLoginFrom[email]": "christina@calistondistro.com",
    "FrontendLoginFrom[password]": "1sexymanhalko",
}
# 下载的html资源文件保存地址
html_res_path = '/Library/WebServer/Documents/code/spider_res/html'


session = requests.session()
response = session.post(base_login, data=data, headers=headers)
# 把html文件写入。存入html路径下
with open(html_res_path + "/response.html", 'w') as f:
    f.write(response.content.decode("utf-8"))

# 第六步 保存cookie
# session.cookies.save()

# 获取首页信息
resp = session.get(home_url)
# 把html文件写入。存入html路径下
with open(html_res_path + "/test.html", 'w') as f:
    f.write(resp.content.decode("utf-8"))
