# -*- coding: utf-8 -*-
from setting import TEST_ANONYMOUS, TEST_VALID_STATUS,TIME_OUT
from database_handler import ProxyPool_DB
import requests
import json
import time
import queue
class IP():
	"""用于处理代理池
	"""
	def __init__(self,
	 headers="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0",
	 proxy_source="antproxy",
	 DB=ProxyPool_DB()):  #并发场景直接接入初始化好的DB类
		"""初始化

		Args:
			headers (str): 模拟浏览器类型. Defaults to "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0".
		"""
		self.proxiesURL = "http://127.0.0.1:59977/getip?price=1&word=&count=8&type=json&detail=true"
		self.ipRegulation = r"(([1-9]?\d|1\d{2}|2[0-4]\d|25[0-5]).){3}([1-9]?\d|1\d{2}|2[0-4]\d|25[0-5])"
		self.proxy_source=proxy_source
		self.headers = {"User-Agent": headers}
		self.session = requests.Session()
		self.session.headers = headers
		self.DB=DB
		print(self.proxy_source)

	def get_wn_IP(self, interval=1):
		"""获取可用代理IP地址及生效时间

		Args:
			interval (int, optional): 获取代理IP成功后后休眠时间. Defaults to 1.

		Returns:
			string : 返回用于映射公网IP的内网IP及内网端口 "ip":"127.0.0.1:12384"
		"""
		address = []
		while True:
			try:
				html = self.session.get(self.proxiesURL).text
			except:
				print("在刷新IP池时无法访问IP池API，尝试重新建立会话并连接")
				self.session = requests.Session()
				continue
			ip_dict = json.loads(html)
			#print(ip_dict)
			
			# print("dict1",dict1)
			if self.proxy_source=="xdaili":
				print('proxytype xdaili')
				if isinstance(ip_dict, dict):
					if str('ERRORCODE') in ip_dict.keys(): 
						if not (ip_dict.get('ERRORCODE')=='0'):
							print(ip_dict.get('ERRORCODE'))
							time.sleep(5)
							continue
				print("切换IP池至：", html)
				for ip in ip_dict['RESULT']:
					ip_str = ip['ip']+":"+str(ip['port'])
					address.append(ip_str)
				break
			if self.proxy_source=='antproxy':
				print('proxytype antproxy')
				if isinstance(ip_dict, dict):
					if str('error') in ip_dict.keys():
						print(ip_dict.get('error'))
						time.sleep(5)
						continue
				print("切换IP池至：", html)
				for ip in ip_dict:
					ip_str = ip['ip']+":"+str(ip['port'])+"@"+str(ip['expires_time'])
					address.append(ip_str)
				break
		print(address)
		# time.sleep(interval)
		return address

	def pool_append(self,interval=1,max_expires_time=180):# 并发DB IO
		pool_size=2*max_expires_time/interval
		while True:
			proxy_pool_headers={
				'ip_address':"",
				"expires_time":""
			}
			'''
			with open('proxy_pool.json','r',encoding='utf-8') as f:
				proxy_pool=json.load(f)
				if not proxy_pool:
					proxy_pool={}
			'''


			if self.DB.estimated_document_count()>pool_size:
				time.sleep(interval)
				break

			address=self.get_wn_IP()
			for line in address:
				proxy_url=line.split('@')[0]
				expires_time=line.split('@')[1]
				if isinstance(proxy_pool_headers,dict):
					line=proxy_pool_headers.copy()
					line['ip_address']=proxy_url
					line['expires_time']=expires_time
					self.DB.insert_one(line)
					line={}
			'''
			with open('proxy_pool.json','w',encoding='utf-8') as f:
				f.seek(0)
				f.truncate()
				json.dump(proxy_pool,f)
			'''
			#如果代理池数量大于ip有效期除以访问周期的两倍，则退出
			time.sleep(interval)


class Tester():
	def __init__(
		self,headers="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0",
		target_url="https://httpbin.org/ip",
		DB=ProxyPool_DB()
		):
		self.headers=headers
		self.target_url=target_url
		self.DB=DB
	#expire_time  没作用
	def run_tester(self,interval=20):     #并发DB IO
		#每二十秒测试一次代理池是否有效: 测试匿名性，测试可连接行
		proxy_pool_headers={
			'ip_address':"",
			"expires_time":""
		}
		while True:
			'''
			with open('proxy_pool.json','r',encoding='utf-8') as f:
				proxy_pool=json.load(f)
			proxy_list=list(proxy_pool.keys())
			'''
			proxy_list=list(self.DB.find_many({}))  #此步如何做并发
			pool_size=len(proxy_list)
			for i in range(pool_size):
				address=proxy_list[i]['ip_address']
				expires_time=proxy_list[i]['expires_time']
				if time.time()<expires_time:
					
					self.tester(address,expires_time,self.target_url)
				else:
					print('proxy {} is expired'.format(proxy_list[i]))
					del proxy_list[i]
					query=proxy_pool_headers.copy()
					query['ip_address']=address
					query['expires_time']=expires_time
					self.DB.delete_many(query)
			'''
			with open('proxy_pool.json','w',encoding='utf-8') as f:
				f.seek(0)
				f.truncate()
				json.dump(proxy_pool,f)
			'''
			time.sleep(interval)
	
	def tester(self,proxy:str,expires_time:float,target_url:str):
		#测试proxy是否可用，如果不可用则过期时间提前,传回过期时间
		print(proxy)
		real_proxy={
			'https':proxy
		}
		if TEST_ANONYMOUS==True:
			url="https://httpbin.org/ip"
			session = requests.session()
			html = session.get(url,timeout=TIME_OUT)
			html_json=html.json()
			origin_ip=html_json['origin']
			html = session.get(url,proxies=real_proxy,timeout=TIME_OUT)
			html_json=html.json()
			anonymous_ip=html_json['origin']
			assert origin_ip != anonymous_ip
			#assert proxy.host == anonymous_ip

class Tester_Xueqiu(Tester):
	def __init__(self):
		super().__init__(self)

	def tester(self,proxy:str,expires_time:float,target_url:str):
		#print(proxy,expires_time,target_url)
		super().tester(proxy, expires_time,target_url)
		real_proxy={
			'https':proxy
		}
		xueqiu_url='https://xueqiu.com'
		invalid_flag=0
		#测试代理是否成功链接至目标网站
		session=requests.session()
		session.headers=self.headers
		try:
			html = session.get(xueqiu_url,proxies=real_proxy,timeout=TIME_OUT)
		except requests.exceptions.Timeout as e:
			print(e)
		except requests.exceptions.HTTPError as e:
			print(e)
		print(html.status_code)
		if html.status_code in TEST_VALID_STATUS:
			print('{} can connect to xueqiu'.format(proxy))
			return
		expires_time=expires_time-20

		
if __name__=='__main__':
	#ip=IP()
	#ip.pool_append()
	tester=Tester_Xueqiu()
	tester.run_tester()