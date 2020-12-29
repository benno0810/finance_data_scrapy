
import os
import re
import json
import time
import math
import urllib
import pandas
import random
import tushare
import requests
import datetime
import csv



	def login(self):													 # 通过手动输入短信验证码的方式登录（后来觉得自动识别短信验证码弄巧成拙）
		codeData = {													 # 获取验证码时提交的表单
			"areacode": self.areaCode,
			"telephone": self.telephone,
		}
		formData = {													 # 登录时提交的表单
			"areacode": self.areaCode,
			"telephone": self.telephone,
			"remember_me": "true",
		}
		r = self.session.post(self.codeURL, codeData)					 # 发送验证码
		while r.text[2] == "e":
			print("获取短信验证码失败！\t{}".format(r.text))
			input("继续获取验证码？")
			r = self.session.post(self.codeURL, codeData)				 # 发送验证码
		# 注意不带cookie现在不能登录，带了cookie又不能访问调仓记录（所以登录时带上cookie，登完就把cookie扔了）
		self.session.headers["Cookie"] = self.cookies
		formData["code"] = input("请输入手机号为{}的验证码：".format(self.telephone))
		r = self.session.post(self.loginURL, formData)					 # 验证码登录
		while r.text[2] == "e":
			r = self.session.post(self.loginURL, formData)				 # 验证码登录
			print("短信验证码登录失败！\t{}".format(r.text))
		print(r.text)
		print("登录成功！")
		self.session.headers = {"User-Agent": self.userAgent}			 # 删除cookie