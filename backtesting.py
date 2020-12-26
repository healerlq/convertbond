#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/11/29 19:08
# @Author : Qian
# @Site : 
# @File : backtesting.py
# @Software: PyCharm

from filter import *
import pandas as pd
import numpy as np
from sequence import *
import openpyxl

class backtest:
    def __init__(self,**kwargs):
        #record(包括每期净值序列、每期成分序列和权重序列)
        self.net_value_ser = [1.0]
        self.comp_list = []
        self.weight_list = []
        self.comp_num_seri=[]#每个标的投资的实际数量
        self.comp_count = []
        self.base_count =[]
        self.time_seri = [] #回测净值观测时间

        # 初始化回测重要参数（包括观察日期、调仓日期等）
        self.start_date= kwargs['start_date'] if 'start_date' in kwargs else None#开始日期
        self.end_date = kwargs['end_date'] if 'end_date' in kwargs else None  #结束日期
        self.freq = kwargs['freq'] if 'freq' in kwargs else None
        self.lag = kwargs['lag'] if 'lag' in kwargs else None #观察期相对回测期滞后多少天!!

        #自定义回测时间线
        self.ob_time_line = kwargs['ob_time_line'] if 'ob_time_line' in kwargs else None
        self.test_time_line = kwargs['test_time_line'] if 'test_time_line' in kwargs else None

        #根据开始、结束时间和频率生成回测时间线
        if (self.freq != None) and (self.start_date != None) and (self.end_date != None) and (self.lag !=None):
            self.test_time_line = w.tdays("{}".format(self.start_date), "{}".format(self.end_date), "Period={}".format(self.freq)).Times
            self.ob_time_line = [w.tdaysoffset(self.lag, "{}".format(i), "").Times[0] for i in self.test_time_line]
        self.test_time_line = [i.strftime("%Y%m%d") for i in self.test_time_line]
        self.ob_time_line = [i.strftime("%Y%m%d") for i in self.ob_time_line]
        self.log = []#日志，暂时未启用
        self.initial()
        self.reset()

    def set_expense(self,expense_ratio):
        self.expense_ratio = expense_ratio

    def initial(self):
        if ((self.test_time_line != None) and (self.ob_time_line != None)) and \
                ((self.test_time_line !=[]) and(self.ob_time_line !=[])):
            print("Initialization succeeded")
        else:
            print("Initialization Failed\n",self.ob_time_line,'\n',self.test_time_line)


    def reset(self):
        #单期记录重置
        self.comp = []
        self.weight = []
        self.factor = []
        self.comp_num=[]#每一次每个标的投资的实际数量
        self.price = []



    #=========================================
    #该回测目前还没有考虑交易费率
    def backtest(self,k,model,method='equal'):
        self.k = k
        #对于开始时刻t0
        ob_date = self.ob_time_line[0]
        test_date = self.test_time_line[0]
        self.time_seri.append(test_date)
        self.reset()  # 重置当期成分券和权重
        ini_base = w.wset("sectorconstituent", "date={};sectorid=a101020600000000".format(ob_date)).Data[1]

        if len(ini_base)==0:
            raise ValueError("初始标的池为空，请检查或更换时间点")
        self.comp, self.factor = model.fit(ob_date, ini_base)

        if k>=len(self.comp):
            raise ValueError('Range Out of Index')
        self.comp=self.comp[self.k]
        self.factor=self.factor[self.k]
        self.comp_list.append(self.comp)
        self.comp_count.append(len(self.comp))

        if len(self.comp)==0:
            raise ValueError('该时间点无合适标的，请更改回测开始时间')
        # 使用等权法
        self.weighting(method)
        self.weight_list.append(self.weight)

        self.price = w.wss(",".join(self.comp), "close", "tradeDate={};priceAdj=U;cycle=D".format(test_date)).Data[0]

        self.comp_num = self.net_value_ser[0] * np.array(self.weight) / np.array(self.price)
        self.comp_num_seri.append(self.comp_num)

        #对于t1以及之后的时刻
        for t in range(1,len(self.ob_time_line)):

            ob_date = self.ob_time_line[t]
            test_date = self.test_time_line[t]
            if len(self.comp_list[-1])==0:
                time_s =w.tdays("{}".format(self.test_time_line[t-1]), "{}".format(test_date), "").Times
                self.time_seri.extend(time_s[1:])
                self.net_value_ser.extend([self.net_value_ser[-1]]*len(time_s[1:]))
            else:
                # 上一观测日选择出来的标的券当前交易日的价格
                price_array = w.wsd(",".join(self.comp_list[-1]), "close","{}".format(self.test_time_line[t-1]), "{}".format(test_date),"Fill=Previous")
                self.time_seri.extend(price_array.Times[1:])
                price = np.array([i[1:] for i in price_array.Data])
                self.net_value_ser.extend((np.array(self.comp_num_seri[-1])*price.T).sum(axis=1).tolist())#得到t时刻的净值

            self.reset() #重置当期成分券和权重
            ini_base = w.wset("sectorconstituent", "date={};sectorid=a101020600000000".format(ob_date)).Data[1]

            #获取根据一定规则筛选后i时刻的标的券
            self.comp,self.factor = model.fit(ob_date,ini_base)
            print('========{}:共挑选出{}只标的========='.format(test_date,len(self.comp[self.k])))
            if len(self.comp[self.k])==0:
                self.comp_list.append([])
                self.comp_count.append(0)
                self.weight_list.append([])
                self.comp_num_seri.append([])
                continue

            self.comp = self.comp[self.k]
            self.factor = self.factor[self.k]
            self.comp_list.append(self.comp)
            self.comp_count.append(len(self.comp))


            #使用等权法
            self.weighting(method)
            self.weight_list.append(self.weight)

            #获取i时刻实际收盘价格
            self.price =w.wss(",".join(self.comp), "close","tradeDate={};priceAdj=U;cycle=D".format(test_date)).Data[0]

            self.comp_num = self.net_value_ser[-1] * np.array(self.weight) / np.array(self.price)
            self.comp_num_seri.append(self.comp_num)


    def weighting(self,method='equal'):
        #暂时考虑等权重
        if method == 'equal':
            self.weight = [1/len(self.comp)]*len(self.comp)


    def cal_sharp(self,**kwargs):
        self.net_value_ser = kwargs['benchmark'] if 'benchmark' in kwargs else self.net_value_ser
        self.total_ret = self.net_value_ser[-1]/self.net_value_ser[0] - 1
        self.days = w.tdayscount(self.time_seri[0],self.time_seri[-1], "").Data[0][0]
        self.ann_ret = (self.total_ret+1)**(252/self.days)-1
        self.ann_vol = pd.Series(self.net_value_ser).pct_change(1).std()*np.sqrt(252)
        self.sharp = self.ann_ret/self.ann_vol

    def cal_withdraw(self,**kwargs):
        net_ser = kwargs['benchmark'] if 'benchmark' in kwargs else self.net_value_ser.copy()
        history_big = [net_ser[0]]+[np.max(net_ser[:i]) for i in range(1,len(net_ser))]
        big_time = [0]+[net_ser[:i].index(history_big[i]) for i in range(1,len(history_big))]
        withdraw = [net_ser[i]/history_big[i]-1 for i in range(len(net_ser))]
        self.biggest_withdraw = np.min(withdraw)
        self.withdraw_end = self.time_seri[withdraw.index(self.biggest_withdraw)]
        self.withdraw_start = self.time_seri[big_time[withdraw.index(self.biggest_withdraw)]]





