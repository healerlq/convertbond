#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/11/27 11:21
# @Author : Qian
# @Site : 
# @File : filter.py
# @Software: PyCharm
import pandas as pd
from WindPy import *
import numpy as np
w.start()


class parent:
    def __init__(self):
        pass

    #设置一些属性，便于该类中的大多数方法调用
    def set(self,base,reference_date):
        self.base = base
        self.reference_date = reference_date


    #一个分层函数
    def layered(self,df,hurdle,inv = False):
        #要求传入的df格式要求比较严格
        # 默认不进行反转，即降序排列
        df = df.sort_values(by=['factor'], ascending=inv)
        nrows = df.shape[0]
        ind = [[int(nrows * i[0]), int(nrows * i[1])] for i in hurdle]
        # 获取各个区间中的转债code以及对应因子值
        code_list = [list(df['code'])[i[0]:i[1]] for i in ind]
        factor_list = [list(df['factor'])[i[0]:i[1]] for i in ind]
        return code_list,factor_list

    #行业中性,根据申万行业分类
    def ind_nuetral(self,df,**kwargs):
        industry = w.wss(",".join(list(df['code'])),
              "industry_sw", "industryType=1;tradeDate={}".format(self.reference_date)).Data[0]
        df['ind'] = industry
        inv = kwargs['inv']
        hurdle = kwargs['hurdle']
        code_list = [[] for i in range(len(hurdle))]
        factor_list = [[] for i in range(len(hurdle))]
        for ind_name,group in df.groupby('ind'):
            c,f = self.layered(group,hurdle,inv)
            code_list = [code_list[i]+c[i] for i in range(len(hurdle))]
            factor_list = [factor_list[i] + f[i] for i in range(len(hurdle))]
        return code_list,factor_list

    #一些其他的预处理函数




    #获取标的底层股票代码和相关因子的函数
    def get_stock_factor(self,factor_name,**kwargs):
        window = kwargs['window'] if 'window' in kwargs else 0
        self.start_date = w.tdaysoffset(-window,self.reference_date, "").Times[0].strftime(r'%Y%m%d')
        stock = w.wss(",".join(self.base), "underlyingcode", "tradeDate={}".format(self.reference_date)).Data[0]
        # 可能会存在找不到对应正股代码的转债：跳过这些转债，并输出对应转债代码
        temp_df = pd.DataFrame({'code': self.base, "stock_code": stock})
        null_list = temp_df[temp_df.isnull().T.any()]['code'].tolist()  # 无对应正股代码的转债
        if len(null_list) > 0:
            print(null_list, "找不到对应正股代码，请检查")
        temp_df = temp_df.dropna()
        self.base = temp_df['code'].tolist()
        stock = temp_df['stock_code'].tolist()
        # 注意：可能多只转债对应同一只正股代码，因此以下程序会出错,这里通过数据框merge来获取factor，后续看如何优化
        if len(stock) == len(set(stock)):
            if window ==0:
                factor = w.wss(",".join(stock), factor_name,
                               "tradedate={}".format(self.reference_date)).Data[0]
            else:
                factor = w.wsd(",".join(stock), factor_name,"{}".format(self.start_date),"{}".format(self.reference_date),"Fill=Previous").Data #shape:[nstock,ndays]
        else:
            stock_new = list(set(stock))
            if window ==0:
                factor = w.wss(",".join(stock_new), factor_name,
                               "tradedate={}".format(self.reference_date)).Data[0]
            else:
                factor = w.wsd(",".join(stock_new), factor_name, "{}".format(self.start_date),"{}".format(self.reference_date),"Fill=Previous").Data#shape:[nstock,ndays]
            df_new = pd.DataFrame({'stock_code': stock_new, "factor": factor})
            factor = pd.merge(temp_df, df_new, how='left', on='stock_code')['factor'].tolist()
        # 返回的factor的shape:[nstock,ndays]
        return factor

#预处理部分，根据一些条件简单剔除部分转债标的
class filter_delist:
    #返回未摘牌的转股代码
    def __init__(self):
        self.factor_value = []

    def run(self, **kwargs):
        self.base = kwargs['base']  # base是是使用筛选标的的基准标的池
        self.reference_date = pd.to_datetime(kwargs['reference_date'])
        if len(self.base) == 0:
            print(self.reference_date, "池中无满足条件的标的，请更改时间点或调整筛选规则")
            self.base = []
            self.factor_value = []
        else:
            convert_date = w.wss(",".join(self.base), "delist_date").Data[0]
            df = pd.DataFrame({'code': self.base, 'delist_date': convert_date})
            self.base = df[df["delist_date"] >= self.reference_date]['code'].tolist()
            self.factor_value = df[df['delist_date'] >= self.reference_date]['delist_date'].tolist()
class filter_zg:
    #返回进入转股期的可转债代码
    def __init__(self):
        self.factor_value = []

    def run(self,**kwargs):
        self.base = kwargs['base']  #base是是使用筛选标的的基准标的池
        self.reference_date = pd.to_datetime(kwargs['reference_date'])
        if len(self.base)==0:
            print(self.reference_date ,"池中无满足条件的标的，请更改时间点或调整筛选规则")
            self.base = []
            self.factor_value = []
        else:
            convert_date = w.wss(",".join(self.base), "clause_conversion_2_swapsharestartdate").Data[0]
            df = pd.DataFrame({'code':self.base,'cv_date':convert_date})
            self.base = df[df['cv_date'] <= self.reference_date]['code'].tolist()
            self.factor_value = df[df['cv_date'] <= self.reference_date]['cv_date'].tolist()
class filter_turn:
    #筛选换手率合理的可转债代码,通常选择在【0，1】之间的标的
    def __init__(self,interval=[0,1.0]):

        self.interval = interval
        self.factor_value = []

    def run(self,**kwargs):
        self.base = kwargs['base']  # base是是使用筛选标的的基准标的池
        self.reference_date = kwargs['reference_date']
        if len(self.base)==0:
            print(self.reference_date ,"池中无满足条件的标的，请更改时间点或调整筛选规则")
            self.base = []
            self.factor_value = []
        else:

            #但注意这里只根据观察日当天的换手率来筛选的，如果加一个窗口期，根据平均换手率来筛选会不会好一些？
            turn = w.wss(",".join(self.base),"turn","tradeDate={};cycle=D".format(self.reference_date)).Data[0]
            df = pd.DataFrame({'code':self.base,'turn':turn})
            #剔除停牌或无交易的
            df =  df.dropna()
            df = df[df['turn']>0]
            sub = self.interval[0]
            sup = self.interval[1]
            self.base = df[(df['turn'] <= sup)&(df['turn'] >= sub)]['code'].tolist()
            self.factor_value = df[(df['turn'] <= sup) & (df['turn'] >= sub)]['turn'].tolist()
class other_filter:
    def __init__(self):
        pass
    def run(self):
        pass

class filt_not_cv:
    #剔除未转股比例少于一定数值的
    def __init__(self,num=0.3):
        self.num = num
        self.factor_value = []

    def run(self,**kwargs):
        self.base = kwargs['base']  # base是是使用筛选标的的基准标的池
        self.reference_date = kwargs['reference_date']
        if len(self.base)==0:
            print(self.reference_date,"池中无满足条件的标的，请更改时间点或调整筛选规则")
            self.base=[]
            self.factor_value=[]
        else:
            not_cv = w.wss(",".join(self.base), "clause_conversion2_bondproportion", "tradeDate={}".format(self.reference_date)).Data[0]
            df = pd.DataFrame({'code': self.base, 'not_cv': not_cv})
            df = df.dropna()
            self.base = df[(df['not_cv']>=self.num)]['code'].tolist()
            self.factor_value = df[(df['not_cv']>=self.num)]['not_cv'].tolist()



    #===================还可以根据因子选择转债的需要，再加入其他的filter函数================


#------------------------------------------------
#可直接调取的wind单因子测试框架
class wind_single_factor(parent):
    def __init__(self,**kwargs):
        parent.__init__(self)
        #self.window_size = kwargs['window_size'] if 'window_size' in kwargs else None#暂未启用
        self.hurdle = kwargs['hurdle'] #hurdle应该是[(sub1,sup1),(sub2,sup2),(sub3,sup3)]的格式
        self.factor_name = kwargs['factor_name']
        self.inv = kwargs['inv'] if 'inv' in kwargs else False
        self.ind_neu = kwargs['ind_neu'] if 'ind_neu' in kwargs else False
        self.is_stock_factor = kwargs['is_stock_factor'] if 'is_stock_factor' in kwargs else False
        self.reset()

    def reset(self):
        self.base = []
        self.factor_value = []

    def run(self,**kwargs):
        parent.set(self,kwargs['base'], kwargs['reference_date'])  # 不知道为什么这里调用方法要传self
        if len(self.base)==0:
            print(self.reference_date,"池中无满足条件的标的，请更改时间点或调整筛选规则")
            self.base = [[] for i in range(len(self.hurdle))]
            self.factor_value = [[] for i in range(len(self.hurdle))]
        else:
            #inv:默认为倒序，如果越小越好的话，则inv等于True
            #ind_neu:是否进行行业中性处理，默认为True进行处理
            #是否为正股因子
            if self.is_stock_factor==True:
                factor = parent.get_stock_factor(self,self.factor_name)
            else:
                factor = w.wss(",".join(self.base),self.factor_name, "tradeDate={}".format(self.reference_date)).Data[0]
            df = pd.DataFrame({'code': self.base, 'factor': factor})
            ##针对正股财务因子，剔除为负的值！!后续调整(目前用于pe、pb),将这种调整函数放在parent类中
            df = df[df['factor']>0]
            #剔除因子值为空的转债，后续可以不剔除或者用其他方法填充
            df = df.dropna()
            #nrows = df.shape[0]
            if self.ind_neu ==True:
                code_list,factor_list = self.ind_nuetral(df,hurdle = self.hurdle,inv = self.inv)
            else:
                code_list,factor_list = self.layered(df,self.hurdle,self.inv)
            self.base = code_list
            self.factor_value = factor_list

#----------------------------------基础计算类--------------------
class calculate_basic:
    # 输入的数据是[nstocks,ndays]的数组
    def ma(self,data):
        return data.mean(axis=1).tolist()

    def vol(self,data):
        return data.std(axis=1).tolist()

    def inv(self,data):
        return (data[:,-1]/data[:,0]).tolist()
    def sum(self):
        pass
    def sub(self):
        pass
    def mul(self):
        pass
    def div(self,factor1,factor2):
        return (factor1/factor2).tolist()

    def perc(self,data):
        #用来计算历史分位点，可能会比较浪费数据，之后改进
        d = data[:,-1]
        n = data.shape[1]
        num=data.shape[0]
        data.sort(axis=1)
        return [(list(data[i]).index(d[i])+1)/n for i in range(num)]





#---------------------需要手动计算的单因子框架(涉及两个指标的运算，其他更复杂的运算自行写class来构建)-------------------------
class single_factor(parent):
    def __init__(self,**kwargs):
        parent.__init__(self)
        self.hurdle = kwargs['hurdle']
        self.method = kwargs['method']#即采用何种计算方法
        self.factor_name = kwargs['factor_name']
        self.windows = kwargs['window'] if 'window' in kwargs else None

        self.inv = kwargs['inv'] if 'inv' in kwargs else False
        self.ind_neu = kwargs['ind_neu'] if 'ind_neu' in kwargs else False
        self.is_stock_factor = kwargs['is_stock_factor'] if 'is_stock_factor' in kwargs else False

    def run(self,**kwargs):
        parent.set(self,kwargs['base'],kwargs['reference_date'])
        if len(self.base) == 0:
            print(self.reference_date, "池中无满足条件的标的，请更改时间点或调整筛选规则")
            self.base = [[] for i in range(len(self.hurdle))]
            self.factor_value = [[] for i in range(len(self.hurdle))]
        else:
            #inv:默认为倒序，如果越小越好的话，则inv等于True
            #ind_neu:是否进行行业中性处理，默认为True进行处理
            #是否为正股因子
            if self.is_stock_factor==True:
                self.factor1 = parent.get_stock_factor(self, self.factor_name,window=self.windows)
            else:
                self.start_date = w.tdaysoffset(-self.windows,self.reference_date, "").Times[0].strftime(r'%Y%m%d')
                self.factor1 = w.wsd(",".join(self.base), self.factor_name,"{}".format(self.start_date),"{}".format(self.reference_date),"Fill=Previous").Data
            #选择计算方法，并进行运算
            factor = self.method(np.array(self.factor1))
            df = pd.DataFrame({'code': self.base, 'factor': factor})
            if self.ind_neu ==True:
                code_list,factor_list = self.ind_nuetral(df,hurdle = self.hurdle,inv = self.inv)
            else:
                code_list,factor_list = self.layered(df,self.hurdle,self.inv)
            self.base = code_list
            self.factor_value = factor_list

class two_factor(parent):
    def __init__(self,**kwargs):
        parent.__init__(self)
        self.hurdle = kwargs['hurdle']
        self.method = kwargs['method']  # 即采用何种计算方法
        self.test_factor1 = kwargs['test_factor1']
        self.test_factor2 = kwargs['test_factor2']
        self.windows = kwargs['window'] if 'window' in kwargs else 0

        self.inv = kwargs['inv'] if 'inv' in kwargs else False
        self.ind_neu = kwargs['ind_neu'] if 'ind_neu' in kwargs else False
        self.is_stock_factor = kwargs['is_stock_factor'] if 'is_stock_factor' in kwargs else False

    def run(self,**kwargs):
        parent.set(self,kwargs['base'],kwargs['reference_date'])
        if len(self.base) == 0:
            print(self.reference_date, "池中无满足条件的标的，请更改时间点或调整筛选规则")
            self.base = [[] for i in range(len(self.hurdle))]
            self.factor_value = [[] for i in range(len(self.hurdle))]
        else:
            #inv:默认为倒序，如果越小越好的话，则inv等于True
            #ind_neu:是否进行行业中性处理，默认为True进行处理
            #是否为正股因子
            if self.is_stock_factor==True:
                self.factor1 = parent.get_stock_factor(self, self.factor1,window=self.windows)
                self.factor2 = parent.get_stock_factor(self, self.factor2,window=self.windows)

            else:
                #self.start_date = w.tdaysoffset(-self.windows,self.reference_date, "").Times[0].strftime(r'%Y%m%d')
                self.factor1 = w.wss(",".join(self.base), self.test_factor1,"tradedate={}".format(self.reference_date)).Data[0]
                self.factor2 = w.wss(",".join(self.base), self.test_factor2,"tradedate={}".format(self.reference_date)).Data[0]

            #选择计算方法，并进行运算
            factor = self.method(np.array(self.factor1),np.array(self.factor2))
            df = pd.DataFrame({'code': self.base, 'factor': factor})
            if self.ind_neu ==True:
                code_list,factor_list = self.ind_nuetral(df,hurdle = self.hurdle,inv = self.inv)
            else:
                code_list,factor_list = self.layered(df,self.hurdle,self.inv)
            self.base = code_list
            self.factor_value = factor_list





#多因子测试框架,暂未启用
class multi_factor:
    def __init__(self):
        pass
'''
if __name__== "__main__":
    reference_date = "2020-11-27"
    data = w.wset("sectorconstituent", "date={};sectorid=a101020600000000".format(reference_date))  # 获取
    wind_code = data.Data[1]

    filt = filter(wind_code,reference_date)
    single_f = single_factor(filt.base,filt.reference_date,hurdle=[(0,0.3),(0.3,0.7),(0.7,1.0)])
    single_f.wind_factor()

'''











