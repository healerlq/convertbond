#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/11/29 23:42
# @Author : Qian
# @Site : 
# @File : demo.py
# @Software: PyCharm

import matplotlib.pyplot as plt
from backtesting import *
import openpyxl

m=calculate_basic()
#================================================================
#demo1:
test_factor = "close"  #修改参数1！！
#test_factor2 = 'strbvalue'
factor_name = '转债20日动量'  #修改参数2，该参数是用于命名生成的结果excel中的sheet
factor_inv = False  #修改参数3，代表该因子于收益是正相关还是负相关，正相关：True,否则为Flase,

#step1:根据筛选条件搭建模型
model = sequence()
model.add(filter_zg()) #进入转股期的转债
model.add(filter_turn([0,2.0])) #换手率在[0,2]之间的转债
model.add(filt_not_cv(0.3)) #未转股比例大于0.3的转债

#根据wind可提取的单一指标构建新因子：20日动量
model.add(single_factor(hurdle=[(0,0.3),(0.3,0.7),(0.7,1.0)],factor_name=test_factor,method=m.inv,window=20,is_stock_factor=False))

#wind直接提取test_factor指标
#model.add(wind_single_factor(hurdle=[(0,0.3),(0.3,0.7),(0.7,1.0)],factor_name=test_factor,is_stock_factor=True))

#根据wind可以提取的两个指标构建新的因子：根据test_factor1和test_factor2，以div(即相除的方式构建新因子，其他方法详见filter模块中的calculate_basic类，也可以根据自己需要写
#model.add(two_factor(hurdle=[(0,0.3),(0.3,0.7),(0.7,1.0)],test_factor1=test_factor1,test_factor2=test_factor2,method=m.div))


#step2:设置回测参数，并对每一层进行回测，也可以只对其中一层进行回测
bk1 = backtest(start_date='20190101',end_date='20201129',freq='Q',lag=0) #季度换仓
bk1.backtest(0,model,'equal') #对第0层，等权重加权

bk2 = backtest(start_date='20190101',end_date='20201129',freq='Q',lag=0) #季度换仓
bk2.backtest(1,model,'equal') #第1层，等权重加权

bk3 = backtest(start_date='20190101',end_date='20201129',freq='Q',lag=0) #季度换仓
bk3.backtest(2,model,'equal') #第2层，等权重加权

bk_bch = backtest(start_date='20190101',end_date='20201129',freq='Q',lag=0) #这一步指数为了用于之后计算基准的业绩表现，因为把业绩表现函数也写在了backtest类中



#============================================================
#=====================业绩展示================================
result=pd.DataFrame()
for i in range(3):
    bk = [bk1, bk2, bk3][i]
    bk.cal_sharp()
    bk.cal_withdraw()
    result.loc["bk{}".format(str(i+1)),"total_return"]=bk.total_ret
    result.loc["bk{}".format(str(i+1)),"ann_ret"]=bk.ann_ret
    result.loc["bk{}".format(str(i + 1)), "ann_vol"] = bk.ann_vol
    result.loc["bk{}".format(str(i + 1)), "sharp"] = bk.sharp
    result.loc["bk{}".format(str(i + 1)), "drawback"]=bk.biggest_withdraw
    result.loc["bk{}".format(str(i + 1)), "start"] = bk.withdraw_start
    result.loc["bk{}".format(str(i + 1)), "end"] = bk.withdraw_end



time_line = [bk.time_seri[0]]+[i.strftime("%Y%m%d") for i in bk.time_seri[1:]]
zz = w.wsd("000832.CSI", "close", time_line[0], time_line[-1], "Fill=Previous").Data[0]
zz = list(np.array(zz)/zz[0])
net_value_df = pd.DataFrame({"time":time_line,\
                             'bk1':bk1.net_value_ser,\
                             "bk2":bk2.net_value_ser,\
                             "bk3":bk3.net_value_ser,\
                             "zz":zz})
bk_bch.time_seri = bk.time_seri
bk_bch.cal_sharp(benchmark=zz)
bk_bch.cal_withdraw(benchmark=zz)
result.loc["benchmark","total_return"]=bk_bch.total_ret
result.loc["benchmark","ann_ret"]=bk_bch.ann_ret
result.loc["benchmark", "ann_vol"] = bk_bch.ann_vol
result.loc["benchmark", "sharp"] = bk_bch.sharp
result.loc["benchmark", "drawback"]=bk_bch.biggest_withdraw
result.loc["benchmark", "start"] = bk_bch.withdraw_start
result.loc["benchmark", "end"] = bk_bch.withdraw_end

'''
#=============信息存储============================================
wb = openpyxl.load_workbook('./result.xlsx')
wb_add = wb.create_sheet(title=factor_name,index=0)
for col in net_value_df.columns:
    wb_add.append([col]+net_value_df[col].tolist())
wb_add.append([])
wb_add.append(['index']+list(result.columns))
for ind in result.index:
    wb_add.append([ind]+result.loc[ind].tolist())
wb_add.append([])
wb_add.append([factor_name])
if factor_inv==False:
    wb_add.append(['备注:第一层到最后最后一层代表根据该因子由大到小排序'])
else:
    wb_add.append(['备注:第一层到最后最后一层代表根据该因子由小到大排序'])

wb.save('./result.xlsx')
wb.close()
'''

net_value_df.plot()
ind = [i for i in range(0,len(time_line),125)]
plt.xticks(range(0,len(time_line),125),(time_line[i] for i in ind),rotation=60)
#plt.savefig('./结果图例展示/'+test_factor+'.jpg')


