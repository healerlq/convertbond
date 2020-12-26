#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/11/29 20:22
# @Author : Qian
# @Site : 
# @File : sequence.py
# @Software: PyCharm

class sequence:
    def __init__(self):
        self._sequence = []

    #占位，之后用于定义其他功能函数
    def func(self):
        pass


    def get_base(self, ):
        return self.last_base


    def set_base(self,comp):
        self.last_base = comp


    def add(self,filter):
        self._sequence.append(filter)

    def set_pipeline(self,method_list):
        self._sequence = method_list

    def fit(self,ob_date,ini_base):
        self.last_base = ini_base
        for filter_i in self._sequence:
            filter_i.run(base = self.last_base,reference_date=ob_date)
            comp,factor = filter_i.base,filter_i.factor_value
            self.set_base(comp)
        comp_output = self.get_base()
        factor_output = factor
        return comp_output,factor_output




