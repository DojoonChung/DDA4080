#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
北京地铁交通网络韧性动态监测与可视化系统
Beijing Metro Network Resilience Dynamic Monitoring System

开发框架：Python + Dash + Plotly
适用场景：北京市规划院公共交通韧性评估项目（实习演示版）

环境要求：Python 3.8~3.11
安装依赖：pip install dash plotly pandas numpy openpyxl
运行方式：python beijing_subway_resilience.py
访问地址：http://127.0.0.1:8050
"""

import random
import time
import math
from datetime import datetime, timedelta
from collections import deque

import numpy as np
import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
import plotly.express as px

# ============================================================
# 0. 原始数据：北京地铁站点坐标 & 线路信息
#    （来源：station_geo.txt + stationinfo.xlsx 预处理结果）
# ============================================================

# 北京地铁各线路官方配色
LINE_COLORS = {
    '1号线':          '#C23A33',
    '2号线':          '#0E63A5',
    '4号线/大兴线':   '#00A14B',
    '5号线':          '#6A278B',
    '6号线':          '#D7A028',
    '7号线':          '#F67C1A',
    '8号线（北段）':  '#009DA8',
    '8号线（南段）':  '#009DA8',
    '9号线':          '#8DC63F',
    '10号线':         '#C4AAD0',
    '13号线':         '#F5A200',
    '14号线东段（含中段）': '#B99863',
    '14号线西段':     '#B99863',
    '15号线':         '#89277E',
    '16号线':         '#B5CD00',
    'S1线':           '#BCBD22',
    '亦庄线':         '#AD5B2E',
    '八通线':         '#C23A33',
    '大兴机场线':     '#C0A070',
    '房山线':         '#CC7E3A',
    '昌平线':         '#6DC6C1',
    '燕房线':         '#9B5935',
    '首都机场线':     '#A0522D',
}

# 站点坐标数据（经度, 纬度）- 来自 station_geo.txt
STATION_GEO_RAW = """
苹果园 116.177388,39.926727
古城 116.190337,39.907450
八角游乐园 116.212684,39.907442
八宝山 116.235948,39.907440
玉泉路 116.252900,39.907431
五棵松 116.274071,39.907214
万寿路 116.294765,39.907474
公主坟 116.309919,39.907469
军事博物馆 116.323385,39.907422
木樨地 116.337583,39.907379
南礼士路 116.352583,39.907234
复兴门 116.356866,39.907242
西单 116.374072,39.907383
天安门西 116.391278,39.907472
天安门东 116.401216,39.907780
王府井 116.411484,39.908902
东单 116.41848,39.908325
建国门 116.434914,39.908500
永安里 116.450497,39.908454
国贸 116.461841,39.909104
大望路 116.476132,39.908670
四惠 116.495456,39.908749
四惠东 116.515664,39.908495
西直门 116.353226,39.941670
积水潭 116.373126,39.948653
鼓楼大街 116.393776,39.948972
安定门 116.408240,39.949180
雍和宫 116.417069,39.949336
东直门 116.435102,39.942352
东四十条 116.434133,39.933801
朝阳门 116.434584,39.924499
北京站 116.427287,39.904983
崇文门 116.417093,39.901063
前门 116.397937,39.900192
和平门 116.384209,39.900098
宣武门 116.374314,39.899765
长椿街 116.363355,39.899433
阜成门 116.356009,39.923686
车公庄 116.354357,39.932397
安河桥北 116.269956,40.012195
北宫门 116.277647,40.002373
西苑 116.290908,39.998258
圆明园 116.310186,39.999662
北京大学东门 116.315842,39.992212
中关村 116.316467,39.983991
海淀黄庄 116.317564,39.975996
人民大学 116.321367,39.966956
魏公村 116.32322,39.957904
国家图书馆 116.32519,39.943114
动物园 116.339031,39.93825
新街口 116.367742,39.940667
平安里 116.372883,39.933949
西四 116.373332,39.924206
灵境胡同 116.373746,39.915912
菜市口 116.374425,39.889296
陶然亭 116.374383,39.87847
北京南站 116.379008,39.865012
马家堡 116.371361,39.853109
角门西 116.371154,39.84585
公益西桥 116.370796,39.836135
宋家庄 116.428368,39.845849
刘家窑 116.422119,39.857521
蒲黄榆 116.421617,39.865572
天坛东门 116.420833,39.882558
磁器口 116.41994,39.893172
灯市口 116.417783,39.917113
东四 116.417493,39.92437
张自忠路 116.417156,39.933592
北新桥 116.416884,39.940782
和平里北街 116.418504,39.958734
和平西桥 116.417975,39.968386
惠新西街南口 116.417537,39.977121
惠新西街北口 116.417028,39.987836
大屯路东 116.417377,40.003841
北苑路北 116.418089,40.030436
立水桥南 116.414496,40.041956
立水桥 116.41235,40.053032
天通苑南 116.412661,40.066458
天通苑 116.412759,40.075222
天通苑北 116.412888,40.083668
金安桥 116.163167,39.92362
杨庄 116.187004,39.92785
西黄村 116.206932,39.933605
廖公庄 116.227292,39.932422
田村 116.252914,39.929503
海淀五路居 116.276531,39.932584
慈寿寺 116.295467,39.933268
花园桥 116.310683,39.93234
白石桥南 116.32568,39.933022
车公庄西 116.344082,39.932466
北海北 116.386829,39.933247
南锣鼓巷 116.404192,39.933848
朝阳公园 116.478291,39.933492
东大桥 116.451657,39.923054
呼家楼 116.461618,39.923337
金台路 116.478115,39.923556
十里堡 116.502045,39.923076
青年路 116.517429,39.923168
褡裢坡 116.563961,39.924021
黄渠 116.578266,39.924201
常营 116.599722,39.925696
草房 116.615574,39.924477
物资学院路 116.639316,39.926801
通州北关 116.661131,39.917962
北运河西 116.688358,39.903024
北运河东 116.707056,39.903268
郝家府 116.717826,39.903195
东夏园 116.73385,39.903147
潞城 116.747434,39.902652
北京西站 116.321262,39.894763
湾子 116.327753,39.889954
达官营 116.336455,39.889884
广安门内 116.358239,39.889418
虎坊桥 116.384596,39.889486
珠市口 116.398372,39.891334
桥湾 116.408464,39.892725
广渠门内 116.433877,39.893673
广渠门外 116.448998,39.893648
双井 116.461834,39.893512
九龙山 116.478695,39.893222
大郊亭 116.487935,39.893183
百子湾 116.497768,39.89253
化工 116.503439,39.887369
南楼梓庄 116.501084,39.874578
欢乐谷景区 116.500067,39.866505
垡头 116.511821,39.860728
双合 116.526836,39.859691
焦化厂 116.537247,39.855488
朱辛庄 116.313698,40.104297
育知路 116.326992,40.087863
平西府 116.350425,40.090607
回龙观东大街 116.363025,40.081175
霍营 116.360286,40.071857
育新 116.347328,40.060039
西小口 116.351644,40.046873
永泰庄 116.35458,40.037728
林萃桥 116.372998,40.021906
森林公园南门 116.3926,40.010122
奥林匹克公园 116.391758,40.002207
奥体中心 116.393759,39.985837
北土城 116.394193,39.976953
安华桥 116.394655,39.968507
安德里北街 116.395145,39.957227
什刹海 116.396219,39.937583
中国美术馆 116.410803,39.923705
天桥 116.398712,39.88192
永定门外 116.399369,39.867435
木樨园 116.399839,39.859262
海户屯 116.400301,39.851805
大红门 116.399154,39.845383
大红门南 116.401045,39.836715
郭公庄 116.301889,39.814322
丰台科技园 116.297176,39.825233
科怡路 116.297432,39.83248
丰台南路 116.296748,39.840444
丰台东大街 116.293857,39.855111
七里庄 116.294292,39.866773
六里桥 116.302808,39.880239
六里桥东 116.315142,39.886886
白堆子 116.325762,39.923818
巴沟 116.293362,39.974408
苏州街 116.306332,39.975642
知春里 116.328709,39.976334
知春路 116.33996,39.976476
西土城 116.354098,39.976549
牡丹园 116.369844,39.976603
健德门 116.381353,39.976723
安贞门 116.405954,39.977005
芍药居 116.435914,39.977636
太阳宫 116.447469,39.972678
三元桥 116.456997,39.961508
亮马桥 116.461794,39.949415
农业展览馆 116.461724,39.941344
团结湖 116.461806,39.933747
金台夕照 116.461743,39.916838
劲松 116.461325,39.884387
潘家园 116.460926,39.875387
十里河 116.457983,39.866417
分钟寺 116.453976,39.852227
成寿寺 116.447531,39.845874
石榴庄 116.414103,39.845905
角门东 116.385649,39.845135
草桥 116.351387,39.845869
纪家庙 116.333381,39.844433
首经贸 116.320202,39.844463
丰台站 116.30454,39.849639
泥洼 116.304173,39.858609
西局 116.303819,39.86677
莲花桥 116.310347,39.897867
西钓鱼台 116.298064,39.923481
车道沟 116.293818,39.947923
长春桥 116.294255,39.958527
火器营 116.289058,39.965938
大钟寺 116.345139,39.966612
五道口 116.337742,39.992894
上地 116.320193,40.033007
西二旗 116.306295,40.053034
龙泽 116.319429,40.070882
回龙观 116.336116,40.0708
北苑 116.434518,40.042997
望京西 116.449884,39.995724
光熙门 116.431761,39.968337
柳芳 116.432728,39.958157
张郭庄 116.187193,39.8581
园博园 116.201643,39.861328
大瓦窑 116.240480,39.859470
郭庄子 116.253068,39.864841
大井 116.276061,39.865226
景泰 116.411026,39.86525
方庄 116.440244,39.865868
北工大西门 116.477318,39.875437
平乐园 116.477307,39.885275
枣营 116.474947,39.944132
东风北桥 116.485919,39.958375
将台 116.489496,39.971109
望京南 116.481634,39.984634
阜通 116.471740,39.991699
望京 116.469409,39.998521
东湖渠 116.467412,40.010670
来广营 116.466994,40.020588
善各庄 116.478195,40.027160
俸伯 116.684732,40.132573
顺义 116.657023,40.129994
石门 116.641117,40.129802
南法信 116.609535,40.128478
后沙峪 116.564211,40.114127
花梨坎 116.557593,40.084436
国展 116.555127,40.07003
孙河 116.5347,40.045113
马泉营 116.50348,40.033721
崔各庄 116.492968,40.022201
望京东 116.487105,40.0032
关庄 116.430947,40.001134
安立路 116.407845,40.002619
北沙滩 116.368143,40.001492
六道口 116.35267,40.000958
清华东路西口 116.33953,40.000673
农大南路 116.282272,40.021398
马连洼 116.27252,40.032637
西北旺 116.257923,40.048703
永丰南 116.248154,40.065575
永丰 116.238481,40.071868
屯佃 116.215850,40.068454
稻香湖路 116.188145,40.068936
温阳路 116.161361,40.068516
北安河 116.130428,40.068128
高碑店 116.531421,39.909448
传媒大学 116.554639,39.909215
双桥 116.57676,39.91014
管庄 116.599002,39.909090
八里桥 116.618658,39.906121
通州北苑 116.637252,39.903864
果园 116.646606,39.893393
九棵树 116.657533,39.890278
梨园 116.66872,39.883677
临河里 116.678811,39.875496
土桥 116.686349,39.871926
昌平西山口 116.195369,40.244629
十三陵景区 116.207637,40.240214
昌平 116.23359,40.22055
昌平东关 116.262059,40.221726
北邵洼 116.281949,40.222001
南邵 116.287534,40.207484
沙河高教园 116.280465,40.164666
沙河 116.288865,40.148278
巩华城 116.293979,40.1309
生命科学园 116.29423,40.09479
肖村 116.448364,39.834217
小红门 116.459226,39.827951
旧宫 116.460789,39.80691
亦庄桥 116.480307,39.803011
亦庄文化园 116.490632,39.80689
万源街 116.505446,39.802921
荣京东街 116.51339,39.793132
荣昌东街 116.521737,39.782799
同济南路 116.539835,39.7729
经海路 116.562245,39.783587
次渠南 116.581357,39.795118
次渠 116.591502,39.8035
亦庄火车站 116.601913,39.812607
大葆台 116.291681,39.80781
稻田 116.21884,39.794885
长阳 116.212692,39.763871
篱笆房 116.189486,39.760636
广阳城 116.184985,39.74793
良乡大学城北 116.183480,39.729906
良乡大学城 116.176541,39.723159
良乡大学城西 116.156282,39.723157
良乡南关 116.140804,39.723202
苏庄 116.125306,39.723188
3号航站楼 116.615579,40.052544
2号航站楼 116.592808,40.079311
新宫 116.365549,39.812592
西红门 116.328689,39.7898
高米店北 116.330787,39.773547
高米店南 116.331605,39.763489
枣园 116.332204,39.753458
清源路 116.332513,39.742724
黄村西大街 116.332631,39.731769
黄村火车站 116.332611,39.722966
义和庄 116.319079,39.712387
生物医药基地 116.321662,39.68651
天宫院 116.319932,39.670342
石厂 116.100334,39.889378
小园 116.114443,39.890465
栗园庄 116.123254,39.89578
上岸 116.122225,39.905138
桥户营 116.125809,39.912383
四道桥 116.13401,39.91603
颐和园西门 116.263248,39.985697
茶棚 116.2481,39.982113
万安 116.231977,39.984001
植物园 116.214853,39.993582
香山 116.204491,39.994056
阎村东 116.100817,39.729028
紫草坞 116.08672,39.724606
阎村 116.080165,39.716731
星城 116.06137,39.713681
大石河东 116.039906,39.709893
马各庄 116.016606,39.705103
饶乐府 116.006103,39.701471
房山城关 115.989599,39.706061
燕山 115.973395,39.719615"""

# 各线路站点顺序（用于绘制线路）- 来自 stationinfo.xlsx 预处理
LINE_STATIONS = {
    '1号线': ['苹果园','古城','八角游乐园','八宝山','玉泉路','五棵松','万寿路','公主坟','军事博物馆','木樨地','南礼士路','复兴门','西单','天安门西','天安门东','王府井','东单','建国门','永安里','国贸','大望路','四惠','四惠东'],
    '2号线': ['西直门','积水潭','鼓楼大街','安定门','雍和宫','东直门','东四十条','朝阳门','建国门','北京站','崇文门','前门','和平门','宣武门','长椿街','复兴门','阜成门','车公庄','西直门'],
    '4号线/大兴线': ['安河桥北','北宫门','西苑','圆明园','北京大学东门','中关村','海淀黄庄','人民大学','魏公村','国家图书馆','动物园','西直门','新街口','平安里','西四','灵境胡同','西单','宣武门','菜市口','陶然亭','北京南站','马家堡','角门西','公益西桥','宋家庄','刘家窑','蒲黄榆','天坛东门','磁器口','崇文门','东单','灯市口','东四','张自忠路','北新桥','雍和宫','和平里北街','和平西桥','惠新西街南口','惠新西街北口','大屯路东'],
    '5号线': ['天通苑北','天通苑','天通苑南','立水桥','立水桥南','北苑路北','大屯路东','惠新西街北口','惠新西街南口','和平西桥','和平里北街','雍和宫','北新桥','张自忠路','东四','灯市口','东单','崇文门','磁器口','天坛东门','蒲黄榆','刘家窑','宋家庄'],
    '6号线': ['金安桥','苹果园','杨庄','西黄村','廖公庄','田村','海淀五路居','慈寿寺','花园桥','白石桥南','车公庄西','车公庄','平安里','北海北','南锣鼓巷','东四','朝阳门','东大桥','呼家楼','金台路','十里堡','青年路','褡裢坡','黄渠','常营','草房','物资学院路','通州北关','北运河西','北运河东','郝家府','东夏园','潞城'],
    '7号线': ['北京西站','湾子','达官营','广安门内','菜市口','虎坊桥','珠市口','桥湾','磁器口','广渠门内','广渠门外','双井','九龙山','大郊亭','百子湾','化工','南楼梓庄','欢乐谷景区','垡头','双合','焦化厂'],
    '8号线（北段）': ['朱辛庄','育知路','平西府','回龙观东大街','霍营','育新','西小口','永泰庄','林萃桥','森林公园南门','奥林匹克公园','奥体中心','北土城','安华桥','安德里北街','鼓楼大街','什刹海','南锣鼓巷','中国美术馆','灯市口','王府井','前门','珠市口','天桥','永定门外','木樨园','海户屯','大红门','大红门南'],
    '8号线（南段）': ['大红门南','大红门','海户屯','木樨园','永定门外','天桥','珠市口','前门','王府井','灯市口','中国美术馆','南锣鼓巷','什刹海','鼓楼大街','安德里北街','安华桥','北土城','奥体中心','奥林匹克公园','森林公园南门','林萃桥','永泰庄','西小口','育新','霍营','回龙观东大街','平西府','育知路','朱辛庄'],
    '9号线': ['郭公庄','丰台科技园','科怡路','丰台南路','丰台东大街','七里庄','六里桥','六里桥东','北京西站','军事博物馆','白堆子','白石桥南','国家图书馆','巴沟'],
    '10号线': ['巴沟','苏州街','海淀黄庄','知春里','知春路','西土城','牡丹园','健德门','北土城','安贞门','惠新西街南口','芍药居','太阳宫','三元桥','亮马桥','农业展览馆','团结湖','呼家楼','金台夕照','国贸','双井','劲松','潘家园','十里河','分钟寺','成寿寺','宋家庄','石榴庄','大红门','角门东','角门西','草桥','纪家庙','首经贸','丰台站','泥洼','西局','六里桥','莲花桥','公主坟','西钓鱼台','慈寿寺','车道沟','长春桥','火器营','巴沟'],
    '13号线': ['西直门','大钟寺','知春路','五道口','上地','西二旗','龙泽','回龙观','霍营','立水桥','北苑','望京西','芍药居','光熙门','柳芳','东直门'],
    '14号线东段（含中段）': ['张郭庄','园博园','大瓦窑','郭庄子','大井','七里庄','西局','北京南站','景泰','蒲黄榆','方庄','十里河','九龙山','大望路','金台路','朝阳公园','枣营','东风北桥','将台','望京南','阜通','望京','东湖渠','来广营','善各庄'],
    '15号线': ['俸伯','顺义','石门','南法信','后沙峪','花梨坎','国展','孙河','马泉营','崔各庄','望京东','望京','望京西','关庄','大屯路东','安立路','奥林匹克公园','北沙滩','六道口','清华东路西口','西苑','农大南路','马连洼','西北旺','永丰南','永丰','屯佃','稻香湖路','温阳路','北安河'],
    '八通线': ['四惠','四惠东','高碑店','传媒大学','双桥','管庄','八里桥','通州北苑','果园','九棵树','梨园','临河里','土桥'],
    '昌平线': ['昌平西山口','十三陵景区','昌平','昌平东关','北邵洼','南邵','沙河高教园','沙河','巩华城','朱辛庄','生命科学园','西二旗'],
    '亦庄线': ['宋家庄','肖村','小红门','旧宫','亦庄桥','亦庄文化园','万源街','荣京东街','荣昌东街','同济南路','经海路','次渠南','次渠','亦庄火车站'],
    '房山线': ['郭公庄','大葆台','稻田','长阳','篱笆房','广阳城','良乡大学城北','良乡大学城','良乡大学城西','良乡南关','苏庄'],
    '首都机场线': ['东直门','三元桥','2号航站楼','3号航站楼'],
    '大兴机场线': ['草桥','新宫','西红门','高米店北','高米店南','枣园','清源路','黄村西大街','黄村火车站','义和庄','生物医药基地','天宫院'],
    'S1线': ['石厂','小园','栗园庄','上岸','桥户营','四道桥','金安桥'],
    '16号线': ['北安河','温阳路','稻香湖路','屯佃','永丰','永丰南','西北旺','马连洼','农大南路','西苑','颐和园西门','茶棚','万安','植物园','香山'],
    '燕房线': ['阎村东','紫草坞','阎村','星城','大石河东','马各庄','饶乐府','房山城关','燕山'],
}

# ============================================================
# 1. 解析站点坐标
# ============================================================

def parse_geo_data():
    """解析站点经纬度数据，返回 {站名: (经度, 纬度)} 字典"""
    geo = {}
    for line in STATION_GEO_RAW.strip().split('\n'):
        line = line.strip()
        if not line:
            continue
        parts = line.split(' ')
        name = parts[0]
        coords = parts[1].split(',')
        if name not in geo:  # 保留第一次出现的坐标
            geo[name] = (float(coords[0]), float(coords[1]))
    return geo

STATION_GEO = parse_geo_data()

# 获取所有唯一站点列表
ALL_STATIONS = list(STATION_GEO.keys())

# 构建站点→所属线路映射
STATION_TO_LINES = {}
for line_name, stations in LINE_STATIONS.items():
    for s in stations:
        if s not in STATION_TO_LINES:
            STATION_TO_LINES[s] = []
        if line_name not in STATION_TO_LINES[s]:
            STATION_TO_LINES[s].append(line_name)

# ============================================================
# 2. 全局状态存储（时序数据缓冲区）
# ============================================================

MAX_HISTORY = 720      # 最多保存 720 个数据点（约1小时，每5秒一个点）
TIME_SERIES = {
    'timestamps': deque(maxlen=MAX_HISTORY),
    'connectivity': deque(maxlen=MAX_HISTORY),    # 网络连通率 %
    'efficiency': deque(maxlen=MAX_HISTORY),       # 平均通行效率 km/h
    'fault_count': deque(maxlen=MAX_HISTORY),      # 故障站点数
    'vulnerable_count': deque(maxlen=MAX_HISTORY), # 高脆弱性站点数
    'avg_passengers': deque(maxlen=MAX_HISTORY),   # 站点平均通行人数（人次/5min）
}

# 站点状态持久化（避免每次刷新全随机，保证连续性）
STATION_STATE = {}  # {站名: {'status': 'normal'|'crowded'|'fault'|'vulnerable', 'score': float}}

def init_station_state():
    """初始化所有站点状态"""
    global STATION_STATE
    for name in ALL_STATIONS:
        r = random.random()
        if r < 0.75:
            status = 'normal'
        elif r < 0.90:
            status = 'crowded'
        elif r < 0.97:
            status = 'vulnerable'
        else:
            status = 'fault'
        # 各站客流基准值：枢纽站偏高，郊区站偏低，模拟真实差异
        base_pax = random.randint(800, 4000)
        STATION_STATE[name] = {
            'status': status,
            'score': round(random.uniform(0.1, 0.9), 3),
            'history': deque([random.uniform(0.1, 0.9) for _ in range(12)], maxlen=12),
            'base_passengers': base_pax,  # 该站基准客流（人次/5min）
        }

init_station_state()

# ============================================================
# 3b. 线路邻接图 & 级联失效引擎
# ============================================================

def build_adjacency():
    """
    根据 LINE_STATIONS 构建站点无向邻接表。
    同一线路上相邻站点互为邻居；换乘站自动连通所有共享该站的线路。
    """
    adj = {name: set() for name in ALL_STATIONS}
    for stations in LINE_STATIONS.values():
        for i in range(len(stations) - 1):
            a, b = stations[i], stations[i + 1]
            if a in adj and b in adj:
                adj[a].add(b)
                adj[b].add(a)
    return adj

ADJACENCY = build_adjacency()

# 级联失效全局状态
CASCADE_STATE = {
    'active': False,           # 是否有正在扩散的级联失效
    'fault_set': set(),        # 已失效站点集合
    'wave_fronts': set(),      # 当前传播波前（下一轮将感染的候选站点）
    'step': 0,                 # 扩散步数
    'triggered_by': None,      # 触发源站点
}

# 暴雨场景开关（全局）
RAINSTORM_MODE = {'active': False}

def trigger_cascade(station_name):
    """
    手动触发以 station_name 为源点的级联失效。
    立即将该站设为 fault，并计算第一波波前。
    """
    global CASCADE_STATE, STATION_STATE
    STATION_STATE[station_name]['status'] = 'fault'
    neighbors = ADJACENCY.get(station_name, set())
    CASCADE_STATE.update({
        'active': True,
        'fault_set': {station_name},
        'wave_fronts': set(neighbors),
        'step': 1,
        'triggered_by': station_name,
    })

def propagate_cascade():
    """
    每次调用推进一步级联传播：
    - 波前站点以一定概率转为 fault 或 vulnerable
    - 暴雨模式下传播概率更高
    - 故障总比例超过 25% 时自动停止（避免全网瘫痪）
    """
    global CASCADE_STATE, STATION_STATE
    if not CASCADE_STATE['active']:
        return

    max_fault_ratio = 0.25
    fault_ratio = len(CASCADE_STATE['fault_set']) / max(len(ALL_STATIONS), 1)
    if fault_ratio >= max_fault_ratio or not CASCADE_STATE['wave_fronts']:
        CASCADE_STATE['active'] = False
        return

    # 传播概率：距源点越远衰减越快；暴雨模式加成
    base_prob = max(0.05, 0.55 - CASCADE_STATE['step'] * 0.10)
    if RAINSTORM_MODE['active']:
        base_prob = min(0.80, base_prob * 1.5)

    new_faults = set()
    new_vulnerables = set()
    next_fronts = set()

    for s in CASCADE_STATE['wave_fronts']:
        if s in CASCADE_STATE['fault_set']:
            continue
        r = random.random()
        if r < base_prob:
            new_faults.add(s)
        elif r < base_prob + 0.25:
            new_vulnerables.add(s)
        # 继续向外扩散
        for nb in ADJACENCY.get(s, set()):
            if nb not in CASCADE_STATE['fault_set'] and nb not in new_faults:
                next_fronts.add(nb)

    for s in new_faults:
        STATION_STATE[s]['status'] = 'fault'
        CASCADE_STATE['fault_set'].add(s)
    for s in new_vulnerables:
        if STATION_STATE[s]['status'] not in ('fault',):
            STATION_STATE[s]['status'] = 'vulnerable'

    CASCADE_STATE['wave_fronts'] = next_fronts - CASCADE_STATE['fault_set']
    CASCADE_STATE['step'] += 1
    if not CASCADE_STATE['wave_fronts']:
        CASCADE_STATE['active'] = False

# ============================================================
# 3. 【真实接口替换处】模拟数据生成函数
# ============================================================

def get_real_time_subway_data():
    """
    ★★★ 真实接口替换处 ★★★
    
    当前：生成模拟数据，模拟北京地铁实时运行状态。
    替换：将此函数内容替换为真实接口调用即可，返回格式保持不变。
    
    真实接口对接示例（替换方法）：
    ─────────────────────────────────────────────────────────────
    import requests
    
    def get_real_time_subway_data():
        # 替换为实际API地址
        response = requests.get("http://your-api-endpoint/subway/realtime")
        raw = response.json()
        
        # 按照下方返回格式整理数据
        station_data = {}
        for item in raw['stations']:
            station_data[item['name']] = {
                'status': item['status'],           # normal/congested/fault/vulnerable
                'vulnerability_score': item['score'], # 0.0~1.0
                'speed': item['avg_speed'],           # km/h
                'predict_status': item['predict'],    # 1h预测
            }
        
        return {
            'stations': station_data,
            'network': {
                'connectivity': raw['connectivity'],   # 0~100
                'efficiency': raw['efficiency'],       # km/h
                'fault_count': raw['fault_count'],
                'vulnerable_count': raw['vulnerable_count'],
            },
            'timestamp': datetime.now(),
        }
    ─────────────────────────────────────────────────────────────
    
    返回格式说明：
    {
        'stations': {
            '站名': {
                'status': str,               # 状态：normal/congested/fault/vulnerable
                'vulnerability_score': float,  # 脆弱性评分 0.0~1.0
                'speed': float,              # 当前速度 km/h
                'passengers': int,           # 当前通行人数（人次/5min），故障时为 0
                'predict_status': str,       # 1小时预测状态
            },
            ...
        },
        'network': {
            'connectivity': float,           # 网络连通率 %
            'efficiency': float,             # 平均通行效率 km/h
            'fault_count': int,              # 故障站点数
            'vulnerable_count': int,         # 高脆弱站点数
            'avg_passengers': int,           # 站点平均通行人数（人次/5min）
        },
        'timestamp': datetime,              # 数据时间戳
    }
    """
    global STATION_STATE

    # ── 如有进行中的级联失效，先推进一步 ────────────────────
    propagate_cascade()

    # ── 模拟站点状态随机小幅漂移 ──────────────────────────────
    station_data = {}
    fault_count = 0
    vulnerable_count = 0

    for name, state in STATION_STATE.items():
        # 级联失效期间，已失效站点不做随机漂移
        if name in CASCADE_STATE['fault_set']:
            new_status = 'fault'
        else:
            r = random.random()
            current = state['status']
            if current == 'normal':
                if r < 0.02:    new_status = 'crowded'
                elif r < 0.005: new_status = 'fault'
                else:           new_status = 'normal'
            elif current == 'crowded':
                if r < 0.15:    new_status = 'normal'
                elif r < 0.02:  new_status = 'fault'
                else:           new_status = 'crowded'
            elif current == 'fault':
                if r < 0.10:    new_status = 'crowded'
                else:           new_status = 'fault'
            else:  # vulnerable
                if r < 0.10:    new_status = 'normal'
                elif r < 0.03:  new_status = 'fault'
                else:           new_status = 'vulnerable'

            # 确保非级联模式下故障站点总数 ≤ 5%
            if not CASCADE_STATE['active']:
                if new_status == 'fault' and fault_count >= max(1, int(len(ALL_STATIONS) * 0.05)):
                    new_status = 'crowded'

        state['status'] = new_status
        if new_status == 'fault':       fault_count += 1
        if new_status == 'vulnerable':  vulnerable_count += 1

        # 脆弱性评分小幅波动；暴雨模式下整体上浮
        delta = random.uniform(-0.02, 0.02)
        if RAINSTORM_MODE['active']:
            delta += 0.015
        state['score'] = max(0.05, min(0.99, state['score'] + delta))
        state['history'].append(state['score'])

        # 预测状态（简单规则：故障站点1小时后大概率恢复）
        predict_map = {'normal': 'normal', 'crowded': 'normal',
                       'fault': 'crowded', 'vulnerable': 'normal'}
        predict_status = predict_map[new_status] if random.random() < 0.7 else new_status

        lines = STATION_TO_LINES.get(name, ['未知线路'])

        # ── 模拟站点通行人数 ──────────────────────────────────
        # 拥挤时客流偏高，故障时清零；暴雨模式全网客流×1.5
        base_pax = state.get('base_passengers', 1500)
        rain_factor = 1.5 if RAINSTORM_MODE['active'] else 1.0
        if new_status == 'normal':
            passengers = int(base_pax * random.uniform(0.85, 1.15) * rain_factor)
        elif new_status == 'crowded':
            passengers = int(base_pax * random.uniform(1.3, 1.8) * rain_factor)  # 拥挤=车厢密度超载
        elif new_status == 'fault':
            passengers = 0                                                          # 失效=停运清零
        else:  # vulnerable
            passengers = int(base_pax * random.uniform(0.7, 1.1) * rain_factor)

        station_data[name] = {
            'status': new_status,
            'vulnerability_score': round(state['score'], 3),
            'passengers': passengers,          # 当前通行人数（人次/5min）
            'predict_status': predict_status,
            'lines': lines,
            'score_history': list(state['history']),
        }

    # ── 模拟网络级指标 ──────────────────────────────────────────
    base_connectivity = 100 - fault_count / len(ALL_STATIONS) * 100 * 3
    connectivity = round(max(85, min(100, base_connectivity + random.uniform(-1, 1))), 2)
    efficiency = round(random.uniform(30, 45), 1)

    # 站点平均通行人数（排除故障站点后求均值）
    active_pax = [info['passengers'] for info in station_data.values() if info['passengers'] > 0]
    avg_passengers = int(sum(active_pax) / len(active_pax)) if active_pax else 0

    return {
        'stations': station_data,
        'network': {
            'connectivity': connectivity,
            'efficiency': efficiency,
            'fault_count': fault_count,
            'vulnerable_count': vulnerable_count,
            'avg_passengers': avg_passengers,   # 站点平均通行人数
        },
        'timestamp': datetime.now(),
    }

# ============================================================
# 4. 地图绘制函数
# ============================================================

STATUS_COLOR = {
    'normal':     '#2EC15B',   # 绿色-正常
    'crowded':    '#F5A623',   # 黄色-拥挤（车厢人流密度超载）
    'fault':      '#E53935',   # 红色-故障/失效
    'vulnerable': '#FF7043',   # 橙色-高脆弱
}
STATUS_LABEL = {
    'normal': '正常', 'crowded': '拥挤',
    'fault': '失效', 'vulnerable': '高脆弱',
}
PREDICT_LABEL = {
    'normal': '正常', 'crowded': '拥挤',
    'fault': '失效', 'vulnerable': '高脆弱',
}

def build_metro_map(station_data, selected_station=None):
    """构建北京地铁网络地图 Plotly 图形"""
    fig = go.Figure()

    # ── 绘制线路 ──────────────────────────────────────────────
    for line_name, stations in LINE_STATIONS.items():
        lons, lats = [], []
        for s in stations:
            if s in STATION_GEO:
                lon, lat = STATION_GEO[s]
                lons.append(lon)
                lats.append(lat)
            else:
                lons.append(None)
                lats.append(None)
        color = LINE_COLORS.get(line_name, '#888888')
        # 暴雨模式下线路颜色加蓝色滤镜（透明度降低模拟能见度下降）
        opacity = 0.5 if RAINSTORM_MODE['active'] else 1.0
        fig.add_trace(go.Scattermapbox(
            lon=lons, lat=lats,
            mode='lines',
            line=dict(width=2.5, color=color),
            opacity=opacity,
            name=line_name,
            hoverinfo='skip',
            showlegend=True,
        ))

    # ── 按状态分组绘制站点 ────────────────────────────────────
    status_groups = {'normal': [], 'crowded': [], 'fault': [], 'vulnerable': []}
    for name, info in station_data.items():
        if name in STATION_GEO:
            status_groups[info['status']].append(name)

    for status, names in status_groups.items():
        if not names:
            continue
        lons = [STATION_GEO[n][0] for n in names]
        lats = [STATION_GEO[n][1] for n in names]
        hover_texts = []
        for n in names:
            info = station_data[n]
            lines_str = '、'.join(info.get('lines', ['未知']))
            pax = info.get('passengers', 0)
            pax_str = f"{pax:,} 人次/5min" if pax > 0 else "停运"
            hover_texts.append(
                f"<b>{n}</b><br>"
                f"线路：{lines_str}<br>"
                f"状态：{STATUS_LABEL[status]}<br>"
                f"脆弱性评分：{info['vulnerability_score']:.3f}<br>"
                f"当前通行人数：{pax_str}<br>"
                f"<i>点击查看详情 / 触发失效</i>"
            )
        size = 7 if status == 'normal' else 10
        fig.add_trace(go.Scattermapbox(
            lon=lons, lat=lats,
            mode='markers',
            marker=dict(size=size, color=STATUS_COLOR[status], opacity=0.9),
            text=hover_texts,
            hoverinfo='text',
            name=STATUS_LABEL[status] + '站点',
            showlegend=False,
            customdata=names,
        ))

    # ── 级联失效：波前涟漪扩散圈 ────────────────────────────
    # 用多层半透明大圆标注已失效站点，模拟"影响波及"动画感
    if CASCADE_STATE['active'] or CASCADE_STATE['fault_set']:
        # 波前站点：橙色半透明大圆（下一步将受影响）
        wf_names = [n for n in CASCADE_STATE['wave_fronts'] if n in STATION_GEO]
        if wf_names:
            for radius_size, alpha in [(22, 0.12), (16, 0.20), (10, 0.30)]:
                fig.add_trace(go.Scattermapbox(
                    lon=[STATION_GEO[n][0] for n in wf_names],
                    lat=[STATION_GEO[n][1] for n in wf_names],
                    mode='markers',
                    marker=dict(size=radius_size, color='#FF7043', opacity=alpha),
                    hoverinfo='skip', showlegend=False,
                ))
        # 失效源点：红色脉冲圈（三层，由大到小）
        src = CASCADE_STATE['triggered_by']
        if src and src in STATION_GEO:
            for radius_size, alpha in [(28, 0.08), (20, 0.18), (13, 0.35)]:
                fig.add_trace(go.Scattermapbox(
                    lon=[STATION_GEO[src][0]],
                    lat=[STATION_GEO[src][1]],
                    mode='markers',
                    marker=dict(size=radius_size, color='#E53935', opacity=alpha),
                    hoverinfo='skip', showlegend=False,
                ))

    # ── 暴雨场景：蓝色半透明网格覆盖层 ──────────────────────
    if RAINSTORM_MODE['active']:
        # 用稀疏格点模拟雨幕视觉效果
        rain_lons = [116.1 + i * 0.08 for i in range(10) for _ in range(8)]
        rain_lats = [39.65 + j * 0.08 for _ in range(10) for j in range(8)]
        fig.add_trace(go.Scattermapbox(
            lon=rain_lons, lat=rain_lats,
            mode='markers',
            marker=dict(size=18, color='#1565C0', opacity=0.07, symbol='circle'),
            hoverinfo='skip', showlegend=False,
        ))

    # ── 高亮选中站点 ──────────────────────────────────────────
    if selected_station and selected_station in STATION_GEO:
        lon, lat = STATION_GEO[selected_station]
        fig.add_trace(go.Scattermapbox(
            lon=[lon], lat=[lat],
            mode='markers',
            marker=dict(size=16, color='#FFD700', symbol='circle'),
            hoverinfo='skip',
            name='已选站点',
            showlegend=False,
        ))

    rain_title = '  🌧 暴雨模式' if RAINSTORM_MODE['active'] else ''
    cascade_title = f'  ⚡ 级联失效传播中（第{CASCADE_STATE["step"]}步）' if CASCADE_STATE['active'] else ''

    fig.update_layout(
        mapbox=dict(
            style='carto-darkmatter',
            center=dict(lon=116.39, lat=39.92),
            zoom=10.5,
        ),
        paper_bgcolor='#0A1628',
        plot_bgcolor='#0A1628',
        font=dict(color='#FFFFFF', size=11),
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(
            bgcolor='rgba(10,22,40,0.8)',
            bordercolor='#1E3A5F',
            borderwidth=1,
            font=dict(size=9, color='#CCDDFF'),
            x=0.01, y=0.99,
            tracegroupgap=1,
        ),
        height=580,
        uirevision='metro-map',
        annotations=[dict(
            x=0.5, y=0.97, xref='paper', yref='paper',
            text=f'<b style="color:#FF7043">{cascade_title}</b><b style="color:#64B5F6">{rain_title}</b>',
            showarrow=False, font=dict(size=13),
            bgcolor='rgba(10,22,40,0.75)',
            bordercolor='#FF7043' if cascade_title else '#1565C0',
            borderwidth=1, borderpad=4,
            visible=bool(cascade_title or rain_title),
        )] if (cascade_title or rain_title) else [],
    )
    return fig

# ============================================================
# 5. 趋势图 & 饼图
# ============================================================

def build_trend_chart():
    """构建韧性指标趋势折线图"""
    ts = list(TIME_SERIES['timestamps'])
    if not ts:
        return go.Figure()

    # 将时间戳转为字符串（仅显示时分秒）
    labels = [t.strftime('%H:%M:%S') for t in ts]
    # 仅显示最近60个点（5分钟内）
    n = min(60, len(labels))
    labels = labels[-n:]
    conn = list(TIME_SERIES['connectivity'])[-n:]
    eff = list(TIME_SERIES['efficiency'])[-n:]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=labels, y=conn,
        name='连通率(%)', mode='lines',
        line=dict(color='#00E5FF', width=2),
        fill='tozeroy', fillcolor='rgba(0,229,255,0.07)',
    ))
    fig.add_trace(go.Scatter(
        x=labels, y=eff,
        name='效率(km/h)', mode='lines',
        line=dict(color='#69FF47', width=2),
        yaxis='y2',
    ))
    fig.update_layout(
        paper_bgcolor='#0A1628',
        plot_bgcolor='#0D2040',
        font=dict(color='#AACCFF', size=10),
        margin=dict(l=40, r=40, t=30, b=30),
        height=200,
        legend=dict(
            bgcolor='rgba(10,22,40,0.8)',
            font=dict(size=9),
            x=0, y=1,
            orientation='h',
        ),
        xaxis=dict(
            showgrid=False, showticklabels=True,
            tickfont=dict(size=8),
            nticks=6,
        ),
        yaxis=dict(
            title='连通率(%)', range=[80, 102],
            gridcolor='#1E3A5F', showgrid=True,
        ),
        yaxis2=dict(
            title='效率(km/h)', overlaying='y', side='right',
            range=[0, 80], showgrid=False,
        ),
        title=dict(text='近期韧性趋势', font=dict(size=11, color='#7BB3FF'), x=0.5),
        uirevision='trend',
    )
    return fig


def build_pie_chart(station_data):
    """构建站点状态分布饼图"""
    counts = {'normal': 0, 'crowded': 0, 'fault': 0, 'vulnerable': 0}
    for info in station_data.values():
        counts[info['status']] += 1

    labels = [STATUS_LABEL[k] for k in counts]
    values = list(counts.values())
    colors = [STATUS_COLOR[k] for k in counts]

    fig = go.Figure(data=[go.Pie(
        labels=labels, values=values,
        hole=0.5,
        marker=dict(colors=colors, line=dict(color='#0A1628', width=1)),
        textfont=dict(size=10, color='white'),
        hovertemplate='%{label}: %{value}站 (%{percent})<extra></extra>',
    )])
    fig.update_layout(
        paper_bgcolor='#0A1628',
        font=dict(color='#AACCFF', size=10),
        margin=dict(l=10, r=10, t=30, b=10),
        height=200,
        showlegend=True,
        legend=dict(
            bgcolor='rgba(10,22,40,0)',
            font=dict(size=9),
            orientation='h',
            x=0.5, xanchor='center', y=-0.05,
        ),
        title=dict(text='站点状态分布', font=dict(size=11, color='#7BB3FF'), x=0.5),
        uirevision='pie',
    )
    return fig


def build_mini_spark(values, color='#00E5FF'):
    """构建迷你趋势小图"""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        y=list(values), mode='lines',
        line=dict(color=color, width=1.5),
        fill='tozeroy', fillcolor=f'rgba(0,229,255,0.1)',
    ))
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=0, r=0, t=0, b=0),
        height=40,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        showlegend=False,
    )
    return fig

# ============================================================
# 6. Dash 应用布局
# ============================================================

app = dash.Dash(
    __name__,
    title='北京地铁韧性监测系统',
    update_title=None,
    meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}],
)

def serve_layout():
    return html.Div(
        style={
            'backgroundColor': '#060F1E',
            'minHeight': '100vh',
            'fontFamily': '"Microsoft YaHei", "PingFang SC", Arial, sans-serif',
            'color': '#E0EAFF',
        },
        children=[
            # ── 顶部标题栏 ────────────────────────────────────
            html.Div(
                style={
                    'background': 'linear-gradient(90deg, #001845 0%, #0052D9 50%, #001845 100%)',
                    'borderBottom': '2px solid #0077FF',
                    'padding': '12px 24px',
                    'display': 'flex',
                    'alignItems': 'center',
                    'justifyContent': 'space-between',
                    'boxShadow': '0 2px 12px rgba(0,82,217,0.4)',
                },
                children=[
                    html.Div([
                        html.Div('◈', style={'color': '#00C8FF', 'fontSize': '20px', 'display': 'inline', 'marginRight': '8px'}),
                        html.Span('北京地铁交通网络韧性动态监测与可视化系统',
                                  style={'fontSize': '20px', 'fontWeight': 'bold', 'letterSpacing': '2px', 'color': '#FFFFFF'}),
                        html.Span(' | 北京市规划和自然资源委员会',
                                  style={'fontSize': '12px', 'color': '#90B8FF', 'marginLeft': '12px'}),
                    ]),
                    html.Div([
                        html.Span('数据更新时间：', style={'color': '#90B8FF', 'fontSize': '13px'}),
                        html.Span(id='update-time',
                                  style={'color': '#00E5FF', 'fontSize': '13px', 'fontFamily': 'monospace', 'marginRight': '20px'}),
                        html.Button(
                            '⟳ 手动刷新',
                            id='manual-refresh-btn',
                            style={
                                'background': 'linear-gradient(135deg, #0052D9, #003399)',
                                'color': '#FFFFFF',
                                'border': '1px solid #4488FF',
                                'padding': '6px 18px',
                                'borderRadius': '4px',
                                'cursor': 'pointer',
                                'fontSize': '13px',
                                'letterSpacing': '1px',
                                'marginRight': '10px',
                            }
                        ),
                        html.Button(
                            '🌧 暴雨模式：关',
                            id='rainstorm-btn',
                            style={
                                'background': 'linear-gradient(135deg, #1A237E, #283593)',
                                'color': '#90CAF9',
                                'border': '1px solid #3F51B5',
                                'padding': '6px 16px',
                                'borderRadius': '4px',
                                'cursor': 'pointer',
                                'fontSize': '13px',
                                'letterSpacing': '1px',
                            }
                        ),
                    ]),
                ]
            ),

            # ── 主内容区域 ────────────────────────────────────
            html.Div(
                style={'display': 'flex', 'height': 'calc(100vh - 62px)', 'gap': '0'},
                children=[

                    # ── 左侧面板（20%）: 韧性指标看板 ─────────
                    html.Div(
                        id='left-panel',
                        style={
                            'width': '20%',
                            'background': 'linear-gradient(180deg, #071428 0%, #0A1E3C 100%)',
                            'borderRight': '1px solid #1E3A5F',
                            'padding': '16px 12px',
                            'overflowY': 'auto',
                        },
                        children=[
                            html.Div('◆ 核心韧性指标', style={
                                'color': '#7BB3FF', 'fontSize': '13px', 'fontWeight': 'bold',
                                'letterSpacing': '2px', 'marginBottom': '14px',
                                'borderBottom': '1px solid #1E3A5F', 'paddingBottom': '8px',
                            }),
                            # 指标卡片容器
                            html.Div(id='kpi-cards'),
                            html.Div('◆ 高脆弱站点 TOP 5', style={
                                'color': '#FF8C42', 'fontSize': '12px', 'fontWeight': 'bold',
                                'letterSpacing': '2px', 'margin': '18px 0 10px',
                                'borderBottom': '1px solid #3A2010', 'paddingBottom': '6px',
                            }),
                            html.Div(id='vulnerable-top5'),
                        ]
                    ),

                    # ── 中间（60%）: 地图 ─────────────────────
                    html.Div(
                        style={'width': '60%', 'position': 'relative', 'backgroundColor': '#0A1628'},
                        children=[
                            dcc.Graph(
                                id='metro-map',
                                config={
                                    'scrollZoom': True,
                                    'displayModeBar': True,
                                    'modeBarButtonsToRemove': ['select2d', 'lasso2d'],
                                    'displaylogo': False,
                                },
                                style={'height': '100%'},
                            ),
                            # 图例说明覆盖层
                            html.Div(
                                style={
                                    'position': 'absolute', 'bottom': '12px', 'left': '12px',
                                    'background': 'rgba(10,22,40,0.85)',
                                    'border': '1px solid #1E3A5F',
                                    'borderRadius': '6px', 'padding': '8px 12px',
                                    'fontSize': '11px',
                                },
                                children=[
                                    html.Span('站点状态: ', style={'color': '#7BB3FF'}),
                                    *[html.Span(f'● {STATUS_LABEL[k]}  ', style={'color': STATUS_COLOR[k]})
                                      for k in ['normal', 'crowded', 'vulnerable', 'fault']],
                                ]
                            ),
                            # 站点详情弹窗
                            html.Div(id='station-popup', style={
                                'position': 'absolute', 'top': '12px', 'right': '12px',
                                'background': 'rgba(6,15,30,0.95)',
                                'border': '1px solid #0052D9',
                                'borderRadius': '8px', 'padding': '14px',
                                'minWidth': '220px', 'maxWidth': '280px',
                                'display': 'none',
                                'boxShadow': '0 4px 20px rgba(0,82,217,0.4)',
                                'fontSize': '12px',
                                'zIndex': '1000',
                            }),
                        ]
                    ),

                    # ── 右侧面板（20%）: 趋势图 ───────────────
                    html.Div(
                        style={
                            'width': '20%',
                            'background': 'linear-gradient(180deg, #071428 0%, #0A1E3C 100%)',
                            'borderLeft': '1px solid #1E3A5F',
                            'padding': '16px 12px',
                            'overflowY': 'auto',
                        },
                        children=[
                            html.Div('◆ 动态趋势监控', style={
                                'color': '#7BB3FF', 'fontSize': '13px', 'fontWeight': 'bold',
                                'letterSpacing': '2px', 'marginBottom': '14px',
                                'borderBottom': '1px solid #1E3A5F', 'paddingBottom': '8px',
                            }),
                            dcc.Graph(id='trend-chart', config={'displayModeBar': False}),
                            html.Div(style={'height': '10px'}),
                            dcc.Graph(id='pie-chart', config={'displayModeBar': False}),
                            html.Div('◆ 当前告警', style={
                                'color': '#FF5252', 'fontSize': '12px', 'fontWeight': 'bold',
                                'letterSpacing': '2px', 'margin': '14px 0 8px',
                                'borderBottom': '1px solid #3A1010', 'paddingBottom': '6px',
                            }),
                            html.Div(id='alert-list'),
                        ]
                    ),
                ]
            ),

            # ── 隐藏数据存储 ──────────────────────────────────
            dcc.Store(id='station-data-store'),
            dcc.Store(id='selected-station-store', data=None),
            dcc.Store(id='cascade-trigger-store', data=None),   # 手动触发级联失效
            dcc.Store(id='rainstorm-store', data=False),        # 暴雨模式开关

            # ── 定时器：每5秒触发一次数据刷新 ────────────────
            dcc.Interval(id='auto-refresh-interval', interval=5000, n_intervals=0),
        ]
    )

app.layout = serve_layout

# ============================================================
# 7. 回调：数据刷新（核心逻辑）
# ============================================================

@app.callback(
    Output('station-data-store', 'data'),
    Output('update-time', 'children'),
    Input('auto-refresh-interval', 'n_intervals'),
    Input('manual-refresh-btn', 'n_clicks'),
    Input('cascade-trigger-store', 'data'),
    State('rainstorm-store', 'data'),
)
def refresh_data(n_intervals, n_clicks, cascade_station, rainstorm_on):
    """
    每5秒（或手动触发）调用数据函数，更新全局时序缓冲区
    真实接口替换：只需修改 get_real_time_subway_data() 函数即可
    """
    # 同步暴雨开关到全局
    RAINSTORM_MODE['active'] = bool(rainstorm_on)

    # 如果本次触发来自"级联触发"按钮，先激活级联
    from dash import ctx
    if ctx.triggered_id == 'cascade-trigger-store' and cascade_station:
        trigger_cascade(cascade_station)
    data = get_real_time_subway_data()

    # 追加到时序数据
    TIME_SERIES['timestamps'].append(data['timestamp'])
    TIME_SERIES['connectivity'].append(data['network']['connectivity'])
    TIME_SERIES['efficiency'].append(data['network']['efficiency'])
    TIME_SERIES['fault_count'].append(data['network']['fault_count'])
    TIME_SERIES['vulnerable_count'].append(data['network']['vulnerable_count'])
    TIME_SERIES['avg_passengers'].append(data['network']['avg_passengers'])

    # 将站点数据序列化（datetime 不可直接存入 dcc.Store）
    station_serializable = {}
    for name, info in data['stations'].items():
        station_serializable[name] = {
            'status': info['status'],
            'vulnerability_score': info['vulnerability_score'],
            'passengers': info['passengers'],
            'predict_status': info['predict_status'],
            'lines': info.get('lines', []),
            'score_history': info.get('score_history', []),
        }

    time_str = data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
    return station_serializable, time_str


@app.callback(
    Output('metro-map', 'figure'),
    Output('kpi-cards', 'children'),
    Output('vulnerable-top5', 'children'),
    Output('trend-chart', 'figure'),
    Output('pie-chart', 'figure'),
    Output('alert-list', 'children'),
    Input('station-data-store', 'data'),
    State('selected-station-store', 'data'),
)
def update_visuals(station_data, selected_station):
    """根据最新数据更新所有可视化组件"""
    if not station_data:
        empty = go.Figure()
        return empty, [], [], empty, empty, []

    # ── 地图 ──────────────────────────────────────────────
    map_fig = build_metro_map(station_data, selected_station)

    # ── KPI 指标卡片 ─────────────────────────────────────
    net = {
        'connectivity': list(TIME_SERIES['connectivity'])[-1] if TIME_SERIES['connectivity'] else 98,
        'efficiency':   list(TIME_SERIES['efficiency'])[-1]   if TIME_SERIES['efficiency']   else 38,
        'fault_count':  list(TIME_SERIES['fault_count'])[-1]  if TIME_SERIES['fault_count']  else 0,
        'vulnerable_count': list(TIME_SERIES['vulnerable_count'])[-1] if TIME_SERIES['vulnerable_count'] else 0,
        'avg_passengers': list(TIME_SERIES['avg_passengers'])[-1] if TIME_SERIES['avg_passengers'] else 0,
    }

    def kpi_card(title, value, unit, series, color):
        spark = build_mini_spark(series, color)
        return html.Div(
            style={
                'background': 'rgba(0,82,217,0.12)',
                'border': '1px solid #1E3A5F',
                'borderRadius': '6px',
                'padding': '10px',
                'marginBottom': '10px',
            },
            children=[
                html.Div(title, style={'color': '#90B8FF', 'fontSize': '11px', 'marginBottom': '4px'}),
                html.Div([
                    html.Span(str(value), style={'fontSize': '22px', 'fontWeight': 'bold', 'color': color}),
                    html.Span(f' {unit}', style={'fontSize': '11px', 'color': '#607090'}),
                ]),
                dcc.Graph(figure=spark, config={'displayModeBar': False}, style={'height': '40px'}),
            ]
        )

    kpi_cards = [
        kpi_card('网络连通率', net['connectivity'], '%',
                 list(TIME_SERIES['connectivity']), '#00E5FF'),
        kpi_card('平均通行效率', net['efficiency'], 'km/h',
                 list(TIME_SERIES['efficiency']), '#69FF47'),
        kpi_card('站点平均通行人数', f"{net['avg_passengers']:,}", '人次/5min',
                 list(TIME_SERIES['avg_passengers']), '#B983FF'),
        kpi_card('故障站点数', net['fault_count'], '个',
                 list(TIME_SERIES['fault_count']), '#FF5252'),
        kpi_card('高脆弱站点数', net['vulnerable_count'], '个',
                 list(TIME_SERIES['vulnerable_count']), '#FF8C42'),
    ]

    # ── TOP5 脆弱站点 ─────────────────────────────────────
    sorted_stations = sorted(
        station_data.items(),
        key=lambda x: x[1]['vulnerability_score'],
        reverse=True
    )[:5]
    top5_items = []
    for rank, (name, info) in enumerate(sorted_stations, 1):
        color = STATUS_COLOR.get(info['status'], '#888')
        top5_items.append(html.Div(
            style={
                'display': 'flex', 'alignItems': 'center',
                'padding': '6px 8px', 'marginBottom': '6px',
                'background': 'rgba(255,140,66,0.08)',
                'border': '1px solid rgba(255,140,66,0.2)',
                'borderRadius': '4px',
            },
            children=[
                html.Span(f'#{rank}', style={'color': '#FF8C42', 'fontSize': '12px', 'width': '24px', 'fontWeight': 'bold'}),
                html.Div([
                    html.Div(name, style={'color': '#FFFFFF', 'fontSize': '12px'}),
                    html.Div(f"评分: {info['vulnerability_score']:.3f}  {STATUS_LABEL[info['status']]}",
                             style={'color': color, 'fontSize': '10px'}),
                ], style={'flex': 1}),
                html.Span('▲' if info['vulnerability_score'] > 0.6 else '→',
                          style={'color': '#FF5252' if info['vulnerability_score'] > 0.6 else '#F5A623'}),
            ]
        ))

    # ── 趋势图 & 饼图 ────────────────────────────────────
    trend_fig = build_trend_chart()
    pie_fig = build_pie_chart(station_data)

    # ── 告警列表 ──────────────────────────────────────────
    fault_stations = [(n, info) for n, info in station_data.items() if info['status'] == 'fault']
    alerts = []
    for name, info in fault_stations[:5]:  # 最多显示5条
        lines_str = '、'.join(info.get('lines', ['未知']))
        alerts.append(html.Div(
            style={
                'background': 'rgba(229,57,53,0.12)',
                'border': '1px solid rgba(229,57,53,0.3)',
                'borderRadius': '4px',
                'padding': '6px 8px',
                'marginBottom': '6px',
                'fontSize': '11px',
            },
            children=[
                html.Div(f'⚠ {name}', style={'color': '#FF5252', 'fontWeight': 'bold'}),
                html.Div(f'{lines_str} | 故障停运', style={'color': '#FF8A80'}),
            ]
        ))
    if not alerts:
        alerts = [html.Div('✓ 暂无告警信息', style={'color': '#4CAF50', 'fontSize': '12px', 'textAlign': 'center', 'padding': '10px'})]

    return map_fig, kpi_cards, top5_items, trend_fig, pie_fig, alerts


@app.callback(
    Output('station-popup', 'children'),
    Output('station-popup', 'style'),
    Output('selected-station-store', 'data'),
    Input('metro-map', 'clickData'),
    State('selected-station-store', 'data'),
    State('station-data-store', 'data'),
)
def show_station_popup(click_data, current_selected, station_data):
    """点击地图站点时，弹出站点详情"""
    base_style = {
        'position': 'absolute', 'top': '12px', 'right': '12px',
        'background': 'rgba(6,15,30,0.95)',
        'border': '1px solid #0052D9',
        'borderRadius': '8px', 'padding': '14px',
        'minWidth': '220px', 'maxWidth': '280px',
        'boxShadow': '0 4px 20px rgba(0,82,217,0.4)',
        'fontSize': '12px', 'zIndex': '1000',
    }

    if not click_data or not station_data:
        return [], {**base_style, 'display': 'none'}, None

    point = click_data['points'][0]
    station_name = point.get('customdata')
    if not station_name or station_name not in station_data:
        return [], {**base_style, 'display': 'none'}, None

    info = station_data[station_name]
    lines_str = '、'.join(info.get('lines', ['未知']))
    status_color = STATUS_COLOR.get(info['status'], '#888')
    already_faulted = info['status'] == 'fault'
    cascade_active = CASCADE_STATE['active']

    # 触发按钮提示文字
    if already_faulted:
        btn_text = '⚡ 已失效'
        btn_disabled_style = {
            'width': '100%', 'marginTop': '10px', 'padding': '7px',
            'background': '#1A1A2E', 'color': '#555', 'border': '1px solid #333',
            'borderRadius': '4px', 'fontSize': '12px', 'cursor': 'not-allowed',
        }
    elif cascade_active:
        btn_text = '⚡ 级联传播中…'
        btn_disabled_style = {
            'width': '100%', 'marginTop': '10px', 'padding': '7px',
            'background': '#1A1A2E', 'color': '#888', 'border': '1px solid #444',
            'borderRadius': '4px', 'fontSize': '12px', 'cursor': 'not-allowed',
        }
    else:
        btn_text = '⚡ 触发级联失效'
        btn_disabled_style = {
            'width': '100%', 'marginTop': '10px', 'padding': '7px',
            'background': 'linear-gradient(135deg, #7B1FA2, #4A148C)',
            'color': '#E1BEE7', 'border': '1px solid #9C27B0',
            'borderRadius': '4px', 'fontSize': '12px', 'cursor': 'pointer',
            'fontWeight': 'bold', 'letterSpacing': '1px',
        }

    popup_content = [
        html.Div([
            html.Span('📍 ', style={'fontSize': '14px'}),
            html.Span(station_name, style={'fontSize': '15px', 'fontWeight': 'bold', 'color': '#00E5FF'}),
            html.Span(' ✕', style={'float': 'right', 'cursor': 'pointer', 'color': '#607090', 'fontSize': '14px'}),
        ], style={'marginBottom': '10px', 'borderBottom': '1px solid #1E3A5F', 'paddingBottom': '8px'}),

        html.Div([
            html.Span('所属线路：', style={'color': '#90B8FF'}),
            html.Span(lines_str, style={'color': '#FFFFFF'}),
        ], style={'marginBottom': '6px'}),

        html.Div([
            html.Span('当前状态：', style={'color': '#90B8FF'}),
            html.Span(f"● {STATUS_LABEL[info['status']]}", style={'color': status_color, 'fontWeight': 'bold'}),
        ], style={'marginBottom': '6px'}),

        html.Div([
            html.Span('当前通行人数：', style={'color': '#90B8FF'}),
            html.Span(
                f"{info.get('passengers', 0):,} 人次/5min" if info.get('passengers', 0) > 0 else '停运（0人）',
                style={'color': '#B983FF' if info.get('passengers', 0) > 0 else '#FF5252'},
            ),
        ], style={'marginBottom': '6px'}),

        html.Div([
            html.Span('脆弱性评分：', style={'color': '#90B8FF'}),
            html.Span(f"{info['vulnerability_score']:.3f}", style={
                'color': '#FF8C42' if info['vulnerability_score'] > 0.6 else '#69FF47',
                'fontWeight': 'bold',
            }),
        ], style={'marginBottom': '6px'}),

        html.Div(
            style={
                'background': 'rgba(0,180,255,0.08)',
                'border': '1px dashed #1E5A7F',
                'borderRadius': '4px', 'padding': '6px 8px', 'marginTop': '8px',
            },
            children=[
                html.Div('⏱ 1小时预测状态（预测）', style={'color': '#7BB3FF', 'fontSize': '10px', 'marginBottom': '4px'}),
                html.Span(
                    f"→ {PREDICT_LABEL.get(info['predict_status'], '未知')}",
                    style={'color': STATUS_COLOR.get(info['predict_status'], '#888'), 'fontWeight': 'bold'},
                ),
                html.Span(' [预测]', style={'color': '#607090', 'fontSize': '10px'}),
            ]
        ),

        # ── 触发级联失效按钮 ──────────────────────────────────
        html.Button(
            btn_text,
            id='trigger-cascade-btn',
            n_clicks=0,
            disabled=(already_faulted or cascade_active),
            **{'data-station': station_name},
            style=btn_disabled_style,
        ),
        # 级联失效说明
        html.Div(
            '模拟该站失效后，故障沿线路网络逐步向邻站传播',
            style={'color': '#607090', 'fontSize': '9px', 'marginTop': '4px', 'textAlign': 'center'}
        ),
    ]

    return popup_content, {**base_style, 'display': 'block'}, station_name


@app.callback(
    Output('rainstorm-store', 'data'),
    Output('rainstorm-btn', 'children'),
    Output('rainstorm-btn', 'style'),
    Input('rainstorm-btn', 'n_clicks'),
    State('rainstorm-store', 'data'),
    prevent_initial_call=True,
)
def toggle_rainstorm(n_clicks, current):
    """切换暴雨场景模式"""
    new_state = not bool(current)
    RAINSTORM_MODE['active'] = new_state
    if new_state:
        label = '🌧 暴雨模式：开'
        style = {
            'background': 'linear-gradient(135deg, #1565C0, #0D47A1)',
            'color': '#E3F2FD', 'border': '1px solid #42A5F5',
            'padding': '6px 16px', 'borderRadius': '4px',
            'cursor': 'pointer', 'fontSize': '13px', 'letterSpacing': '1px',
            'boxShadow': '0 0 10px rgba(66,165,245,0.5)',
        }
    else:
        label = '🌧 暴雨模式：关'
        style = {
            'background': 'linear-gradient(135deg, #1A237E, #283593)',
            'color': '#90CAF9', 'border': '1px solid #3F51B5',
            'padding': '6px 16px', 'borderRadius': '4px',
            'cursor': 'pointer', 'fontSize': '13px', 'letterSpacing': '1px',
        }
    return new_state, label, style


@app.callback(
    Output('cascade-trigger-store', 'data'),
    Input('trigger-cascade-btn', 'n_clicks'),
    State('selected-station-store', 'data'),
    prevent_initial_call=True,
)
def handle_cascade_trigger(n_clicks, station_name):
    """弹窗"触发级联失效"按钮 → 写入 cascade-trigger-store，驱动 refresh_data 激活级联"""
    if n_clicks and station_name:
        return station_name
    return None


# ============================================================
# 8. 主入口
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("  北京地铁网络韧性动态监测系统  |  正在启动...")
    print("=" * 60)
    print(f"  站点总数: {len(ALL_STATIONS)} 个")
    print(f"  线路总数: {len(LINE_STATIONS)} 条")
    print(f"  访问地址: http://127.0.0.1:8050")
    print("=" * 60)
    app.run(debug=False, host='127.0.0.1', port=8050)