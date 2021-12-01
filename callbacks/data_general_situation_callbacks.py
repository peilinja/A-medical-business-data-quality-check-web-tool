import json
import io
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
import plotly.express as px
from dash.dependencies import Output, Input, State
from datetime import datetime, timedelta
from server import app
import plotly.graph_objects as go
import plotly.express as px
from sqlalchemy import create_engine
from flask import send_file
import os
from joblib import Parallel, delayed
from dash.exceptions import PreventUpdate

# -----------------------------------------------------------------------------------------------------    一级图一   ----------------------------------------------------------------------------------------------------------------------
# 获取概览一级第一张图数据
def get_first_lev_first_fig_date(engine):
    res_数据时间缺失及汇总 = pd.DataFrame(columns=['业务类型', '问题数', '总数', '问题数量占比'])
    # 问题类别、问题数据量统计、全数据统计
    bus_dic = {
        # '患者基本信息': ['select count(distinct caseid) as num from overall where in_time is null or out_time is null','select count(distinct caseid) as num from overall'],
               '入院时间': ['select count(distinct caseid) as num from overall where in_time is null ',
                          'select count(distinct caseid) as num from overall'],
               '出院时间': ['select count(distinct caseid) as num from overall where  out_time is null',
                          'select count(distinct caseid) as num from overall'],
               '手术': ['select count(1) as num from oper2 where  BEGINTIME is null or ENDTIME is null ','select count(1) as num from  oper2  '],
               '给药': ['select count(1) as num from ANTIBIOTICS where  BEGINTIME is null or ENDTIME is null ','select count(1) as num from  ANTIBIOTICS  '],
               '入出转': ['select count(1) as num from DEPARTMENT where  BEGINTIME is null or ENDTIME is null ','select count(1) as num  from DEPARTMENT   '],
               '菌检出': ['select count(1) as num from BACTERIA where  REQUESTTIME is null ','select count(1) as num  from BACTERIA   '],
               '体温': ['select count(1) as num from TEMPERATURE where  RECORDDATE is null ','select count(1) as num  from TEMPERATURE   '],
               '药敏': ['select count(1) as num from DRUGSUSCEPTIBILITY where  REQUESTTIME is null or REPORTTIME is null ','select count(1) as num  from DRUGSUSCEPTIBILITY   '],
               '检查': ['select count(1) as num from EXAM where  EXAM_DATE is null   ','select count(1) as num  from EXAM   '],
               '生化': ['select count(1) as num from ROUTINE2 where  REQUESTTIME is null or REPORTTIME is null ','select count(1) as num  from ROUTINE2   '],
               '三管': ['select count(1) as num from TREATMENT1 where  BEGINTIME is null or ENDTIME is null ','select count(1) as num  from TREATMENT1   '],
               }

    for bus in bus_dic:
        try:
            count_时间为空 = pd.read_sql(bus_dic[bus][0],con=engine)['num'][0]
            count_总 = pd.read_sql(bus_dic[bus][1],con=engine)['num'][0]
            res_数据时间缺失及汇总.loc[res_数据时间缺失及汇总.shape[0]] = [bus,count_时间为空,count_总,round(count_时间为空 / count_总, 4) * 100]
        except:
            res_数据时间缺失及汇总.loc[res_数据时间缺失及汇总.shape[0]] = [bus,-1,-1,-1]
        print('一级图一',bus)
    return res_数据时间缺失及汇总
# 更新一级图一
@app.callback(
    Output('first_level_first_fig','figure'),
    Output('general_situation_first_level_first_fig_data','data'),

    Input('general_situation_first_level_first_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_first_level_first_fig(general_situation_first_level_first_fig_data,db_con_url,count_time):
    if db_con_url is None :
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        if general_situation_first_level_first_fig_data is None:
            general_situation_first_level_first_fig_data = {}
            first_level_first_fig_data = get_first_lev_first_fig_date(engine)
            general_situation_first_level_first_fig_data['first_level_first_fig_data'] = first_level_first_fig_data.to_json(orient='split', date_format='iso')
            general_situation_first_level_first_fig_data['hosname'] = db_con_url['hosname']
            general_situation_first_level_first_fig_data = json.dumps(general_situation_first_level_first_fig_data)
        else:
            general_situation_first_level_first_fig_data = json.loads(general_situation_first_level_first_fig_data)
            if db_con_url['hosname'] != general_situation_first_level_first_fig_data['hosname']:
                first_level_first_fig_data = get_first_lev_first_fig_date(engine)
                general_situation_first_level_first_fig_data['first_level_first_fig_data'] = first_level_first_fig_data.to_json(orient='split',date_format='iso')
                general_situation_first_level_first_fig_data['hosname'] = db_con_url['hosname']
                general_situation_first_level_first_fig_data = json.dumps(general_situation_first_level_first_fig_data)
            else:
                first_level_first_fig_data = pd.read_json(general_situation_first_level_first_fig_data['first_level_first_fig_data'], orient='split')
                general_situation_first_level_first_fig_data = dash.no_update
            #


        fig_概览一级_时间缺失 = make_subplots(specs=[[{"secondary_y": True}]])
        res_数据时间缺失及汇总 = first_level_first_fig_data.sort_values(['问题数'], ascending=False)
        # 各业务缺失数量--柱形图
        fig_概览一级_时间缺失.add_trace(
            go.Bar(x=res_数据时间缺失及汇总['业务类型'], y=res_数据时间缺失及汇总['问题数'], name="问题数量",
                   marker_color=px.colors.qualitative.Dark24, ),
            secondary_y=False,
        )
        # 各业务缺失数量占比--折线图
        fig_概览一级_时间缺失.add_trace(
            go.Scatter(x=res_数据时间缺失及汇总['业务类型'], y=res_数据时间缺失及汇总['问题数量占比'], name="问题数量占比", ),
            secondary_y=True,
        )

        # 设置X轴title
        fig_概览一级_时间缺失.update_xaxes(tickangle=45,title_text="业务指标")

        # 设置Y轴title
        fig_概览一级_时间缺失.update_yaxes(title_text="缺失数量", secondary_y=False)
        fig_概览一级_时间缺失.update_yaxes(title_text="缺失占比（%）", secondary_y=True)
        # 设置水平图例及位置
        fig_概览一级_时间缺失.update_layout(
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ))
        # 设置图片边距
        fig_概览一级_时间缺失.update_layout(margin=dict(l=20, r=20, t=20, b=20), )

        return fig_概览一级_时间缺失,general_situation_first_level_first_fig_data
# 下载一级图一明细
@app.callback(
    Output('first_level_first_fig_data_detail', 'data'),
    Input('first_level_first_fig_data_detail_down','n_clicks'),
    Input("db_con_url", "data"),
    prevent_initial_call=True,
)
def download_first_level_first_fig_data_detail(n_clicks,db_con_url):
    if db_con_url is None :
        return dash.no_update
    else:
        if n_clicks is not None and n_clicks>0:
            n_clicks = 0
            db_con_url = json.loads(db_con_url)
            engine = create_engine(db_con_url['db'])
            bus_dic = {
                '入院时间': 'select * from overall where in_time is null ',
                '出院时间': 'select * from overall where out_time is null',
                '手术': 'select * from oper2 where  BEGINTIME is null or ENDTIME is null ',
                '给药': 'select * from ANTIBIOTICS where  BEGINTIME is null or ENDTIME is null ',
                '入出转': 'select * from DEPARTMENT where  BEGINTIME is null or ENDTIME is null ',
                '菌检出': 'select * from BACTERIA where  REQUESTTIME is null ',
                '药敏': 'select * from DRUGSUSCEPTIBILITY where  REQUESTTIME is null or REPORTTIME is null ',
                '检查': 'select * from EXAM where  EXAM_DATE is null',
                '生化': 'select * from ROUTINE2 where  REQUESTTIME is null or REPORTTIME is null ',
                '三管': 'select * from TREATMENT1 where  BEGINTIME is null or ENDTIME is null ',
            }
            output = io.BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            for key in bus_dic.keys():
                try:
                    temp = pd.read_sql(bus_dic[key],con=engine)
                    if temp.shape[0]>0:
                        temp.to_excel(writer, sheet_name=key)
                except:
                    error_df = pd.DataFrame(['明细数据获取出错'],columns=[key])
                    error_df.to_excel(writer, sheet_name = key)
            writer.save()
            data = output.getvalue()
            hosName = db_con_url['hosname']
            return dcc.send_bytes(data, f'{hosName}各业务时间缺失数量占比.xlsx')
        else:
            return dash.no_update







# -----------------------------------------------------------------------------------------------------    一级图二   ----------------------------------------------------------------------------------------------------------------------
# 获取概览一级第二张图数据
def get_first_lev_second_fig_date(engine,btime,etime):
    res_数据关键字缺失及汇总 = pd.DataFrame(columns=['业务类型', '问题数', '总数', '关键字缺失占比'])
    bus_dic = {'用药目的': [f"select count(1) as num from ANTIBIOTICS where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and (GOAL is null or replace(GOAL,' ','') is null)",
                          f"select count(1) as num  from ANTIBIOTICS where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' "],
               '药敏结果': [f"select count(1) as num from drugsusceptibility where substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and SUSCEPTIBILITY is null or replace(SUSCEPTIBILITY,' ','') is null",
                      f"select count(1) as num  from drugsusceptibility where substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' "],
               '手术名称': [f"select count(1) as num from oper2 where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and (OPER_NAME is null or replace(OPER_NAME,' ','') is null)",
                      f"select count(1) as num  from oper2  where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' "],
               '手术切口等级': [f"select count(1) as num from oper2 where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and ( WOUND_GRADE is null or replace(WOUND_GRADE,' ','') is null)",
                       f"select count(1) as num  from oper2 where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}'  "],
               '出入院科室': [f"select count(1) as num from overall where substr(IN_TIME,1,7)>='{btime}' and substr(IN_TIME,1,7)<='{etime}' and ( IN_DEPT is null or replace(IN_DEPT,' ','') is null or OUT_DEPT is null or replace(OUT_DEPT,' ','') is null )",
                       f"select count(1) as num  from overall where substr(IN_TIME,1,7)>='{btime}' and substr(IN_TIME,1,7)<='{etime}' "],
               '入出转科室': [f"select count(1) as num from department where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and ( DEPT is null or replace(DEPT,' ','') is null)",
                      f"select count(1) as num  from department  where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}'  "]
               }
    for bus in bus_dic:
        try:
            count_时间为空 = pd.read_sql(bus_dic[bus][0],con=engine)['num'][0]
            count_总 = pd.read_sql(bus_dic[bus][1],con=engine)['num'][0]
            res_数据关键字缺失及汇总.loc[res_数据关键字缺失及汇总.shape[0]] = [bus,count_时间为空,count_总,round(count_时间为空 / count_总, 4) * 100]
        except:
            res_数据关键字缺失及汇总.loc[res_数据关键字缺失及汇总.shape[0]] = [bus,-1,-1,-1]
        print('一级图二', bus)
    return res_数据关键字缺失及汇总
# 更新一级图二
@app.callback(
    Output('first_level_second_fig','figure'),
    Output('general_situation_first_level_second_fig_data','data'),

    Input('general_situation_first_level_second_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_first_level_second_fig(general_situation_first_level_second_fig_data,db_con_url,count_time):
    if db_con_url is None :
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if general_situation_first_level_second_fig_data is None:
            general_situation_first_level_second_fig_data = {}
            first_level_second_fig_data = get_first_lev_second_fig_date(engine,btime,etime)
            general_situation_first_level_second_fig_data['first_level_second_fig_data'] = first_level_second_fig_data.to_json(orient='split', date_format='iso')
            general_situation_first_level_second_fig_data['hosname'] = db_con_url['hosname']
            general_situation_first_level_second_fig_data['btime'] = btime 
            general_situation_first_level_second_fig_data['etime'] = etime
            general_situation_first_level_second_fig_data = json.dumps(general_situation_first_level_second_fig_data)
        else:
            general_situation_first_level_second_fig_data = json.loads(general_situation_first_level_second_fig_data)
            if db_con_url['hosname'] != general_situation_first_level_second_fig_data['hosname']:
                first_level_second_fig_data = get_first_lev_second_fig_date(engine, btime, etime)
                general_situation_first_level_second_fig_data['first_level_second_fig_data'] = first_level_second_fig_data.to_json(orient='split',date_format='iso')
                general_situation_first_level_second_fig_data['hosname'] = db_con_url['hosname']
                general_situation_first_level_second_fig_data['btime'] = btime 
                general_situation_first_level_second_fig_data['etime'] = etime
                general_situation_first_level_second_fig_data = json.dumps( general_situation_first_level_second_fig_data)
            else:
                if general_situation_first_level_second_fig_data['btime'] != btime or general_situation_first_level_second_fig_data['etime'] != etime:
                    first_level_second_fig_data = get_first_lev_second_fig_date(engine, btime, etime)
                    general_situation_first_level_second_fig_data[ 'first_level_second_fig_data'] = first_level_second_fig_data.to_json(orient='split', date_format='iso')
                    general_situation_first_level_second_fig_data['btime'] = btime
                    general_situation_first_level_second_fig_data['etime'] = etime
                    general_situation_first_level_second_fig_data = json.dumps(general_situation_first_level_second_fig_data)
                else:
                    first_level_second_fig_data = pd.read_json(general_situation_first_level_second_fig_data['first_level_second_fig_data'], orient='split')
                    general_situation_first_level_second_fig_data = dash.no_update
  
    print("一级第二张图数据：")
    print(first_level_second_fig_data)

    fig_概览一级_关键字缺失 = make_subplots()
    res_数据关键字缺失及汇总 = first_level_second_fig_data.sort_values(['关键字缺失占比'], ascending=False)
    fig_概览一级_关键字缺失.add_trace(
        go.Bar(x=res_数据关键字缺失及汇总['业务类型'], y=res_数据关键字缺失及汇总['关键字缺失占比'], marker_color=px.colors.qualitative.Dark24, )
    )
    fig_概览一级_关键字缺失.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        #title=f"{btime}--{etime}",
    )
    fig_概览一级_关键字缺失.update_yaxes(title_text="关键字缺失占比（%）")
    fig_概览一级_关键字缺失.update_xaxes(title_text="业务指标")

    return fig_概览一级_关键字缺失,general_situation_first_level_second_fig_data

# 下载一级图二明细
@app.callback(
    Output('first_level_second_fig_data_detail', 'data'),
    Input('first_level_second_fig_data_detail_down','n_clicks'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    prevent_initial_call=True,
)
def download_first_level_second_fig_data_detail(n_clicks,db_con_url,count_time):
    if db_con_url is None :
        return dash.no_update
    else:
        if n_clicks is not None and n_clicks>0:
            n_clicks = 0
            db_con_url = json.loads(db_con_url)
            count_time = json.loads(count_time)
            engine = create_engine(db_con_url['db'])
            btime = count_time['btime']
            etime = count_time['etime']
            bus_dic = {
                '用药目的': f"select * from ANTIBIOTICS where (GOAL is null or replace(GOAL,' ','') is null) and BEGINTIME is not null and substr(BEGINTIME,1,10)>='{btime}' and substr(BEGINTIME,1,10)<='{etime}' ",
                '药敏结果': f"select * from drugsusceptibility where  (SUSCEPTIBILITY is null or replace(SUSCEPTIBILITY,' ','') is null) and REQUESTTIME is not null and substr(REQUESTTIME,1,10)>='{btime}' and substr(REQUESTTIME,1,10)<='{etime}' ",
                '手术名称': f"select * from oper2 where (OPER_NAME is null or replace(OPER_NAME,' ','') is null) and BEGINTIME is not null and substr(BEGINTIME,1,10)>='{btime}' and substr(BEGINTIME,1,10)<='{etime}'",
                '手术切口等级': f"select * from oper2 where (WOUND_GRADE is null or replace(WOUND_GRADE,' ','') is null) and BEGINTIME is not null and substr(BEGINTIME,1,10)>='{btime}' and substr(BEGINTIME,1,10)<='{etime}' ",
                '出入院科室': f" select * from overall where (IN_DEPT is null or replace(IN_DEPT,' ','') is null or OUT_DEPT is null or replace(OUT_DEPT,' ','') is null) and in_time is not null and substr(in_time,1,10)>='{btime}' and substr(in_time,1,10)<='{etime}' ",
                '入出转科室': f"select * from department where (DEPT is null or replace(DEPT,' ','') is null) and BEGINTIME is not null and substr(BEGINTIME,1,10)>='{btime}' and substr(BEGINTIME,1,10)<='{etime}'  ",
                }

            output = io.BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            for key in bus_dic.keys():
                try:
                    temp = pd.read_sql(bus_dic[key], con=engine)
                    if temp.shape[0] > 0:
                        temp.to_excel(writer, sheet_name=key)
                except:
                    error_df = pd.DataFrame(['明细数据获取出错'], columns=[key])
                    error_df.to_excel(writer, sheet_name=key)
            writer.save()
            data = output.getvalue()
            hosName = db_con_url['hosname']
            return dcc.send_bytes(data, f'{hosName}各业务关键字缺失数量占比.xlsx')
        else:
            return dash.no_update












# -----------------------------------------------------------------------------------------------------    一级图三   ----------------------------------------------------------------------------------------------------------------------
# 获取概览一级第三张图数据
def get_first_lev_third_fig_date(engine,btime,etime):
    res_数据科室信息缺失及汇总 = pd.DataFrame(columns=['业务类型', '问题数', '总数', '科室信息映射问题占比'])

    bus_dic = {'入院科室': [f" select count(1) as num  from OVERALL t1 where  not exists (select 1 from S_DEPARTMENTS t2 where t1.in_dept = t2.code) and t1.in_dept is not null and (substr(t1.IN_TIME,1,7)>='{btime}' and substr(t1.IN_TIME,1,7)<='{etime}')  ",
                        f"select count(1) as num  from overall where substr(IN_TIME,1,7)>='{btime}' and substr(IN_TIME,1,7)<='{etime}' "],
               '出院科室': [
                   f" select count(1) as num  from OVERALL t1 where  not exists (select 1 from S_DEPARTMENTS t2 where t1.out_dept = t2.code) and t1.out_dept is not null and (substr(t1.IN_TIME,1,7)>='{btime}' and substr(t1.IN_TIME,1,7)<='{etime}') ",
                   f"select count(1) as num  from overall where substr(IN_TIME,1,7)>='{btime}' and substr(IN_TIME,1,7)<='{etime}' "],
               '入出转科室': [
                   f" select count(1) as num  from department t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}') ",
                   f"select count(1) as num  from department where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' "],
               '抗菌药物医嘱科室': [
                   f" select count(1) as num  from ANTIBIOTICS t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}') ",
                   f"select count(1) as num  from ANTIBIOTICS where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' "],
               '手术科室': [
                   f" select count(1) as num  from OPER2 t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}') ",
                   f"select count(1) as num  from OPER2 where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' "],
               '菌检出送检科室': [
                   f" select count(1) as num  from BACTERIA t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}') ",
                   f"select count(1) as num  from BACTERIA where substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' "],
               '药敏送检科室': [
                   f" select count(1) as num  from DRUGSUSCEPTIBILITY t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}') ",
                   f"select count(1) as num  from DRUGSUSCEPTIBILITY where substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' "],
               '体温科室': [
                   f" select count(1) as num  from TEMPERATURE t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.RECORDDATE,1,7)>='{btime}' and substr(t1.RECORDDATE,1,7)<='{etime}') ",
                   f"select count(1) as num  from TEMPERATURE where substr(RECORDDATE,1,7)>='{btime}' and substr(RECORDDATE,1,7)<='{etime}' "],
               '治疗科室': [
                   f" select count(1) as num  from TREATMENT1 t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}') ",
                   f"select count(1) as num  from TREATMENT1 where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' "],
               '常规科室': [
                   f" select count(1) as num  from ROUTINE2 t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}') ",
                   f"select count(1) as num  from ROUTINE2 where substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' "],
               }
    for bus in bus_dic:
        try:
            count_时间为空 = pd.read_sql(bus_dic[bus][0], con=engine)['num'][0]
            count_总 = pd.read_sql(bus_dic[bus][1], con=engine)['num'][0]
            res_数据科室信息缺失及汇总.loc[res_数据科室信息缺失及汇总.shape[0]] = [bus, count_时间为空, count_总,round(count_时间为空 / count_总, 4) * 100]
        except:
            res_数据科室信息缺失及汇总.loc[res_数据科室信息缺失及汇总.shape[0]] = [bus, -1, -1, -1]
    return res_数据科室信息缺失及汇总

# 更新一级图三
@app.callback(
    Output('first_level_third_fig','figure'),
    Output('general_situation_first_level_third_fig_data','data'),
    Input('general_situation_first_level_third_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_first_level_third_fig(general_situation_first_level_third_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if general_situation_first_level_third_fig_data is None:
            first_level_third_fig_data = get_first_lev_third_fig_date(engine, btime, etime)
            general_situation_first_level_third_fig_data={}
            general_situation_first_level_third_fig_data['first_level_third_fig_data'] = first_level_third_fig_data.to_json(orient='split', date_format='iso')
            general_situation_first_level_third_fig_data['hosname'] = db_con_url['hosname']
            general_situation_first_level_third_fig_data['btime'] = btime
            general_situation_first_level_third_fig_data['etime'] = etime
            general_situation_first_level_third_fig_data = json.dumps(general_situation_first_level_third_fig_data)
        else:
            general_situation_first_level_third_fig_data = json.loads(general_situation_first_level_third_fig_data)
            if db_con_url['hosname'] != general_situation_first_level_third_fig_data['hosname']:
                first_level_third_fig_data = get_first_lev_third_fig_date(engine, btime, etime)
                general_situation_first_level_third_fig_data['first_level_third_fig_data'] = first_level_third_fig_data.to_json(orient='split',date_format='iso')
                general_situation_first_level_third_fig_data['hosname'] = db_con_url['hosname']
                general_situation_first_level_third_fig_data['btime'] = btime
                general_situation_first_level_third_fig_data['etime'] = etime
                general_situation_first_level_third_fig_data = json.dumps(general_situation_first_level_third_fig_data)
            else:
                if general_situation_first_level_third_fig_data['btime'] != btime or general_situation_first_level_third_fig_data['etime'] != etime:
                    first_level_third_fig_data = get_first_lev_third_fig_date(engine, btime, etime)
                    general_situation_first_level_third_fig_data['first_level_third_fig_data'] = first_level_third_fig_data.to_json(orient='split',date_format='iso')
                    general_situation_first_level_third_fig_data['btime'] = btime
                    general_situation_first_level_third_fig_data['etime'] = etime
                    general_situation_first_level_third_fig_data = json.dumps(general_situation_first_level_third_fig_data)
                else:
                    first_level_third_fig_data = pd.read_json(general_situation_first_level_third_fig_data['first_level_third_fig_data'], orient='split')
                    general_situation_first_level_third_fig_data = dash.no_update

    fig_概览一级_科室映射缺失 = go.Figure()
    res_数据科室信息缺失及汇总 = first_level_third_fig_data.sort_values(['科室信息映射问题占比'], ascending=False)
    fig_概览一级_科室映射缺失.add_trace(
        go.Bar(x=res_数据科室信息缺失及汇总['业务类型'], y=res_数据科室信息缺失及汇总['科室信息映射问题占比'], marker_color=px.colors.qualitative.Dark24  )
    )
    fig_概览一级_科室映射缺失.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
    )
    fig_概览一级_科室映射缺失.update_yaxes(title_text="科室信息映射问题占比（%）")
    fig_概览一级_科室映射缺失.update_xaxes(title_text="业务指标")
    return fig_概览一级_科室映射缺失,general_situation_first_level_third_fig_data

# 下载一级图三明细
@app.callback(
    Output('first_level_third_fig_data_detail', 'data'), 
    Input('first_level_third_fig_data_detail_down','n_clicks'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    prevent_initial_call=True,
)
def download_first_level_third_fig_data_detail(n_clicks,db_con_url,count_time): 
    if db_con_url is None :
        return dash.no_update
    else:
        if n_clicks is not None and n_clicks>0:
            n_clicks = 0
            db_con_url = json.loads(db_con_url)
            count_time = json.loads(count_time)
            engine = create_engine(db_con_url['db'])
            btime = count_time['btime']
            etime = count_time['etime']
            bus_dic = {
                '入院科室': f" select * from OVERALL t1 where  not exists (select 1 from S_DEPARTMENTS t2 where t1.in_dept = t2.code) and t1.in_dept is not null and substr(t1.IN_TIME,1,10)>='{btime}' and  substr(t1.IN_TIME,1,10)<='{etime}' ",
                '出院科室': f" select * from OVERALL t1 where  not exists (select 1 from S_DEPARTMENTS t2 where t1.out_dept = t2.code) and t1.out_dept is not null and substr(t1.IN_TIME,1,10)>='{btime}' and  substr(t1.IN_TIME,1,10)<='{etime}'  ",
                '入出转科室': f" select * from department t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and substr(t1.BEGINTIME,1,10) >='{btime}' and  substr(t1.BEGINTIME,1,10) <='{etime}' ",
                '抗菌药物医嘱科室': f" select * from ANTIBIOTICS t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.BEGINTIME,1,10)>='{btime}' and substr(t1.BEGINTIME,1,10)<='{etime}') ",
                '手术科室': f" select * from OPER2 t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.BEGINTIME,1,10)>='{btime}' and substr(t1.BEGINTIME,1,10)<='{etime}') ",
                '菌检出送检科室': f" select * from BACTERIA t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.REQUESTTIME,1,10)>='{btime}' and substr(t1.REQUESTTIME,1,10)<='{etime}') ",
                '药敏送检科室': f" select * from DRUGSUSCEPTIBILITY t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.REQUESTTIME,1,10)>='{btime}' and substr(t1.REQUESTTIME,1,10)<='{etime}') ",
                '体温科室': " select * from TEMPERATURE t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.RECORDDATE,1,10)>='{btime}' and substr(t1.RECORDDATE,1,10)<='{etime}') ",
                '治疗科室': f" select * from TREATMENT1 t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.BEGINTIME,1,10)>='{btime}' and substr(t1.BEGINTIME,1,10)<='{etime}') ",
                '常规科室': f" select * from ROUTINE2 t1 where t1.dept is not null and not exists (select 1 from s_departments t2 where t1.dept = t2.code) and (substr(t1.REQUESTTIME,1,10)>='{btime}' and substr(t1.REQUESTTIME,1,10)<='{etime}') ",

            }
            output = io.BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            for key in bus_dic.keys():
                try:
                    temp = pd.read_sql(bus_dic[key], con=engine)
                    if temp.shape[0] > 0:
                        temp.to_excel(writer, sheet_name=key)
                except:
                    error_df = pd.DataFrame(['明细数据获取出错'], columns=[key])
                    error_df.to_excel(writer, sheet_name=key)
            writer.save()
            data = output.getvalue()
            hosName = db_con_url['hosname']
            return dcc.send_bytes(data, f'{hosName}科室映射缺失数量占比.xlsx')
        else:
            return dash.no_update









# -----------------------------------------------------------------------------------------------------    二级图一   ----------------------------------------------------------------------------------------------------------------------
# 获取概二级各业务逻辑问题数据
def get_second_level_fig_date(engine,btime,etime):
    res_业务逻辑问题数据汇总 = pd.DataFrame(columns=['问题数据数量', '问题'])
    ques_dic = {
        '出院时间小于等于入院时间' : f""" select count(1) from overall where in_time is not null and  out_time is not null and in_time >= out_time and (substr(in_time,1,7)>='{btime}' and substr(in_time,1,7)<='{etime}')""",
        '存在测试患者数据' : f""" select count(1) from overall where (pname like '%测试%' or  pname like '%test%') and (substr(in_time,1,7)>='{btime}' and substr(in_time,1,7)<='{etime}') """,
        '存在住院时长超四个月患者' : f""" select  count(1) from overall where (((out_time is null or out_time='9999') and ( trunc(sysdate)-to_date(substr(in_time,0,10),'yyyy-mm-dd') )> 120)
                                    or (out_time is not null and ( to_date(substr(out_time,0,10),'yyyy-mm-dd')-to_date(substr(in_time,0,10),'yyyy-mm-dd') )> 120)) and (substr(in_time,1,7)>='{btime}' and substr(in_time,1,7)<='{etime}') 
                                """,
        '存在住院天数不足一天患者' : f""" select count(1) from overall where  (out_time is not null and out_time <> '9999' and ( to_date(substr(out_time,0,10),'yyyy-mm-dd')-to_date(substr(in_time,0,10),'yyyy-mm-dd') )< 1 ) and (substr(in_time,1,7)>='{btime}' and substr(in_time,1,7)<='{etime}')  """,
        '转科时间在出入院时间之外' : f""" select count(1)  from department t1,overall t2 where 
                                    ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                    and t1.caseid = t2.caseid 
                                    and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME ) 
                                    and (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}')
                                 """,
        '转入时间大于等于转出时间' : f""" select count(1) from department  where  BEGINTIME is not null and ENDTIME is not null  and  BEGINTIME >= ENDTIME and (substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}') """,

        '治疗开始时间大于等于结束时间' : f""" select count(1)  from TREATMENT1 where BEGINTIME is not null and ENDTIME is not null and  BEGINTIME>= ENDTIME and (substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}') """,
        '治疗时间在出入院时间之外' : f""" select count(1) from TREATMENT1 t1,overall t2 where 
                                    ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                    and t1.caseid = t2.caseid 
                                    and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME ) 
                                    and (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}')
                                """,
        '医嘱开始时间大于结束时间' : f""" select count(1) from ANTIBIOTICS where BEGINTIME is not null and ENDTIME is not null and  BEGINTIME> ENDTIME and (substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}')""",
        '医嘱时间在出入院时间之外' : f""" select count(1) from ANTIBIOTICS t1,overall t2 where 
                                    ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                    and t1.caseid = t2.caseid 
                                    and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME ) 
                                    and (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}')
                                 """,
        '送检时间大于等于报告时间' : f""" select count(1) from BACTERIA where REQUESTTIME is not null and REPORTTIME is not null and  REQUESTTIME>= REPORTTIME and (substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}')""",
        '送检时间在出入院时间之外' : f""" select count(1) from BACTERIA t1,overall t2 where 
                                    ( t1.REQUESTTIME is not null   and t2.in_time is not null and t2.out_time is not null) 
                                    and t1.caseid = t2.caseid 
                                    and (t1.REQUESTTIME<t2.IN_TIME or t1.REQUESTTIME > t2.OUT_TIME   ) 
                                    and (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}')
                                """,
        '药敏送检时间大于等于报告时间' : f""" select count(1) from DRUGSUSCEPTIBILITY where REQUESTTIME is not null and REPORTTIME is not null and  REQUESTTIME>= REPORTTIME and ( substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' )""",
        '药敏送检时间在出入院时间之外' : f""" select count(1) from DRUGSUSCEPTIBILITY t1,overall t2 where 
                                        ( t1.REQUESTTIME is not null   and t2.in_time is not null and t2.out_time is not null) 
                                        and t1.caseid = t2.caseid 
                                        and (t1.REQUESTTIME<t2.IN_TIME or t1.REQUESTTIME > t2.OUT_TIME   ) 
                                        and (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}')
                                     """,
        '手术开始时间大于结束时间' : f""" select count(1) from OPER2 where BEGINTIME is not null and ENDTIME is not null and  BEGINTIME> ENDTIME and ( substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' )""",
        '手术时间在出入院时间之外' : f""" select count(1) from OPER2 t1,overall t2 where 
                                    ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                    and t1.caseid = t2.caseid 
                                    and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME ) 
                                    and (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}')
                                 """,
        'OPERID重复' : f""" select count(1) from oper2 where operid in (select operid from oper2 group by operid having count(operid)>1) and ( substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' ) order by operid """,
        '体温值异常' : f""" select count(1) from TEMPERATURE where (VALUE > 46 or VALUE < 34 or VALUE is null) and ( substr(RECORDDATE,1,7) >='{btime}' and substr(RECORDDATE,1,7) <='{etime}')  """,
        '体温测量时间在出入院时间之外' : f""" select count(1) from TEMPERATURE t1,overall t2 where 
                                        ( t1.RECORDDATE is not null  and t2.in_time is not null and t2.out_time is not null) 
                                        and t1.caseid = t2.caseid 
                                        and (t1.RECORDDATE<t2.IN_TIME or t1.RECORDDATE > t2.OUT_TIME   ) 
                                        and ( substr(t1.RECORDDATE,1,7)>='{btime}' and substr(t1.RECORDDATE,1,7)<='{etime}')
                                     """,

        '入出转入科时间重复': f""" select count(1) from department t1,
                                        (select caseid ,begintime from department where substr(begintime,1,7)>='{btime}' and substr(begintime,1,7)<='{etime}' group by caseid ,begintime having count(1)>1) t2
                                        where t1.caseid=t2.caseid and t1.begintime = t2.begintime
                                         """,
    }

    for ques in ques_dic:
        try:
            ques_df = pd.read_sql(ques_dic[ques], con=engine)
            ques_df.columns = ['问题数据数量']
            ques_df['问题'] = ques
            res_业务逻辑问题数据汇总 = res_业务逻辑问题数据汇总.append( ques_df )
        except:
            res_业务逻辑问题数据汇总.loc[res_业务逻辑问题数据汇总.shape[0]] = [ -1 , ques ]
        print('二级图  ' , ques)
    return res_业务逻辑问题数据汇总
# def get_second_level_fig_date(engine,btime,etime):
#     res_业务逻辑问题数据汇总 = pd.DataFrame(columns=['问题数据数量', '问题','month'])
#     ques_dic = {
#         '出院时间小于等于入院时间' : f""" select count(1) as 问题数据数量, '出院时间小于等于入院时间' as 问题, substr(in_time,1,7) as month from overall where in_time is not null and  out_time is not null and in_time >= out_time group by substr(in_time,1,7) """,
#         '存在测试患者数据' : f""" select count(1) as 问题数据数量, '存在测试患者数据' as 问题, substr(in_time,1,7) as month from overall where (pname like '%测试%' or  pname like '%test%') group by substr(in_time,1,7)   """,
#         '存在住院时长超四个月患者' : f""" select  count(1) as 问题数据数量, '存在住院时长超四个月患者' as 问题, substr(in_time,1,7) as month from overall where
#                                         (((out_time is null or out_time='9999') and ( trunc(sysdate)-to_date(substr(in_time,0,10),'yyyy-mm-dd') )> 120)
#                                     or (out_time is not null and ( to_date(substr(out_time,0,10),'yyyy-mm-dd')-to_date(substr(in_time,0,10),'yyyy-mm-dd') )> 120))
#                                      group by substr(in_time,1,7) )
#                                 """,
#         '存在住院天数不足一天患者' : f""" select count(1) as 问题数据数量, '存在住院天数不足一天患者' as 问题, substr(in_time,1,7) as month from overall where
#                                     (out_time is not null and out_time <> '9999' and ( to_date(substr(out_time,0,10),'yyyy-mm-dd')-to_date(substr(in_time,0,10),'yyyy-mm-dd') )< 1 )
#                                      group by substr(in_time,1,7)  """,
#         '转科时间在出入院时间之外' : f""" select count(1) as 问题数据数量, '转科时间在出入院时间之外' as 问题, substr(t1.BEGINTIME,1,7) as month from department t1,overall t2 where
#                                     ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null)
#                                     and t1.caseid = t2.caseid
#                                     and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME )
#                                       group by substr(t1.BEGINTIME,1,7)
#                                  """,
#         '转入时间大于等于转出时间' : f""" select count(1) as 问题数据数量, '转入时间大于等于转出时间' as 问题, substr(t1.BEGINTIME,1,7) as month from department  where
#                                         BEGINTIME is not null and ENDTIME is not null  and  BEGINTIME >= ENDTIME
#                                         group by substr( BEGINTIME,1,7)
#                                 """,
#
#         '治疗开始时间大于等于结束时间' : f""" select count(1) as 问题数据数量, '治疗开始时间大于等于结束时间' as 问题, substr(BEGINTIME,1,7) as month from TREATMENT1 where
#                                         BEGINTIME is not null and ENDTIME is not null and  BEGINTIME>= ENDTIME
#                                         group by substr(BEGINTIME,1,7)
#                                     """,
#         '治疗时间在出入院时间之外' : f""" select count(1) as 问题数据数量, '治疗时间在出入院时间之外' as 问题, substr(t1.BEGINTIME,1,7) as month from TREATMENT1 t1,overall t2 where
#                                     ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null)
#                                     and t1.caseid = t2.caseid
#                                     and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME )
#                                     group by substr(t1.BEGINTIME,1,7)
#                                 """,
#         '医嘱开始时间大于结束时间' : f""" select count(1) as 问题数据数量, '医嘱开始时间大于结束时间' as 问题, substr(BEGINTIME,1,7) as month from ANTIBIOTICS where
#                                     BEGINTIME is not null and ENDTIME is not null and  BEGINTIME> ENDTIME
#                                     group by substr( BEGINTIME,1,7)
#                                 """,
#         '医嘱时间在出入院时间之外' : f""" select count(1) as 问题数据数量, '医嘱时间在出入院时间之外' as 问题, substr(t1.BEGINTIME,1,7) as month from ANTIBIOTICS t1,overall t2 where
#                                     ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null)
#                                     and t1.caseid = t2.caseid
#                                     and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME )
#                                     group by substr(t1.BEGINTIME,1,7)
#                                  """,
#         '送检时间大于等于报告时间' : f""" select count(1) as 问题数据数量, '送检时间大于等于报告时间' as 问题, substr(REQUESTTIME,1,7) as month from BACTERIA where
#                                     REQUESTTIME is not null and REPORTTIME is not null and  REQUESTTIME>= REPORTTIME
#                                     group by substr( REQUESTTIME,1,7)
#                                    """,
#         '送检时间在出入院时间之外' : f""" select count(1) as 问题数据数量, '送检时间在出入院时间之外' as 问题, substr(t1.REQUESTTIME,1,7) as month from BACTERIA t1,overall t2 where
#                                     ( t1.REQUESTTIME is not null   and t2.in_time is not null and t2.out_time is not null)
#                                     and t1.caseid = t2.caseid
#                                     and (t1.REQUESTTIME<t2.IN_TIME or t1.REQUESTTIME > t2.OUT_TIME   )
#                                     group by substr(t1.REQUESTTIME,1,7)
#                                 """,
#         '药敏送检时间大于等于报告时间' : f""" select count(1) as 问题数据数量, '药敏送检时间大于等于报告时间' as 问题, substr(REQUESTTIME,1,7) as month from DRUGSUSCEPTIBILITY where
#                                          REQUESTTIME is not null and REPORTTIME is not null and  REQUESTTIME>= REPORTTIME
#                                          group by substr( REQUESTTIME,1,7)
#                                     """,
#         '药敏送检时间在出入院时间之外' : f""" select count(1) as 问题数据数量, '药敏送检时间在出入院时间之外' as 问题, substr( t1.REQUESTTIME,1,7) as month from DRUGSUSCEPTIBILITY t1,overall t2 where
#                                         ( t1.REQUESTTIME is not null   and t2.in_time is not null and t2.out_time is not null)
#                                         and t1.caseid = t2.caseid
#                                         and (t1.REQUESTTIME<t2.IN_TIME or t1.REQUESTTIME > t2.OUT_TIME   )
#                                         group by substr(t1.REQUESTTIME,1,7)
#                                      """,
#         '手术开始时间大于结束时间' : f""" select count(1) as 问题数据数量, '手术开始时间大于结束时间' as 问题, substr(BEGINTIME,1,7) as month from OPER2 where
#                                       BEGINTIME is not null and ENDTIME is not null and  BEGINTIME> ENDTIME
#                                       group by substr( BEGINTIME,1,7)
#                                  """,
#         '手术时间在出入院时间之外' : f""" select count(1) as 问题数据数量, '手术时间在出入院时间之外' as 问题, substr( t1.BEGINTIME,1,7) as month from OPER2 t1,overall t2 where
#                                     ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null)
#                                     and t1.caseid = t2.caseid
#                                     and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME )
#                                     group by substr(t1.BEGINTIME,1,7)
#                                  """,
#         'OPERID重复' : f""" select count(1) as 问题数据数量, 'OPERID重复' as 问题, substr(BEGINTIME,1,7) as month from oper2 where
#                             operid in (select operid from oper2 group by operid having count(operid)>1)
#                             group by substr( BEGINTIME,1,7)
#                         """,
#         '体温值异常' : f""" select count(1) as 问题数据数量, '体温值异常' as 问题, substr(RECORDDATE,1,7) as month from TEMPERATURE where
#                           (VALUE > 46 or VALUE < 34 or VALUE is null) group by substr( RECORDDATE,1,7)   """,
#         '体温测量时间在出入院时间之外' : f""" select count(1) as 问题数据数量, '体温测量时间在出入院时间之外' as 问题, substr(t1.RECORDDATE,1,7) as month from TEMPERATURE t1,overall t2 where
#                                         ( t1.RECORDDATE is not null  and t2.in_time is not null and t2.out_time is not null)
#                                         and t1.caseid = t2.caseid
#                                         and (t1.RECORDDATE<t2.IN_TIME or t1.RECORDDATE > t2.OUT_TIME   )
#                                         group by substr( t1.RECORDDATE,1,7)
#                                      """,
#     }
#
#     for ques in ques_dic:
#         try:
#             # ques_df = pd.read_sql(ques_dic[ques], con=engine)
#             # ques_df.columns = ['问题数据数量']
#             # ques_df['问题'] = ques
#             # res_业务逻辑问题数据汇总 = res_业务逻辑问题数据汇总.append( ques_df )
#             res_业务逻辑问题数据汇总 = res_业务逻辑问题数据汇总.append(pd.read_sql(ques_dic[ques], con=engine) )
#         except:
#             res_业务逻辑问题数据汇总.loc[res_业务逻辑问题数据汇总.shape[0]] = [ -1 , ques ,]
#         print('二级图  ' , ques)
#     return res_业务逻辑问题数据汇总

# 获取概二级各业务逻辑问题明细数据数据

# 更新二级图
@app.callback(
    Output('second_level_fig','figure'),
    Output('general_situation_secod_level_fig_data','data'),
    Input('general_situation_secod_level_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_second_level_fig(general_situation_secod_level_fig_data,db_con_url,count_time): 
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if general_situation_secod_level_fig_data is None:
            general_situation_secod_level_fig_data = {}
            second_level_fig_date = get_second_level_fig_date(engine, btime, etime)
            general_situation_secod_level_fig_data['second_level_fig_date'] = second_level_fig_date.to_json(orient='split', date_format='iso')
            general_situation_secod_level_fig_data['hosname'] = db_con_url['hosname']
            general_situation_secod_level_fig_data['btime'] = btime
            general_situation_secod_level_fig_data['etime'] = etime
            general_situation_secod_level_fig_data = json.dumps(general_situation_secod_level_fig_data)
        else:
            general_situation_secod_level_fig_data = json.loads(general_situation_secod_level_fig_data)
            if db_con_url['hosname'] != general_situation_secod_level_fig_data['hosname']:
                second_level_fig_date = get_second_level_fig_date(engine, btime, etime)
                general_situation_secod_level_fig_data['second_level_fig_date'] = second_level_fig_date.to_json(orient='split',date_format='iso')
                general_situation_secod_level_fig_data['hosname'] = db_con_url['hosname']
                general_situation_secod_level_fig_data['btime'] = btime
                general_situation_secod_level_fig_data['etime'] = etime
                general_situation_secod_level_fig_data = json.dumps(general_situation_secod_level_fig_data)
            else:
                if general_situation_secod_level_fig_data['btime'] != btime or general_situation_secod_level_fig_data['etime'] != etime:
                    second_level_fig_date = get_second_level_fig_date(engine, btime, etime)
                    general_situation_secod_level_fig_data['second_level_fig_date'] = second_level_fig_date.to_json(orient='split',date_format='iso')
                    general_situation_secod_level_fig_data['btime'] = btime
                    general_situation_secod_level_fig_data['etime'] = etime
                    general_situation_secod_level_fig_data = json.dumps(general_situation_secod_level_fig_data)
                else:
                    second_level_fig_date = pd.read_json(general_situation_secod_level_fig_data['second_level_fig_date'], orient='split')
                    general_situation_secod_level_fig_data = dash.no_update
    
    print('二级图数据:')
    print(second_level_fig_date)
    fig_概览二级 = second_level_fig_date
    fig_概览二级_业务逻辑问题 = make_subplots()
    fig_概览二级 = fig_概览二级.sort_values(['问题数据数量'],ascending=False)
    fig_概览二级_业务逻辑问题.add_trace(
        go.Bar(x=fig_概览二级['问题'], y=fig_概览二级['问题数据数量'], marker_color=px.colors.qualitative.Dark24, )
    )
    fig_概览二级_业务逻辑问题.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        #title=f"{btime}--{etime}",
    )
    fig_概览二级_业务逻辑问题.update_yaxes(title_text="问题数据数量", )
    fig_概览二级_业务逻辑问题.update_xaxes(title_text="业务问题", )
    return fig_概览二级_业务逻辑问题,general_situation_secod_level_fig_data

# 下载二级图明细
@app.callback(
    Output('second_level_fig_date_detail','data'), 
    Input('second_level_fig_data_detail_down', 'n_clicks'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    prevent_initial_call=True,
)
def download_second_level_fig(n_clicks,db_con_url,count_time):
    if db_con_url is None :
        return dash.no_update
    else:
        if n_clicks is not None and n_clicks>0:
            n_clicks = 0
            db_con_url = json.loads(db_con_url)
            count_time = json.loads(count_time)
            engine = create_engine(db_con_url['db'])
            btime = count_time['btime']
            etime = count_time['etime']
            ques_dic = {
                '出院时间小于等于入院时间': f""" select * from overall where in_time is not null and  out_time is not null and in_time >= out_time and (substr(in_time,1,10)>='{btime}' and substr(in_time,1,10)<='{etime}')""",
                '存在测试患者数据': f""" select * from overall where (pname like '%测试%' or  pname like '%test%') and (substr(in_time,1,10)>='{btime}' and substr(in_time,1,10)<='{etime}') """,
                '存在住院时长超四个月患者': f""" select  * from overall where (((out_time is null or out_time='9999') and ( trunc(sysdate)-to_date(substr(in_time,0,10),'yyyy-mm-dd') )> 120)
                                               or (out_time is not null and ( to_date(substr(out_time,0,10),'yyyy-mm-dd')-to_date(substr(in_time,0,10),'yyyy-mm-dd') )> 120)) and (substr(in_time,1,10)>='{btime}' and substr(in_time,1,10)<='{etime}') 
                                           """,
                '存在住院天数不足一天患者': f""" select * from overall where  (out_time is not null and out_time <> '9999' and ( to_date(substr(out_time,0,10),'yyyy-mm-dd')-to_date(substr(in_time,0,10),'yyyy-mm-dd') )< 1 ) and (substr(in_time,1,10)>='{btime}' and substr(in_time,1,10)<='{etime}')  """,
                '转科时间在出入院时间之外': f""" select t1.*,t2.in_time as 入院时间,t2.out_time as 出院时间  from department t1,overall t2 where 
                                               ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                               and t1.caseid = t2.caseid 
                                               and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME ) 
                                               and (substr(t1.BEGINTIME,1,10)>='{btime}' and substr(t1.BEGINTIME,1,10)<='{etime}')
                                            """,
                '转入时间大于等于转出时间': f""" select * from department  where  BEGINTIME is not null and ENDTIME is not null  and  BEGINTIME >= ENDTIME and (substr(BEGINTIME,1,10)>='{btime}' and substr(BEGINTIME,1,10)<='{etime}') """,

                '治疗开始时间大于等于结束时间': f""" select *  from TREATMENT1 where BEGINTIME is not null and ENDTIME is not null and  BEGINTIME>= ENDTIME and (substr(BEGINTIME,1,10)>='{btime}' and substr(BEGINTIME,1,10)<='{etime}') """,
                '治疗时间在出入院时间之外': f""" select t1.*,t2.in_time as 入院时间,t2.out_time as 出院时间 from TREATMENT1 t1,overall t2 where 
                                               ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                               and t1.caseid = t2.caseid 
                                               and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME ) 
                                               and (substr(t1.BEGINTIME,1,10)>='{btime}' and substr(t1.BEGINTIME,1,10)<='{etime}')
                                           """,
                '医嘱开始时间大于结束时间': f""" select * from ANTIBIOTICS where BEGINTIME is not null and ENDTIME is not null and  BEGINTIME> ENDTIME and (substr(BEGINTIME,1,10)>='{btime}' and substr(BEGINTIME,1,10)<='{etime}')""",
                '医嘱时间在出入院时间之外': f""" select t1.*,t2.in_time as 入院时间,t2.out_time as 出院时间 from ANTIBIOTICS t1,overall t2 where 
                                               ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                               and t1.caseid = t2.caseid 
                                               and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME ) 
                                               and (substr(t1.BEGINTIME,1,10)>='{btime}' and substr(t1.BEGINTIME,1,10)<='{etime}')
                                            """,
                '送检时间大于等于报告时间': f""" select * from BACTERIA where REQUESTTIME is not null and REPORTTIME is not null and  REQUESTTIME>= REPORTTIME and (substr(REQUESTTIME,1,10)>='{btime}' and substr(REQUESTTIME,1,10)<='{etime}')""",
                '送检时间在出入院时间之外': f""" select t1.*,t2.in_time as 入院时间,t2.out_time as 出院时间 from BACTERIA t1,overall t2 where 
                                               ( t1.REQUESTTIME is not null   and t2.in_time is not null and t2.out_time is not null) 
                                               and t1.caseid = t2.caseid 
                                               and (t1.REQUESTTIME<t2.IN_TIME or t1.REQUESTTIME > t2.OUT_TIME   ) 
                                               and (substr(t1.REQUESTTIME,1,10)>='{btime}' and substr(t1.REQUESTTIME,1,10)<='{etime}')
                                           """,
                '药敏送检时间大于等于报告时间': f""" select * from DRUGSUSCEPTIBILITY where REQUESTTIME is not null and REPORTTIME is not null and  REQUESTTIME>= REPORTTIME and ( substr(REQUESTTIME,1,10)>='{btime}' and substr(REQUESTTIME,1,10)<='{etime}' )""",
                '药敏送检时间在出入院时间之外': f""" select t1.*,t2.in_time as 入院时间,t2.out_time as 出院时间 from DRUGSUSCEPTIBILITY t1,overall t2 where 
                                                   ( t1.REQUESTTIME is not null   and t2.in_time is not null and t2.out_time is not null) 
                                                   and t1.caseid = t2.caseid 
                                                   and (t1.REQUESTTIME<t2.IN_TIME or t1.REQUESTTIME > t2.OUT_TIME   ) 
                                                   and (substr(t1.REQUESTTIME,1,10)>='{btime}' and substr(t1.REQUESTTIME,1,10)<='{etime}')
                                                """,
                '手术开始时间大于结束时间': f""" select * from OPER2 where BEGINTIME is not null and ENDTIME is not null and  BEGINTIME> ENDTIME and ( substr(BEGINTIME,1,10)>='{btime}' and substr(BEGINTIME,1,10)<='{etime}' )""",
                '手术时间在出入院时间之外': f""" select t1.*,t2.in_time as 入院时间,t2.out_time as 出院时间 from OPER2 t1,overall t2 where 
                                               ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                               and t1.caseid = t2.caseid 
                                               and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME ) 
                                               and (substr(t1.BEGINTIME,1,10)>='{btime}' and substr(t1.BEGINTIME,1,10)<='{etime}')
                                            """,
                'OPERID重复': f""" select * from oper2 where operid in (select operid from oper2 group by operid having count(operid)>1) and ( substr(BEGINTIME,1,10)>='{btime}' and substr(BEGINTIME,1,10)<='{etime}' ) order by operid """,
                '体温值异常': f""" select * from TEMPERATURE where (VALUE > 46 or VALUE < 34 or VALUE is null) and ( substr(RECORDDATE,1,10) >='{btime}' and substr(RECORDDATE,1,10) <='{etime}')  """,
                '体温测量时间在出入院时间之外': f""" select t1.*,t2.in_time as 入院时间,t2.out_time as 出院时间 from TEMPERATURE t1,overall t2 where 
                                                   ( t1.RECORDDATE is not null  and t2.in_time is not null and t2.out_time is not null) 
                                                   and t1.caseid = t2.caseid 
                                                   and (t1.RECORDDATE<t2.IN_TIME or t1.RECORDDATE > t2.OUT_TIME   ) 
                                                   and ( substr(t1.RECORDDATE,1,10)>='{btime}' and substr(t1.RECORDDATE,1,10)<='{etime}')
                                                """,
                '体温测量时间在出入院时间之外': f""" select t1.* from department t1,
                                                    (select caseid ,begintime from department where substr(begintime,1,10)>='{btime}' and substr(begintime,1,10)<='{etime}' group by caseid ,begintime having count(1)>1) t2
                                                    where t1.caseid=t2.caseid and t1.begintime = t2.begintime
                                                     """,
                '入出转入科时间重复': f""" select t1.* from department t1,
                                                        (select caseid ,begintime from department where substr(begintime,1,7)>='{btime}' and substr(begintime,1,7)<='{etime}' group by caseid ,begintime having count(1)>1) t2
                                                        where t1.caseid=t2.caseid and t1.begintime = t2.begintime
                                                         """,
            }
            output = io.BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            for key in ques_dic.keys():
                try:
                    temp = pd.read_sql(ques_dic[key], con=engine)
                    if temp.shape[0] > 0:
                        temp.to_excel(writer, sheet_name=key)
                except:
                    error_df = pd.DataFrame(['明细数据获取出错'], columns=[key])
                    error_df.to_excel(writer, sheet_name=key)
            writer.save()
            data = output.getvalue()
            hosName = db_con_url['hosname']
            return dcc.send_bytes(data, f'{hosName}全院数据逻辑问题明细.xlsx')
        else:
            return dash.no_update
    




# -----------------------------------------------------------------------------------------------------    三级图一   ----------------------------------------------------------------------------------------------------------------------
# 获取概览三级第一张图数据
def get_third_level_first_fig_date(engine):
    res_全业务 = pd.DataFrame(columns=['num', 'month', '业务类型'])

    bus_dic = {
        '入院人数':"select count(distinct caseid) as num ,substr(in_time,1,7) as month,'入院人数' as 业务类型 from overall where in_time is not null group by substr(in_time,1,7) having substr(in_time,1,7) <= to_char(sysdate,'yyyy-mm') and substr(in_time,1,7) >= '1990-01' order by substr(in_time,1,7)",
        '出院人数':"select count(distinct caseid) as num ,substr(out_time,1,7) as month,'出院人数' as 业务类型 from overall where in_time is not null and out_time is not null group by substr(out_time,1,7) having substr(out_time,1,7) <= to_char(sysdate,'yyyy-mm') and substr(out_time,1,7) >= '1990-01' order by substr(out_time,1,7)",
        '抗菌药物医嘱数':"select count( distinct CASEID||ORDERNO||ANAME ) as num ,substr(BEGINTIME,1,7) as month ,'抗菌药物医嘱数' as 业务类型 from antibiotics where BEGINTIME is not null group by substr(BEGINTIME,1,7)  having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' order by substr(BEGINTIME,1,7)",
        '手术台数':"select count( distinct CASEID||OPERID ) as num ,substr(BEGINTIME,1,7) as month,'手术台数' as 业务类型 from oper2 where BEGINTIME is not null group by substr(BEGINTIME,1,7) having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' order by substr(BEGINTIME,1,7)",
        '菌检出结果记录数':"select count( distinct CASEID||TESTNO||BACTERIA ) as num ,substr(REQUESTTIME,1,7) as month ,'菌检出结果记录数' as 业务类型 from bacteria where REQUESTTIME is not null group by substr(REQUESTTIME,1,7) having substr(REQUESTTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(REQUESTTIME,1,7) >= '1990-01' order by substr(REQUESTTIME,1,7)",
        '药敏结果记录数':"select count( distinct CASEID||TESTNO||BACTERIA||ANTIBIOTICS ) as num ,substr(REQUESTTIME,1,7) as month ,'药敏结果记录数' as 业务类型  from DRUGSUSCEPTIBILITY where REQUESTTIME is not null group by substr(REQUESTTIME,1,7) having substr(REQUESTTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(REQUESTTIME,1,7) >= '1990-01' order by substr(REQUESTTIME,1,7)",
        '体温测量数':"select count( distinct CASEID||RECORDDATE ) as num ,substr(RECORDDATE,1,7) as month  ,'体温测量数' as 业务类型 from TEMPERATURE where RECORDDATE is not null group by substr(RECORDDATE,1,7) having substr(RECORDDATE,1,7) <= to_char(sysdate,'yyyy-mm') and substr(RECORDDATE,1,7) >= '1990-01' order by substr(RECORDDATE,1,7)",
        '入出转记录数':"select count( distinct CASEID||BEGINTIME||DEPT ) as num ,substr(BEGINTIME,1,7) as month ,'入出转记录数' as 业务类型  from department where BEGINTIME is not null group by substr(BEGINTIME,1,7) having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' order by substr(BEGINTIME,1,7)",
        '常规结果记录数':"select count( distinct CASEID||TESTNO||RINDEX ) as num ,substr(REQUESTTIME,1,7) as month ,'常规结果记录数' as 业务类型  from ROUTINE2 where REQUESTTIME is not null group by substr(REQUESTTIME,1,7) having substr(REQUESTTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(REQUESTTIME,1,7) >= '1990-01' order by substr(REQUESTTIME,1,7)",
        '影像检查记录数':"select count( distinct CASEID||EXAM_NO ) as num ,substr(EXAM_DATE,1,7) as month  ,'影像检查记录数' as 业务类型 from EXAM where EXAM_DATE is not null group by substr(EXAM_DATE,1,7) having substr(EXAM_DATE,1,7) <= to_char(sysdate,'yyyy-mm') and substr(EXAM_DATE,1,7) >= '1990-01' order by substr(EXAM_DATE,1,7)",
        '治疗记录数':"select count( distinct CASEID||TNO||TTYPE||DEPT ) as num ,substr(BEGINTIME,1,7) as month  ,'治疗记录数' as 业务类型 from TREATMENT1 where BEGINTIME is not null group by substr(BEGINTIME,1,7) having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' order by substr(BEGINTIME,1,7)",
        '中心静脉插管记录数':"select count(1) as num ,substr(BEGINTIME,1,7) as month,'中心静脉插管记录数' as 业务类型 from treatment1 where TTYPE like '%中心%静脉%' and BEGINTIME is not null group by substr(BEGINTIME,1,7) having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' ",
        '呼吸机记录数':"select count(1) as num ,substr(BEGINTIME,1,7) as month,'呼吸机记录数' as 业务类型 from treatment1 where TTYPE like '%呼吸机%' and BEGINTIME is not null group by substr(BEGINTIME,1,7) having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' ",
        '泌尿道插管记录数':"select count(1) as num ,substr(BEGINTIME,1,7) as month,'泌尿道插管记录数' as 业务类型 from treatment1 where TTYPE like '%泌尿道%' and BEGINTIME is not null group by substr(BEGINTIME,1,7) having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' ",
    }

    for bus in bus_dic:
        res_全业务 = res_全业务.append(pd.read_sql(bus_dic[bus],con=engine))
    return res_全业务

# 获取概览三级第一张图数据
# def get_third_level_first_fig_date(engine,date_type):
#     res_全业务 = pd.DataFrame(columns=['num', 'month', '业务类型'])
#     if date_type == 'month':
#         bus_dic = {
#             '入院人数': "select count(distinct caseid) as num ,substr(in_time,1,7) as month,'入院人数' as 业务类型 from overall where in_time is not null group by substr(in_time,1,7) having substr(in_time,1,7) <= to_char(sysdate,'yyyy-mm') and substr(in_time,1,7) >= '1990-01' order by substr(in_time,1,7)",
#             '出院人数': "select count(distinct caseid) as num ,substr(out_time,1,7) as month,'出院人数' as 业务类型 from overall where in_time is not null and out_time is not null group by substr(out_time,1,7) having substr(out_time,1,7) <= to_char(sysdate,'yyyy-mm') and substr(out_time,1,7) >= '1990-01' order by substr(out_time,1,7)",
#             '抗菌药物医嘱数': "select count( distinct CASEID||ORDERNO||ANAME ) as num ,substr(BEGINTIME,1,7) as month ,'抗菌药物医嘱数' as 业务类型 from antibiotics where BEGINTIME is not null group by substr(BEGINTIME,1,7)  having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' order by substr(BEGINTIME,1,7)",
#             '手术台数': "select count( distinct CASEID||OPERID ) as num ,substr(BEGINTIME,1,7) as month,'手术台数' as 业务类型 from oper2 where BEGINTIME is not null group by substr(BEGINTIME,1,7) having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' order by substr(BEGINTIME,1,7)",
#             '菌检出结果记录数': "select count( distinct CASEID||TESTNO||BACTERIA ) as num ,substr(REQUESTTIME,1,7) as month ,'菌检出结果记录数' as 业务类型 from bacteria where REQUESTTIME is not null group by substr(REQUESTTIME,1,7) having substr(REQUESTTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(REQUESTTIME,1,7) >= '1990-01' order by substr(REQUESTTIME,1,7)",
#             '药敏结果记录数': "select count( distinct CASEID||TESTNO||BACTERIA||ANTIBIOTICS ) as num ,substr(REQUESTTIME,1,7) as month ,'药敏结果记录数' as 业务类型  from DRUGSUSCEPTIBILITY where REQUESTTIME is not null group by substr(REQUESTTIME,1,7) having substr(REQUESTTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(REQUESTTIME,1,7) >= '1990-01' order by substr(REQUESTTIME,1,7)",
#             '体温测量数': "select count( distinct CASEID||RECORDDATE ) as num ,substr(RECORDDATE,1,7) as month  ,'体温测量数' as 业务类型 from TEMPERATURE where RECORDDATE is not null group by substr(RECORDDATE,1,7) having substr(RECORDDATE,1,7) <= to_char(sysdate,'yyyy-mm') and substr(RECORDDATE,1,7) >= '1990-01' order by substr(RECORDDATE,1,7)",
#             '入出转记录数': "select count( distinct CASEID||BEGINTIME||DEPT ) as num ,substr(BEGINTIME,1,7) as month ,'入出转记录数' as 业务类型  from department where BEGINTIME is not null group by substr(BEGINTIME,1,7) having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' order by substr(BEGINTIME,1,7)",
#             '常规结果记录数': "select count( distinct CASEID||TESTNO||RINDEX ) as num ,substr(REQUESTTIME,1,7) as month ,'常规结果记录数' as 业务类型  from ROUTINE2 where REQUESTTIME is not null group by substr(REQUESTTIME,1,7) having substr(REQUESTTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(REQUESTTIME,1,7) >= '1990-01' order by substr(REQUESTTIME,1,7)",
#             '影像检查记录数': "select count( distinct CASEID||EXAM_NO ) as num ,substr(EXAM_DATE,1,7) as month  ,'影像检查记录数' as 业务类型 from EXAM where EXAM_DATE is not null group by substr(EXAM_DATE,1,7) having substr(EXAM_DATE,1,7) <= to_char(sysdate,'yyyy-mm') and substr(EXAM_DATE,1,7) >= '1990-01' order by substr(EXAM_DATE,1,7)",
#             '治疗记录数': "select count( distinct CASEID||TNO||TTYPE||DEPT ) as num ,substr(BEGINTIME,1,7) as month  ,'治疗记录数' as 业务类型 from TREATMENT1 where BEGINTIME is not null group by substr(BEGINTIME,1,7) having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' order by substr(BEGINTIME,1,7)",
#             '中心静脉插管记录数': "select count(1) as num ,substr(BEGINTIME,1,7) as month,'中心静脉插管记录数' as 业务类型 from treatment1 where TTYPE like '%中心%静脉%' and BEGINTIME is not null group by substr(BEGINTIME,1,7) having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' ",
#             '呼吸机记录数': "select count(1) as num ,substr(BEGINTIME,1,7) as month,'呼吸机记录数' as 业务类型 from treatment1 where TTYPE like '%呼吸机%' and BEGINTIME is not null group by substr(BEGINTIME,1,7) having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' ",
#             '泌尿道插管记录数': "select count(1) as num ,substr(BEGINTIME,1,7) as month,'泌尿道插管记录数' as 业务类型 from treatment1 where TTYPE like '%泌尿道%' and BEGINTIME is not null group by substr(BEGINTIME,1,7) having substr(BEGINTIME,1,7) <= to_char(sysdate,'yyyy-mm') and substr(BEGINTIME,1,7) >= '1990-01' ",
#         }
#         for bus in bus_dic:
#             temp = pd.read_sql(bus_dic[bus], con=engine)
#             res_全业务 = res_全业务.append(temp)
#         return res_全业务
#     else:
#         bus_dic = {
#             '入院人数': "select count(distinct caseid) as num ,to_char(to_date(substr(in_time,1,10),'yyyy-mm-dd'), 'iyyy-iw')  as month,'入院人数' as 业务类型 from overall where in_time is not null group by to_char(to_date(substr(in_time,1,10),'yyyy-mm-dd'), 'iyyy-iw')  having to_char(to_date(substr(in_time,1,10),'yyyy-mm-dd'), 'iyyy-iw')  <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(in_time,1,10),'yyyy-mm-dd'), 'iyyy-iw')  >= '1990-01'",
#             '出院人数': "select count(distinct caseid) as num ,to_char(to_date(substr(out_time,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month,'出院人数' as 业务类型 from overall where in_time is not null and out_time is not null group by to_char(to_date(substr(out_time,1,10),'yyyy-mm-dd'), 'iyyy-iw') having to_char(to_date(substr(out_time,1,10),'yyyy-mm-dd'), 'iyyy-iw') <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(out_time,1,10),'yyyy-mm-dd'), 'iyyy-iw') >= '1990-01' ",
#             '抗菌药物医嘱数': "select count( distinct CASEID||ORDERNO||ANAME ) as num , to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month ,'抗菌药物医嘱数' as 业务类型 from antibiotics where BEGINTIME is not null group by to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw')  having to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') >= '1990-01' ",
#             '手术台数': "select count( distinct CASEID||OPERID ) as num ,to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month,'手术台数' as 业务类型 from oper2 where BEGINTIME is not null group by to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') having to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') >= '1990-01' ",
#             '菌检出结果记录数': "select count( distinct CASEID||TESTNO||BACTERIA ) as num , to_char(to_date(substr(REQUESTTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month ,'菌检出结果记录数' as 业务类型 from bacteria where REQUESTTIME is not null group by  to_char(to_date(substr(REQUESTTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw')  having  to_char(to_date(substr(REQUESTTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw')  <= to_char(sysdate,'iyyy-iw') and  to_char(to_date(substr(REQUESTTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') >= '1990-01'",
#             '药敏结果记录数': "select count( distinct CASEID||TESTNO||BACTERIA||ANTIBIOTICS ) as num ,to_char(to_date(substr(REQUESTTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month ,'药敏结果记录数' as 业务类型  from DRUGSUSCEPTIBILITY where REQUESTTIME is not null group by to_char(to_date(substr(REQUESTTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw')  having to_char(to_date(substr(REQUESTTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw')  <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(REQUESTTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') >= '1990-01' ",
#             '体温测量数': "select count( distinct CASEID||RECORDDATE ) as num ,to_char(to_date(substr(RECORDDATE,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month  ,'体温测量数' as 业务类型 from TEMPERATURE where RECORDDATE is not null group by to_char(to_date(substr(RECORDDATE,1,10),'yyyy-mm-dd'), 'iyyy-iw')  having to_char(to_date(substr(RECORDDATE,1,10),'yyyy-mm-dd'), 'iyyy-iw') <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(RECORDDATE,1,10),'yyyy-mm-dd'), 'iyyy-iw') >= '1990-01'",
#             '入出转记录数': "select count( distinct CASEID||BEGINTIME||DEPT ) as num , to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month ,'入出转记录数' as 业务类型  from department where BEGINTIME is not null group by to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') having to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw')  >= '1990-01'",
#             '常规结果记录数': "select count( distinct CASEID||TESTNO||RINDEX ) as num ,to_char(to_date(substr(REQUESTTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month ,'常规结果记录数' as 业务类型  from ROUTINE2 where REQUESTTIME is not null group by to_char(to_date(substr(REQUESTTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') having to_char(to_date(substr(REQUESTTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(REQUESTTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') >= '1990-01' ",
#             '影像检查记录数': "select count( distinct CASEID||EXAM_NO ) as num ,to_char(to_date(substr(EXAM_DATE,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month  ,'影像检查记录数' as 业务类型 from EXAM where EXAM_DATE is not null group by to_char(to_date(substr(EXAM_DATE,1,10),'yyyy-mm-dd'), 'iyyy-iw')  having to_char(to_date(substr(EXAM_DATE,1,10),'yyyy-mm-dd'), 'iyyy-iw') <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(EXAM_DATE,1,10),'yyyy-mm-dd'), 'iyyy-iw') >= '1990-01'",
#             '治疗记录数': "select count( distinct CASEID||TNO||TTYPE||DEPT ) as num ,to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month  ,'治疗记录数' as 业务类型 from TREATMENT1 where BEGINTIME is not null group by to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') having to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') >= '1990-01' ",
#             '中心静脉插管记录数': "select count(1) as num ,to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month,'中心静脉插管记录数' as 业务类型 from treatment1 where TTYPE like '%中心%静脉%' and BEGINTIME is not null group by to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') having to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') >= '1990-01' ",
#             '呼吸机记录数': "select count(1) as num ,to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month,'呼吸机记录数' as 业务类型 from treatment1 where TTYPE like '%呼吸机%' and BEGINTIME is not null group by to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') having to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') >= '1990-01' ",
#             '泌尿道插管记录数': "select count(1) as num ,to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') as month,'泌尿道插管记录数' as 业务类型 from treatment1 where TTYPE like '%泌尿道%' and BEGINTIME is not null group by to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') having to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') <= to_char(sysdate,'iyyy-iw') and to_char(to_date(substr(BEGINTIME,1,10),'yyyy-mm-dd'), 'iyyy-iw') >= '1990-01'",
#         }
#
#         for bus in bus_dic:
#             temp = pd.read_sql(bus_dic[bus],con=engine)
#             temp['month'] = temp['month'].str.replace('-','年') +'周'
#             res_全业务 = res_全业务.append(temp)
#         return res_全业务


# 三级第一张图更新
@app.callback(
    Output('third_level_first_fig','figure'),
    Output('general_situation_third_level_first_fig_data', 'data'),

    Input('general_situation_third_level_first_fig_data', 'data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    Input('third_level_first_window_choice', 'value'),
    # Input('third_level_first_date_type_choice', 'value'),
    # prevent_initial_call=True,
)
# def update_third_level_first_fig(general_situation_third_level_first_fig_data,db_con_url,count_time,window,date_type):
def update_third_level_first_fig(general_situation_third_level_first_fig_data,db_con_url,count_time,window):
    # print(date_type)
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if general_situation_third_level_first_fig_data is None:
            general_situation_third_level_first_fig_data = {}
            # third_level_first_fig_date = get_third_level_first_fig_date(engine, 'week')
            # general_situation_third_level_first_fig_data['week'] = third_level_first_fig_date.to_json(orient='split', date_format='iso')
            # third_level_first_fig_date = get_third_level_first_fig_date(engine,'month')
            # general_situation_third_level_first_fig_data['month'] = third_level_first_fig_date.to_json(orient='split',date_format='iso')
            third_level_first_fig_date = get_third_level_first_fig_date(engine)
            general_situation_third_level_first_fig_data['third_level_first_fig_date'] = third_level_first_fig_date.to_json(orient='split',date_format='iso')
            general_situation_third_level_first_fig_data['hosname'] = db_con_url['hosname']
            general_situation_third_level_first_fig_data = json.dumps(general_situation_third_level_first_fig_data)
        else:
            general_situation_third_level_first_fig_data = json.loads(general_situation_third_level_first_fig_data)
            if db_con_url['hosname'] != general_situation_third_level_first_fig_data['hosname']:

                # third_level_first_fig_date = get_third_level_first_fig_date(engine, 'week')
                # general_situation_third_level_first_fig_data['week'] = third_level_first_fig_date.to_json( orient='split', date_format='iso')
                # third_level_first_fig_date = get_third_level_first_fig_date(engine, 'month')
                # general_situation_third_level_first_fig_data['month'] = third_level_first_fig_date.to_json( orient='split', date_format='iso')
                third_level_first_fig_date = get_third_level_first_fig_date(engine)
                general_situation_third_level_first_fig_data[ 'third_level_first_fig_date'] = third_level_first_fig_date.to_json(orient='split', date_format='iso')
                general_situation_third_level_first_fig_data['hosname'] = db_con_url['hosname']
                general_situation_third_level_first_fig_data = json.dumps( general_situation_third_level_first_fig_data)
            else:

                third_level_first_fig_date = pd.read_json(general_situation_third_level_first_fig_data['third_level_first_fig_date'],orient='split')

                general_situation_third_level_first_fig_data = dash.no_update

    # 布林图子图顺序
    # bus = ['入院人数', '入出转记录数', '抗菌药物医嘱数', '手术台数', '菌检出结果记录数', '药敏结果记录数', '体温测量数', '常规结果记录数', '影像检查记录数', '治疗记录数']
    bus = [  '抗菌药物医嘱数', '手术台数', '菌检出结果记录数', '药敏结果记录数', '体温测量数', '常规结果记录数', '影像检查记录数', '治疗记录数','中心静脉插管记录数','呼吸机记录数','出院人数','泌尿道插管记录数','入出转记录数','入院人数']
    # print(third_level_first_fig_date)
    fig = make_subplots(rows= 7 , cols=2, shared_xaxes=True)
    # btime = pd.read_sql(f"select to_char(to_date('{btime}-01','yyyy-mm-dd'),'iyyy-iw') as week from dual",con=engine)['week'][0].replace('-','年')+'周' if date_type == 'week' else btime
    # etime = pd.read_sql(f"select to_char(to_date('{etime}-01','yyyy-mm-dd'),'iyyy-iw') as week from dual",con=engine)['week'][0].replace('-','年')+'周' if date_type == 'week' else etime

    for i in range(1, 8):
        temp1 = bus[(i - 1) * 2]
        temp2 = bus[i * 2 - 1]
        df1 = third_level_first_fig_date[third_level_first_fig_date['业务类型'] == temp1]
        df1 = df1[ (df1['month']>=btime) & (df1['month']<=etime) ]
        df1 = df1.sort_values(['month'])

        df2 = third_level_first_fig_date[third_level_first_fig_date['业务类型'] == temp2]
        df2 = df2[ (df2['month'] >= btime) & (df2['month'] <= etime)]
        df2 = df2.sort_values(['month'])
        print(df1, df2)
        fig.add_trace(
            go.Scatter(x=df1['month'], y=df1['num'], name=bus[(i - 1) * 2]),
            row=i, col=1
        )
        data = df1[['month', 'num']]
        mean_data = np.array([data[i: i + window]['num'].mean() for i in range(len(data) - window + 1)])  # 计算移动平均线，转换为ndarray对象数据类型是为了更方便的计算上下轨线
        std_data = np.array([data[i: i + window]['num'].std() for i in range(len(data) - window + 1)])  # 计算移动标准差
        up_line = pd.DataFrame(mean_data + 2 * std_data, columns=['num'])  # 上轨线
        down_line = pd.DataFrame(mean_data - 2 * std_data, columns=['num'])  # 下轨线
        up_line['month'] = data['month'][(window - 1):].reset_index(drop=True)
        up_line = up_line.sort_values(['month'])
        down_line['month'] = data['month'][(window - 1):].reset_index(drop=True)
        down_line = down_line.sort_values(['month'])
        fig.add_trace(
            go.Scatter(x=up_line['month'], y=up_line['num'], name=f'{bus[(i - 1) * 2]}hign', line=dict(dash='dot')),
            row=i, col=1
        )
        fig.add_trace(
            go.Scatter(x=down_line['month'], y=down_line['num'], name=f'{bus[(i - 1) * 2]}low',
                       line=dict(dash='dot')),
            row=i, col=1
        )
        fig.update_yaxes(title_text=bus[(i - 1) * 2], row=i, col=1)

        fig.add_trace(
            go.Scatter(x=df2['month'], y=df2['num'], name=bus[i * 2 - 1], ),
            row=i, col=2
        )
        data = df2[['month', 'num']]
        # window = 5

        mean_data = np.array([data[i: i + window]['num'].mean() for i in range(len(data) - window + 1)])  # 计算移动平均线，转换为ndarray对象数据类型是为了更方便的计算上下轨线
        std_data = np.array([data[i: i + window]['num'].std() for i in range(len(data) - window + 1)])  # 计算移动标准差
        up_line = pd.DataFrame(mean_data + 2 * std_data, columns=['num'])  # 上轨线
        down_line = pd.DataFrame(mean_data - 2 * std_data, columns=['num'])  # 下轨线
        up_line['month'] = data['month'][(window - 1):].reset_index(drop=True)
        down_line['month'] = data['month'][(window - 1):].reset_index(drop=True)

        fig.add_trace(
            go.Scatter(x=up_line['month'], y=up_line['num'], name=f'{bus[i * 2 - 1]}hign', line=dict(dash='dot'), ),
            row=i, col=2
        )
        fig.add_trace(
            go.Scatter(x=down_line['month'], y=down_line['num'], name=f'{bus[i * 2 - 1]}low',
                       line=dict(dash='dot'), ),
            row=i, col=2
        )
        fig.update_yaxes(title_text=bus[i * 2 - 1], row=i, col=2)

    fig.update_layout(
        margin=dict(l=30, r=30, t=30, b=30),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        height=3000, width=1500,
        #title=f"{btime}--{etime}",
    )

    return fig,general_situation_third_level_first_fig_data



# -----------------------------------------------------------------------------------------------------    三级图二   ----------------------------------------------------------------------------------------------------------------------
# 获取概览三级第二张图数据
def get_third_level_second_fig_date(engine,bus_choice):
    res_各科室各业务数据量 = pd.DataFrame(columns=['业务类型','num','month','科室','科室名称'])
    bus_dic = {
        '入院人数':"""  select '入院人数' as 业务类型,num,MONTH,科室,label as 科室名称 from
                        (select count(distinct caseid) as num ,in_dept as 科室 ,substr(in_time,1,7) as month from overall where in_time is not null group by substr(in_time,1,7),in_dept order by substr(in_time,1,7),in_dept) t1,
                        s_departments t2 where t1.科室=t2.code(+)
                 """,
        '抗菌药物医嘱数':""" select '抗菌药物医嘱数' as 业务类型,num,MONTH,科室,label as 科室名称 from
                            (select count( distinct CASEID||ORDERNO||ANAME ) as num ,DEPT as  科室 ,substr(BEGINTIME,1,7) as month from antibiotics where BEGINTIME is not null group by substr(BEGINTIME,1,7),dept  ) t1,
                            s_departments t2 where t1.科室=t2.code(+)
                    """,
        '手术台数':""" select '手术台数' as 业务类型,num,MONTH,科室,label as 科室名称 from
                        (select count( distinct CASEID||OPERID ) as num ,DEPT as 科室 ,substr(BEGINTIME,1,7) as month from oper2 where BEGINTIME is not null group by substr(BEGINTIME,1,7),dept) t1,
                        s_departments t2 where t1.科室=t2.code(+) 
                 """,
        '菌检出结果记录数':""" select '菌检出结果记录数' as 业务类型,num,MONTH,科室,label as 科室名称 from
                            (select count( distinct CASEID||TESTNO||BACTERIA ) as num ,DEPT as 科室 ,substr(REQUESTTIME,1,7) as month from bacteria where REQUESTTIME is not null group by substr(REQUESTTIME,1,7),dept) t1,
                            s_departments t2 where t1.科室=t2.code(+)
                         """,
        '药敏结果记录数' : """ select '药敏结果记录数' as 业务类型,num,MONTH,科室,label as 科室名称 from
                            (select count( distinct CASEID||TESTNO||BACTERIA||ANTIBIOTICS ) as num ,DEPT as 科室 ,substr(REQUESTTIME,1,7) as month from DRUGSUSCEPTIBILITY where REQUESTTIME is not null group by substr(REQUESTTIME,1,7),dept ) t1,
                            s_departments t2 where t1.科室=t2.code(+)
                        """,
        '体温测量数' : """ select '体温测量数' as 业务类型,num,MONTH,科室,label as 科室名称 from
                        (select count( distinct CASEID||RECORDDATE ) as num ,DEPT as 科室 ,substr(RECORDDATE,1,7) as month from TEMPERATURE where RECORDDATE is not null group by substr(RECORDDATE,1,7),dept ) t1,
                        s_departments t2 where t1.科室=t2.code(+)
                    """,
        '入出转记录数' : """ select '入出转记录数' as 业务类型,num,MONTH,科室,label as 科室名称 from
                            (select count( distinct CASEID||BEGINTIME||DEPT ) as num ,DEPT as 科室 ,substr(BEGINTIME,1,7) as month from department where BEGINTIME is not null group by substr(BEGINTIME,1,7),dept ) t1,
                            s_departments t2 where t1.科室=t2.code(+)
                         """,
        '常规结果记录数' : """ select '常规结果记录数' as 业务类型,num,MONTH,科室,label as 科室名称 from
                            (select count( distinct CASEID||TESTNO||RINDEX ) as num ,DEPT as  科室 ,substr(REQUESTTIME,1,7) as month from ROUTINE2 where REQUESTTIME is not null group by substr(REQUESTTIME,1,7),dept ) t1,
                            s_departments t2 where t1.科室=t2.code(+)
                        """,
        '影像检查记录数' : """ select '影像检查记录数' as 业务类型,num,MONTH,科室,label as 科室名称 from
                            (select count( distinct CASEID||EXAM_NO ) as num ,DEPT as 科室 ,substr(EXAM_DATE,1,7) as month from (select t1.*,t2.dept from EXAM t1,DEPARTMENT t2 where t1.caseid = t2.caseid and t1.EXAM_DATE between t2.begintime(+) and t2. endtime(+)) where EXAM_DATE is not null group by substr(EXAM_DATE,1,7),dept)t1,
                            s_departments t2  where t1.科室=t2.code(+)
                        """,
        '治疗记录数' : """ select '治疗记录数' as 业务类型,num,MONTH,科室,label as 科室名称 from
                        (select count( distinct CASEID||TNO||TTYPE||DEPT ) as num ,DEPT as 科室 ,substr(BEGINTIME,1,7) as month from TREATMENT1 where BEGINTIME is not null group by substr(BEGINTIME,1,7),dept) t1,
                        s_departments t2 where t1.科室=t2.code(+)
                     """,
    }
    res_各科室各业务数据量 = pd.read_sql(bus_dic[bus_choice], con=engine)
    # for bus in bus_dic:
    #     res_各科室各业务数据量 = res_各科室各业务数据量.append(pd.read_sql(bus_dic[bus],con=engine))
    # return res_各科室各业务数据量.to_json(orient='split', date_format='iso')
    return res_各科室各业务数据量

# 三级第二张图更新
@app.callback(
    Output('third_level_second_fig', 'figure'),
    Output('general_situation_third_level_second_fig_data', 'data'),
    Output('third_level_second_dept_choice', 'options'),
    Output('third_level_second_dept_choice', 'value'),
    
    Input('general_situation_third_level_second_fig_data', 'data'),
    Input('third_level_second_business_choice', 'value'),
    Input('third_level_second_dept_choice', 'options'),
    Input('third_level_second_dept_choice', 'value'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
)
def update_third_level_second_fig(general_situation_third_level_second_fig_data, third_level_second_business_choice,third_level_second_dept_choice_ops, third_level_second_dept_choice,db_con_url,count_time):
    
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]



        if general_situation_third_level_second_fig_data is None:
            general_situation_third_level_second_fig_data = {}
            third_level_second_fig_date = get_third_level_second_fig_date(engine,third_level_second_business_choice)
            general_situation_third_level_second_fig_data[third_level_second_business_choice] = third_level_second_fig_date.to_json(orient='split', date_format='iso')
            general_situation_third_level_second_fig_data['hosname'] = db_con_url['hosname']
            general_situation_third_level_second_fig_data = json.dumps(general_situation_third_level_second_fig_data)
        else:
            general_situation_third_level_second_fig_data = json.loads(general_situation_third_level_second_fig_data)
            if db_con_url['hosname'] != general_situation_third_level_second_fig_data['hosname']:
                third_level_second_fig_date = get_third_level_second_fig_date(engine,third_level_second_business_choice)
                general_situation_third_level_second_fig_data[ third_level_second_business_choice] = third_level_second_fig_date.to_json(orient='split', date_format='iso')
                general_situation_third_level_second_fig_data['hosname'] = db_con_url['hosname']
                general_situation_third_level_second_fig_data = json.dumps( general_situation_third_level_second_fig_data)
            else:
                if third_level_second_business_choice not in general_situation_third_level_second_fig_data.keys():
                    third_level_second_fig_date = get_third_level_second_fig_date(engine, third_level_second_business_choice)
                    general_situation_third_level_second_fig_data[ third_level_second_business_choice] = third_level_second_fig_date.to_json(orient='split', date_format='iso')
                    general_situation_third_level_second_fig_data = json.dumps(general_situation_third_level_second_fig_data)
                else:
                    third_level_second_fig_date = pd.read_json(general_situation_third_level_second_fig_data[third_level_second_business_choice],orient='split')
                    general_situation_third_level_second_fig_data = dash.no_update

    third_level_second_dept_choice = '全部科室' if third_level_second_dept_choice is None else third_level_second_dept_choice

    print("三级第二张图业务：")
    print(third_level_second_fig_date)
    third_level_second_fig_date['科室名称'] = third_level_second_fig_date['科室名称'].fillna('科室无映射')
    if third_level_second_dept_choice_ops is None:
        third_level_second_dept_choice_options_lis = list( third_level_second_fig_date['科室名称'].fillna('科室无映射').drop_duplicates())
        third_level_second_dept_choice_options_lis.insert(0, '全部科室')
        third_level_second_dept_choice_ops = [{'label': dept, 'value': dept} for dept in third_level_second_dept_choice_options_lis]
        third_level_second_dept_choice = '全部科室'
    else:
        if type(third_level_second_dept_choice) == type([]):
            if "全部科室" in third_level_second_dept_choice:
                print()
            else:
                third_level_second_fig_date = third_level_second_fig_date[third_level_second_fig_date['科室名称'].isin(third_level_second_dept_choice)]
        else:
            if "全部科室" == third_level_second_dept_choice:
                print()
            else:
                third_level_second_fig_date = third_level_second_fig_date[third_level_second_fig_date['科室名称'] == third_level_second_dept_choice]

    third_level_second_fig_date = third_level_second_fig_date[ (third_level_second_fig_date['month']>=btime) & (third_level_second_fig_date['month']<=etime) ]
    # 画图

    third_level_second_fig_date = third_level_second_fig_date.sort_values(['month','科室名称'])
    fig1 = px.line(third_level_second_fig_date, x='month', y= 'num' , color= '科室名称', color_discrete_sequence=px.colors.qualitative.Dark24)

    fig1.update_layout(
        margin=dict(l=30, r=30, t=30, b=30),
        #title=f"{btime}--{etime}",
    )
    fig1.update_yaxes(title_text= third_level_second_business_choice, )
    fig1.update_xaxes(title_text= '月份', )

    return fig1,general_situation_third_level_second_fig_data,third_level_second_dept_choice_ops,third_level_second_dept_choice



# -----------------------------------------------------------------------------------------------------    概览下载   ----------------------------------------------------------------------------------------------------------------------
# 三级第二张图数据更新(为概览页面数据统计结果下载做数据准备)
@app.callback(
    Output('general_situation_third_level_second_fig_data1', 'data'),

    Input('general_situation_third_level_second_fig_data1', 'data'),
    Input('third_level_second_business_choice', 'options'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
)
def update_third_level_second_fig(general_situation_third_level_second_fig_data, third_level_second_business_choice, db_con_url ,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]

        if general_situation_third_level_second_fig_data is None:
            general_situation_third_level_second_fig_data = {}
            for bus in third_level_second_business_choice:
                print(bus)
                general_situation_third_level_second_fig_data[bus['label']] = get_third_level_second_fig_date(engine, bus['label']).to_json(orient='split', date_format='iso')
            general_situation_third_level_second_fig_data['hosname'] = db_con_url['hosname']
            general_situation_third_level_second_fig_data['btime'] = btime
            general_situation_third_level_second_fig_data['etime'] = etime
            general_situation_third_level_second_fig_data = json.dumps(general_situation_third_level_second_fig_data)
        else:
            general_situation_third_level_second_fig_data = json.loads(general_situation_third_level_second_fig_data)
            if db_con_url['hosname'] != general_situation_third_level_second_fig_data['hosname']:
                general_situation_third_level_second_fig_data = {}
                for bus in third_level_second_business_choice:
                    general_situation_third_level_second_fig_data[bus['label']] = get_third_level_second_fig_date(engine, bus['label']).to_json( orient='split', date_format='iso')
                general_situation_third_level_second_fig_data['hosname'] = db_con_url['hosname']
                general_situation_third_level_second_fig_data['btime'] = btime
                general_situation_third_level_second_fig_data['etime'] = etime
                general_situation_third_level_second_fig_data = json.dumps( general_situation_third_level_second_fig_data)
            else:
                general_situation_third_level_second_fig_data = dash.no_update

    return general_situation_third_level_second_fig_data


# 概览页面数据统计结果下载
@app.callback(
    Output("down-all-count-data", "data"),
    Input("all-count-data-down", "n_clicks"),
    Input("general_situation_first_level_first_fig_data", "data"),
    Input("general_situation_first_level_second_fig_data", "data"),
    Input("general_situation_first_level_third_fig_data", "data"),
    Input("general_situation_secod_level_fig_data", "data"),
    Input("general_situation_third_level_first_fig_data", "data"),
    Input("general_situation_third_level_second_fig_data1", "data"),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    Input('third_level_second_business_choice', 'options'),
    prevent_initial_call=True,
)
def get_all_count_data(n_clicks, general_situation_first_level_first_fig_data,
                                general_situation_first_level_second_fig_data,
                                general_situation_first_level_third_fig_data,
                                general_situation_secod_level_fig_data,
                                general_situation_third_level_first_fig_data,
                                general_situation_third_level_second_fig_data1,
                       db_con_url,count_time,third_level_second_business_choice):
    if db_con_url is None :
        return dash.no_update
    else:
        if n_clicks is not None and n_clicks>0:
            n_clicks = 0
            db_con_url = json.loads(db_con_url)
            count_time = json.loads(count_time)
            engine = create_engine(db_con_url['db'])
            hosName = db_con_url['hosname']
            btime = count_time['btime'][0:7]
            etime = count_time['etime'][0:7]
            now_time = str(datetime.now())[0:19].replace(' ', '_').replace(':', '_')
            if general_situation_first_level_first_fig_data is not None and general_situation_first_level_second_fig_data is not None and general_situation_first_level_third_fig_data is not None and \
                    general_situation_secod_level_fig_data is not None and general_situation_third_level_first_fig_data is not None and general_situation_third_level_second_fig_data1 is not None :
                general_situation_first_level_first_fig_data = json.loads(general_situation_first_level_first_fig_data )
                general_situation_first_level_second_fig_data = json.loads(general_situation_first_level_second_fig_data )
                general_situation_first_level_third_fig_data = json.loads(general_situation_first_level_third_fig_data )
                general_situation_secod_level_fig_data = json.loads(general_situation_secod_level_fig_data )
                general_situation_third_level_first_fig_data = json.loads(general_situation_third_level_first_fig_data )
                general_situation_third_level_second_fig_data1 = json.loads(general_situation_third_level_second_fig_data1 )
                if general_situation_first_level_first_fig_data['hosname'] == hosName and  general_situation_first_level_second_fig_data['hosname'] == hosName and general_situation_first_level_second_fig_data['btime'] == btime and  general_situation_first_level_second_fig_data['etime'] == etime and \
                   general_situation_first_level_third_fig_data['hosname'] == hosName and  general_situation_first_level_third_fig_data['btime'] == btime and  general_situation_first_level_third_fig_data['etime'] == etime and \
                   general_situation_secod_level_fig_data['hosname'] == hosName and  general_situation_secod_level_fig_data['btime'] == btime and  general_situation_secod_level_fig_data['etime'] == etime and \
                   general_situation_third_level_first_fig_data['hosname'] == hosName and \
                   general_situation_third_level_second_fig_data1['hosname'] == hosName and general_situation_third_level_second_fig_data1['btime'] == btime and  general_situation_third_level_second_fig_data1['etime'] == etime :

                    first_level_first_fig_data = pd.read_json( general_situation_first_level_first_fig_data['first_level_first_fig_data'], orient='split')
                    first_level_second_fig_data = pd.read_json( general_situation_first_level_second_fig_data['first_level_second_fig_data'], orient='split')

                    first_level_third_fig_data = pd.read_json( general_situation_first_level_third_fig_data['first_level_third_fig_data'], orient='split')
                    second_level_fig_date = pd.read_json(general_situation_secod_level_fig_data['second_level_fig_date'], orient='split')

                    third_level_first_fig_date = pd.read_json( general_situation_third_level_first_fig_data['third_level_first_fig_date'], orient='split')
                    # third_level_first_fig_date = pd.read_json(general_situation_third_level_first_fig_data['month'],orient='split')
                    # third_level_first_fig_date = third_level_first_fig_date.append(pd.read_json(general_situation_third_level_first_fig_data['week'],orient='split'))
                    third_level_second_fig_date = pd.DataFrame()
                    for bus in third_level_second_business_choice:
                        if third_level_second_fig_date.shape[0] == 0:
                            third_level_second_fig_date = pd.read_json( general_situation_third_level_second_fig_data1[bus['label']], orient='split')
                        else:
                            third_level_second_fig_date = third_level_second_fig_date.append( pd.read_json( general_situation_third_level_second_fig_data1[bus['label']], orient='split') )

                    output = io.BytesIO()
                    writer = pd.ExcelWriter(output, engine='xlsxwriter')
                    first_level_first_fig_data.to_excel(writer, sheet_name='各业务时间缺失数量占比',index=False)
                    first_level_second_fig_data.to_excel(writer, sheet_name='各业务关键字缺失数量占比',index=False)
                    first_level_third_fig_data.to_excel(writer, sheet_name='科室映射缺失数量占比',index=False)
                    second_level_fig_date.to_excel(writer, sheet_name='全院数据逻辑问题',index=False)
                    third_level_first_fig_date.to_excel(writer, sheet_name='全院数据时间趋势图全业务',index=False)
                    third_level_second_fig_date.to_excel(writer, sheet_name='全院数据时间趋势图单业务',index=False)
                    writer.save()
                    data = output.getvalue()
                    hosName = db_con_url['hosname']
                    return dcc.send_bytes(data, f'{hosName}_{now_time}.xlsx')

                else:
                    return dash.no_update
            else:
                return dash.no_update
        else:
            return dash.no_update

