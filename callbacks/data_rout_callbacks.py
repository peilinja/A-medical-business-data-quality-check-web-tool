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
import time
import re


# -----------------------------------------------------------------------------------------------------    一级图一   ----------------------------------------------------------------------------------------------------------------------
# 获取抗菌药物-菌检出-药敏一级第一张图数据
def get_first_lev_first_fig_date(engine):
    res = pd.DataFrame(columns=['业务类型', 'num', 'month' ])
    # 问题类别、问题数据量统计、全数据统计
    bus_dic = {
               '生化': "select '生化' as 业务类型 ,count(1) as num ,substr(REQUESTTIME,1,7) as month from ROUTINE2 where  REQUESTTIME is not null group by substr(REQUESTTIME,1,7)",
               '检查': " select '检查' as 业务类型 , count(1) as num ,substr(EXAM_DATE,1,7) as month from EXAM where  EXAM_DATE is not null group by substr(EXAM_DATE,1,7) ",
               '体温': " select '体温' as 业务类型 , count(1) as num ,substr(RECORDDATE,1,7) as month from TEMPERATURE where  RECORDDATE is not null group by substr(RECORDDATE,1,7) ",
               }

    for bus in bus_dic:
        res = res.append(pd.read_sql(bus_dic[bus],con=engine))
    return res
# 更新抗菌药物-菌检出-药敏一级图一
@app.callback(
    Output('rout_exam_temp_first_level_first_fig','figure'),
    Output('rout_exam_temp_first_level_first_fig_data','data'),

    Input('rout_exam_temp_first_level_first_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_first_level_first_fig(rout_exam_temp_first_level_first_fig_data,db_con_url,count_time):
    if db_con_url is None :
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        engine = create_engine(db_con_url['db'])
        if rout_exam_temp_first_level_first_fig_data is None:
            rout_exam_temp_first_level_first_fig_data = {}
            rout_exam_temp_first_level_first_fig = get_first_lev_first_fig_date(engine)
            rout_exam_temp_first_level_first_fig_data['rout_exam_temp_first_level_first_fig'] = rout_exam_temp_first_level_first_fig.to_json(orient='split', date_format='iso')
            rout_exam_temp_first_level_first_fig_data['hosname'] = db_con_url['hosname']
            rout_exam_temp_first_level_first_fig_data['btime'] = btime
            rout_exam_temp_first_level_first_fig_data['etime'] = etime
            rout_exam_temp_first_level_first_fig_data = json.dumps(rout_exam_temp_first_level_first_fig_data)
        else:
            rout_exam_temp_first_level_first_fig_data = json.loads(rout_exam_temp_first_level_first_fig_data)
            if db_con_url['hosname'] != rout_exam_temp_first_level_first_fig_data['hosname']:
                rout_exam_temp_first_level_first_fig = get_first_lev_first_fig_date(engine)
                rout_exam_temp_first_level_first_fig_data['rout_exam_temp_first_level_first_fig'] = rout_exam_temp_first_level_first_fig.to_json(orient='split',date_format='iso')
                rout_exam_temp_first_level_first_fig_data['hosname'] = db_con_url['hosname']
                rout_exam_temp_first_level_first_fig_data = json.dumps(rout_exam_temp_first_level_first_fig_data)
            else:
                rout_exam_temp_first_level_first_fig = pd.read_json(rout_exam_temp_first_level_first_fig_data['rout_exam_temp_first_level_first_fig'], orient='split')
                rout_exam_temp_first_level_first_fig_data = dash.no_update
            #
        rout_exam_temp_first_level_first_fig = rout_exam_temp_first_level_first_fig[(rout_exam_temp_first_level_first_fig['month']>=btime) & (rout_exam_temp_first_level_first_fig['month']<=etime)]
        rout_exam_temp_first_level_first_fig = rout_exam_temp_first_level_first_fig.sort_values(['month','业务类型'])
        fig1 = px.line(rout_exam_temp_first_level_first_fig, x='month', y='num', color='业务类型',
                       color_discrete_sequence=px.colors.qualitative.Dark24)

        # 设置水平图例及位置
        fig1.update_layout(
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ))
        fig1.update_yaxes(title_text="业务数据量")
        fig1.update_xaxes(title_text="时间")
        return fig1,rout_exam_temp_first_level_first_fig_data


# -----------------------------------------------------------------------------------------------------    一级图二   ----------------------------------------------------------------------------------------------------------------------
# 获取一级第二张图片数据
def get_first_lev_second_fig_date(engine):
    res = pd.DataFrame(columns=['问题类型', 'num' ])
    # 问题类别、问题数据量统计、全数据统计
    bus_dic = {
               '体温测量时间缺失': f"select '体温测量时间缺失' as 问题类型 ,count(1) as num from TEMPERATURE where  RECORDDATE is null ",
               '生化检验申请时间缺失': f"select '生化检验申请时间缺失' as 问题类型 ,count(1) as num from ROUTINE2 where  REQUESTTIME is null ",
               '生化检验报告时间缺失': f"select '生化检验报告时间缺失' as 问题类型 ,count(1) as num from ROUTINE2 where  REPORTTIME is null",
               '检查时间为空': f"select '检查时间为空' as 问题类型 ,count(1) as num  from exam where  EXAM_DATE is null ",
               }

    for bus in bus_dic: 
        res = res.append(pd.read_sql(bus_dic[bus],con=engine))
    return res
# 更新一级图二
@app.callback(
    Output('rout_exam_temp_first_level_second_fig','figure'),
    Output('rout_exam_temp_first_level_second_fig_data','data'),

    Input('rout_exam_temp_first_level_second_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_first_level_first_fig(rout_exam_temp_first_level_second_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if rout_exam_temp_first_level_second_fig_data is None:
            rout_exam_temp_first_level_second_fig = get_first_lev_second_fig_date(engine)
            rout_exam_temp_first_level_second_fig_data = {}
            rout_exam_temp_first_level_second_fig_data['rout_exam_temp_first_level_second_fig'] = rout_exam_temp_first_level_second_fig.to_json( orient='split', date_format='iso')
            rout_exam_temp_first_level_second_fig_data['hosname'] = db_con_url['hosname']
            rout_exam_temp_first_level_second_fig_data = json.dumps(rout_exam_temp_first_level_second_fig_data)
        else:
            rout_exam_temp_first_level_second_fig_data = json.loads(rout_exam_temp_first_level_second_fig_data)
            if db_con_url['hosname'] != rout_exam_temp_first_level_second_fig_data['hosname']:
                rout_exam_temp_first_level_second_fig = get_first_lev_second_fig_date(engine)
                rout_exam_temp_first_level_second_fig_data = {}
                rout_exam_temp_first_level_second_fig_data[ 'rout_exam_temp_first_level_second_fig'] = rout_exam_temp_first_level_second_fig.to_json( orient='split', date_format='iso')
                rout_exam_temp_first_level_second_fig_data['hosname'] = db_con_url['hosname']
                rout_exam_temp_first_level_second_fig_data = json.dumps(rout_exam_temp_first_level_second_fig_data)
            else:
                rout_exam_temp_first_level_second_fig = pd.read_json( rout_exam_temp_first_level_second_fig_data['rout_exam_temp_first_level_second_fig'], orient='split')
                rout_exam_temp_first_level_second_fig_data = dash.no_update

        fig = go.Figure()
        # fig = px.bar(rout_exam_temp_first_level_second_fig,x='问题类型',y='num',color_discrete_sequence=px.colors.qualitative.Dark24 )
        fig.add_trace(
            go.Bar(x=rout_exam_temp_first_level_second_fig['问题类型'], y=rout_exam_temp_first_level_second_fig['num'], name="问题类型",
                   marker_color=px.colors.qualitative.Dark24, )
        )
        fig.update_layout(
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )

        )
        fig.update_yaxes(title_text="问题数量")
        fig.update_xaxes(title_text="月份")
        return fig, rout_exam_temp_first_level_second_fig_data


# 下载一级图二明细
@app.callback(
    Output('rout_exam_temp_first_level_second_fig_detail', 'data'),
    Input('rout_exam_temp_first_level_second_fig_data_detail_down','n_clicks'),
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
            bus_dic = {
                '体温测量时间缺失': f"select * from TEMPERATURE where  RECORDDATE is null ",
                '生化检验申请时间缺失': f"select * from ROUTINE2 where  REQUESTTIME is null ",
                '生化检验报告时间缺失': f"select * from ROUTINE2 where  REPORTTIME is null",
                '检查时间为空': f"select * from exam where  EXAM_DATE is null ",
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
            return dcc.send_bytes(data, f'{hosName}时间缺失数据明细.xlsx')
        else:
            return dash.no_update


# # -----------------------------------------------------------------------------------------------------    二级图一   ----------------------------------------------------------------------------------------------------------------------
# # 获取体温二级第一张图数据
def get_second_lev_first_fig_date(engine,btime,etime):
    res = pd.DataFrame(columns=['问题类型','num','momth'])
    bus_dic = {
        '体温测量值异常': f"select '体温测量值异常' as 问题类型 ,count(1) as num ,substr(RECORDDATE,1,7) as month from TEMPERATURE where (VALUE >46 or VALUE<34) and substr(RECORDDATE,1,7)>='{btime}' and substr(RECORDDATE,1,7)<='{etime}' group by substr(RECORDDATE,1,7)",
        '体温测量值缺失': f"select '体温测量值缺失' as 问题类型 ,count(1) as num ,substr(RECORDDATE,1,7) as month from TEMPERATURE where VALUE is null and substr(RECORDDATE,1,7)>='{btime}' and substr(RECORDDATE,1,7)<='{etime}' group by substr(RECORDDATE,1,7)",
        '科室缺失': f"select '科室缺失' as 问题类型 ,count(1) as num ,substr(RECORDDATE,1,7) as month from TEMPERATURE where DEPT is null and substr(RECORDDATE,1,7)>='{btime}' and substr(RECORDDATE,1,7)<='{etime}' group by substr(RECORDDATE,1,7)",
        '体温测量时机缺失': f"select '体温测量时机缺失' as 问题类型 ,count(1) as num ,substr(RECORDDATE,1,7) as month from TEMPERATURE where OUTSIDE is null and substr(RECORDDATE,1,7)>='{btime}' and substr(RECORDDATE,1,7)<='{etime}' group by substr(RECORDDATE,1,7)",
        '体温测量时间无时间点': f"select '检验测量时间无时间点' as 问题类型 ,count(1) as num ,substr(RECORDDATE,1,7) as month from TEMPERATURE where  length(RECORDDATE)<19 and substr(RECORDDATE,1,7)>='{btime}' and substr(RECORDDATE,1,7)<='{etime}' group by substr(RECORDDATE,1,7)",
        '体温测量时间在出入院时间之外': f""" select '体温测量时间在出入院时间之外' as 问题类型,count(1) as num ,substr(RECORDDATE,1,7) as month from TEMPERATURE t1,overall t2 where 
                                        ( t1.RECORDDATE is not null and t2.in_time is not null and t2.out_time is not null) 
                                        and t1.caseid = t2.caseid 
                                        and (t1.RECORDDATE<t2.IN_TIME or t1.RECORDDATE > t2.OUT_TIME ) 
                                        and (substr(t1.RECORDDATE,1,7)>='{btime}' and substr(t1.RECORDDATE,1,7)<='{etime}')
                                        group by substr(RECORDDATE,1,7)
                                     """,
    }
    for bus in bus_dic:
        res = res.append(pd.read_sql(bus_dic[bus],con=engine))
    return res
# 更新二级图一
@app.callback(
    Output('temp_second_level_first_fig','figure'),
    Output('temp_second_level_first_fig_data','data'),

    Input('temp_second_level_first_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
)
def update_first_level_second_fig(temp_second_level_first_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if temp_second_level_first_fig_data is None:
            temp_second_level_first_fig_data = {}
            temp_second_level_first_fig = get_second_lev_first_fig_date(engine, btime, etime)
            temp_second_level_first_fig_data['temp_second_level_first_fig'] = temp_second_level_first_fig.to_json( orient='split', date_format='iso')
            temp_second_level_first_fig_data['hosname'] = db_con_url['hosname']
            temp_second_level_first_fig_data['btime'] = btime
            temp_second_level_first_fig_data['etime'] = etime
            temp_second_level_first_fig_data = json.dumps(temp_second_level_first_fig_data)
        else:
            temp_second_level_first_fig_data = json.loads(temp_second_level_first_fig_data)
            if db_con_url['hosname'] != temp_second_level_first_fig_data['hosname']:
                temp_second_level_first_fig = get_second_lev_first_fig_date(engine, btime, etime)
                temp_second_level_first_fig_data['temp_second_level_first_fig'] = temp_second_level_first_fig.to_json( orient='split', date_format='iso')
                temp_second_level_first_fig_data['hosname'] = db_con_url['hosname']
                temp_second_level_first_fig_data['btime'] = btime
                temp_second_level_first_fig_data['etime'] = etime
                temp_second_level_first_fig_data = json.dumps(temp_second_level_first_fig_data)
            else:
                if temp_second_level_first_fig_data['btime'] != btime or temp_second_level_first_fig_data[ 'etime'] != etime:
                    temp_second_level_first_fig = get_second_lev_first_fig_date(engine, btime, etime)
                    temp_second_level_first_fig_data[ 'temp_second_level_first_fig'] = temp_second_level_first_fig.to_json( orient='split', date_format='iso')
                    temp_second_level_first_fig_data['btime'] = btime
                    temp_second_level_first_fig_data['etime'] = etime
                    temp_second_level_first_fig_data = json.dumps(temp_second_level_first_fig_data)
                else:
                    temp_second_level_first_fig = pd.read_json( temp_second_level_first_fig_data['temp_second_level_first_fig'], orient='split')
                    temp_second_level_first_fig_data = dash.no_update

    temp_second_level_first_fig = temp_second_level_first_fig.sort_values(['month'])
    fig = px.line(temp_second_level_first_fig, x="month", y="num", color='问题类型',
                 color_discrete_sequence=px.colors.qualitative.Dark24)

    fig.update_layout(
        margin=dict(l=30, r=30, t=30, b=30),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    fig.update_yaxes(title_text="体温测量数量", )
    fig.update_xaxes(title_text="月份", )
    return fig, temp_second_level_first_fig_data

# 下载二级图一明细
@app.callback(
    Output('temp_second_level_first_fig_detail', 'data'),
    Input('temp_second_level_first_fig_data_detail_down','n_clicks'),
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
            btime = count_time['btime'][0:7]
            etime = count_time['etime'][0:7]
            engine = create_engine(db_con_url['db'])
            bus_dic = {
                '体温测量值异常': f"select * from TEMPERATURE where (VALUE >46 or VALUE<34) and substr(RECORDDATE,1,7)>='{btime}' and substr(RECORDDATE,1,7)<='{etime}' ",
                '体温测量值缺失': f"select * from TEMPERATURE where VALUE is null and substr(RECORDDATE,1,7)>='{btime}' and substr(RECORDDATE,1,7)<='{etime}' ",
                '科室缺失': f"select * from TEMPERATURE where DEPT is null and substr(RECORDDATE,1,7)>='{btime}' and substr(RECORDDATE,1,7)<='{etime}' ",
                '体温测量时机缺失': f"select * from TEMPERATURE where OUTSIDE is null and substr(RECORDDATE,1,7)>='{btime}' and substr(RECORDDATE,1,7)<='{etime}' ",
                '体温测量时间无时间点': f"select * from TEMPERATURE where  length(RECORDDATE)<19 and substr(RECORDDATE,1,7)>='{btime}' and substr(RECORDDATE,1,7)<='{etime}' ",
                '体温测量时间在出入院时间之外': f""" select t1.*,t2.in_time as 入院时间,t2.out_time as 出院时间  from TEMPERATURE t1,overall t2 where 
                                                    ( t1.RECORDDATE is not null and t2.in_time is not null and t2.out_time is not null) 
                                                    and t1.caseid = t2.caseid 
                                                    and (t1.RECORDDATE<t2.IN_TIME or t1.RECORDDATE > t2.OUT_TIME ) 
                                                    and (substr(t1.RECORDDATE,1,7)>='{btime}' and substr(t1.RECORDDATE,1,7)<='{etime}') 
                                                 """,
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
            return dcc.send_bytes(data, f'{hosName}体温问题数据明细.xlsx')
        else:
            return dash.no_update

#
# # # -----------------------------------------------------------------------------------------------------    三级图一   ----------------------------------------------------------------------------------------------------------------------
# 获取生化检验三级第一张图数据
def get_third_lev_first_fig_date(engine,btime,etime):
    res_数据时间缺失及汇总 = pd.DataFrame(columns=['问题类型', 'num', 'month' ])
    # 问题类别、问题数据量统计、全数据统计
    bus_dic = {
               '标本缺失': f"select '标本缺失' as 问题类型 ,count(1) as num ,substr(REQUESTTIME,1,7) as month from ROUTINE2 where  REQUESTTIME is not null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and SPECIMEN is null group by substr(REQUESTTIME,1,7)",
               '检验项目缺失': f"select '检验项目缺失' as 问题类型 ,count(1) as num ,substr(REQUESTTIME,1,7) as month from ROUTINE2 where  REQUESTTIME is not null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and RTYPE is null group by substr(REQUESTTIME,1,7)",
               '检验结果缺失': f"select '检验结果缺失' as 问题类型 ,count(1) as num ,substr(REQUESTTIME,1,7) as month from ROUTINE2 where  REQUESTTIME is not null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and RVALUE is null group by substr(REQUESTTIME,1,7)",
               '院内外标识缺失': f"select '院内外标识缺失' as 问题类型 ,count(1) as num ,substr(REQUESTTIME,1,7) as month from ROUTINE2 where  REQUESTTIME is not null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and OUTSIDE is null group by substr(REQUESTTIME,1,7)",
               '检验子项缺失': f"select '检验子项缺失' as 问题类型 ,count(1) as num ,substr(REQUESTTIME,1,7) as month from ROUTINE2 where  REQUESTTIME is not null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and RITEM is null group by substr(REQUESTTIME,1,7)",
               '定性结果缺失': f"select '定性结果缺失' as 问题类型 ,count(1) as num ,substr(REQUESTTIME,1,7) as month from ROUTINE2 where  REQUESTTIME is not null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and ABNORMAL is null group by substr(REQUESTTIME,1,7)",
               '申请时间大于等于报告时间': f"select '申请时间大于等于报告时间' as 问题类型 ,count(1) as num ,substr(REQUESTTIME,1,7) as month from ROUTINE2 where  REQUESTTIME >= REPORTTIME and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' group by substr(REQUESTTIME,1,7)",
               '申请时间在出入院时间之外': f""" select '申请时间在出入院时间之外' as 问题类型,count(1) as num ,substr(REQUESTTIME,1,7) as month from ROUTINE2 t1,overall t2 where 
                                    ( t1.REQUESTTIME is not null and t1.REPORTTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                    and t1.caseid = t2.caseid 
                                    and (t1.REQUESTTIME<t2.IN_TIME or t1.REQUESTTIME > t2.OUT_TIME ) 
                                    and (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}')
                                    group by substr(REQUESTTIME,1,7)
                                 """,
               }

    for bus in bus_dic:
        res_数据时间缺失及汇总 = res_数据时间缺失及汇总.append(pd.read_sql(bus_dic[bus],con=engine))
    return res_数据时间缺失及汇总
# 更新抗菌药物-菌检出-药敏一级图一
@app.callback(
    Output('rout_third_level_first_fig','figure'),
    Output('rout_third_level_first_fig_data','data'),

    Input('rout_third_level_first_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_first_level_first_fig(rout_third_level_first_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if rout_third_level_first_fig_data is None:
            rout_third_level_first_fig_data = {}
            rout_third_level_first_fig = get_third_lev_first_fig_date(engine, btime, etime)
            rout_third_level_first_fig_data['rout_third_level_first_fig'] = rout_third_level_first_fig.to_json( orient='split', date_format='iso')
            rout_third_level_first_fig_data['hosname'] = db_con_url['hosname']
            rout_third_level_first_fig_data['btime'] = btime
            rout_third_level_first_fig_data['etime'] = etime
            rout_third_level_first_fig_data = json.dumps(rout_third_level_first_fig_data)
        else:
            rout_third_level_first_fig_data = json.loads(rout_third_level_first_fig_data)
            if db_con_url['hosname'] != rout_third_level_first_fig_data['hosname']:
                rout_third_level_first_fig = get_third_lev_first_fig_date(engine, btime, etime)
                rout_third_level_first_fig_data['rout_third_level_first_fig'] = rout_third_level_first_fig.to_json( orient='split', date_format='iso')
                rout_third_level_first_fig_data['hosname'] = db_con_url['hosname']
                rout_third_level_first_fig_data['btime'] = btime
                rout_third_level_first_fig_data['etime'] = etime
                rout_third_level_first_fig_data = json.dumps(rout_third_level_first_fig_data)
            else:
                if rout_third_level_first_fig_data['btime'] != btime or rout_third_level_first_fig_data[ 'etime'] != etime:
                    rout_third_level_first_fig = get_third_lev_first_fig_date(engine, btime, etime)
                    rout_third_level_first_fig_data[ 'rout_third_level_first_fig'] = rout_third_level_first_fig.to_json(orient='split',  date_format='iso')
                    rout_third_level_first_fig_data['btime'] = btime
                    rout_third_level_first_fig_data['etime'] = etime
                    rout_third_level_first_fig_data = json.dumps(rout_third_level_first_fig_data)
                else:
                    rout_third_level_first_fig = pd.read_json( rout_third_level_first_fig_data['rout_third_level_first_fig'], orient='split')
                    rout_third_level_first_fig_data = dash.no_update
        rout_third_level_first_fig = rout_third_level_first_fig.sort_values(['month'])
        fig = px.line(rout_third_level_first_fig,x='month',y='num',color='问题类型',color_discrete_sequence=px.colors.qualitative.Dark24 )
        fig.update_layout(
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )

        )
        fig.update_yaxes(title_text="问题数量")
        fig.update_xaxes(title_text="月份")
        return fig, rout_third_level_first_fig_data


# 下载三级图一明细
@app.callback(
    Output('rout_third_level_first_fig_detail', 'data'),
    Input('rout_third_level_first_fig_data_detail_down','n_clicks'),
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
            btime = count_time['btime'][0:7]
            etime = count_time['etime'][0:7]
            bus_dic = {
                '标本缺失': f"select * from ROUTINE2 where  REQUESTTIME is not null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and SPECIMEN is null ",
                '检验项目缺失': f"select * from ROUTINE2 where  REQUESTTIME is not null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and RTYPE is null  ",
                '检验结果缺失': f"select * from ROUTINE2 where  REQUESTTIME is not null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and RVALUE is null  ",
                '院内外标识缺失': f"select * from ROUTINE2 where  REQUESTTIME is not null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and OUTSIDE is null  ",
                '检验子项缺失': f"select * from ROUTINE2 where  REQUESTTIME is not null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and RITEM is null  ",
                '定性结果缺失': f"select * from ROUTINE2 where  REQUESTTIME is not null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and ABNORMAL is null  ",
                '申请时间大于等于报告时间': f"select * from ROUTINE2 where  REQUESTTIME >= REPORTTIME and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}'  ",
                '申请时间在出入院时间之外': f""" select t1.* ,t2.in_time as 入院时间,t2.out_time as 出院时间  from ROUTINE2 t1,overall t2 where 
                                                ( t1.REQUESTTIME is not null and t1.REPORTTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                                and t1.caseid = t2.caseid 
                                                and (t1.REQUESTTIME<t2.IN_TIME or t1.REQUESTTIME > t2.OUT_TIME ) 
                                                and (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}') 
                                             """,
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
            return dcc.send_bytes(data, f'{hosName}生化检验问题数据明细.xlsx')
        else:
            return dash.no_update


# # # -----------------------------------------------------------------------------------------------------    三级图二   ----------------------------------------------------------------------------------------------------------------------
# 获取生化三级第二张图数据
def get_third_level_second_fig_date(engine,btime,etime):
    res = pd.read_sql(f"select RTYPE as 生化检验类型,count(distinct CASEID||TESTNO||RTYPE) as num ,substr(REQUESTTIME,1,7) as month from ROUTINE2 where RTYPE is not  null and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' group by RTYPE,substr(REQUESTTIME,1,7)",con=engine)
    return res


# 更新生化三级第二张图
@app.callback(
    Output('rout_third_level_second_fig','figure'),
    Output('rout_third_level_second_fig_data','data'),

    Input('rout_third_level_second_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_second_level_fig(rout_third_level_second_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if rout_third_level_second_fig_data is None:
            rout_third_level_second_fig_data = {}
            rout_third_level_second_fig = get_third_level_second_fig_date(engine, btime, etime)
            rout_third_level_second_fig_data['rout_third_level_second_fig'] = rout_third_level_second_fig.to_json(orient='split', date_format='iso')
            rout_third_level_second_fig_data['hosname'] = db_con_url['hosname']
            rout_third_level_second_fig_data['btime'] = btime
            rout_third_level_second_fig_data['etime'] = etime
            rout_third_level_second_fig_data = json.dumps(rout_third_level_second_fig_data)
        else:
            rout_third_level_second_fig_data = json.loads(rout_third_level_second_fig_data)
            if db_con_url['hosname'] != rout_third_level_second_fig_data['hosname']:
                rout_third_level_second_fig = get_third_level_second_fig_date(engine, btime, etime)
                rout_third_level_second_fig_data['rout_third_level_second_fig'] = rout_third_level_second_fig.to_json(orient='split',date_format='iso')
                rout_third_level_second_fig_data['hosname'] = db_con_url['hosname']
                rout_third_level_second_fig_data['btime'] = btime
                rout_third_level_second_fig_data['etime'] = etime
                rout_third_level_second_fig_data = json.dumps(rout_third_level_second_fig_data)
            else:
                if rout_third_level_second_fig_data['btime'] != btime or rout_third_level_second_fig_data['etime'] != etime:
                    rout_third_level_second_fig = get_third_level_second_fig_date(engine, btime, etime)
                    rout_third_level_second_fig_data['rout_third_level_second_fig'] = rout_third_level_second_fig.to_json(orient='split',date_format='iso')
                    rout_third_level_second_fig_data['btime'] = btime
                    rout_third_level_second_fig_data['etime'] = etime
                    rout_third_level_second_fig_data = json.dumps(rout_third_level_second_fig_data)
                else:
                    rout_third_level_second_fig = pd.read_json(rout_third_level_second_fig_data['rout_third_level_second_fig'], orient='split')
                    rout_third_level_second_fig_data = dash.no_update

    rout_third_level_second_fig = rout_third_level_second_fig.sort_values(['month'])
    # fig = px.line(rout_third_level_second_fig,x='month',y='num',color='生化检验类型',color_discrete_sequence=px.colors.qualitative.Dark24)
    fig = px.bar(rout_third_level_second_fig,x='month',y='num',color='生化检验类型',color_discrete_sequence=px.colors.qualitative.Dark24)
    fig.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    fig.update_yaxes(title_text="生化检验数量", )
    fig.update_xaxes(title_text="月份", )
    return fig,rout_third_level_second_fig_data
#
# # -----------------------------------------------------------------------------------------------------    四级图一   ----------------------------------------------------------------------------------------------------------------------
# 获取检查四级第一张图数据
def get_fourth_level_first_fig_date(engine,btime,etime):
    res = pd.DataFrame(columns=['问题类型', 'num', 'month'])
    # 问题类别、问题数据量统计、全数据统计
    bus_dic = {
        '检查类别缺失': f"select '检查类别缺失' as 问题类型 ,count(1) as num ,substr(EXAM_DATE,1,7) as month from EXAM where  EXAM_DATE is not null and substr(EXAM_DATE,1,7)>='{btime}' and substr(EXAM_DATE,1,7)<='{etime}' and EXAM_CLASS is null group by substr(EXAM_DATE,1,7)",
        '检查部位缺失': f"select '检验部位缺失' as 问题类型 ,count(1) as num ,substr(EXAM_DATE,1,7) as month from EXAM where  EXAM_DATE is not null and substr(EXAM_DATE,1,7)>='{btime}' and substr(EXAM_DATE,1,7)<='{etime}' and EXAM_PARA is null group by substr(EXAM_DATE,1,7)",
        '检查所见缺失': f"select '检查所见缺失' as 问题类型 ,count(1) as num ,substr(EXAM_DATE,1,7) as month from EXAM where  EXAM_DATE is not null and substr(EXAM_DATE,1,7)>='{btime}' and substr(EXAM_DATE,1,7)<='{etime}' and DESCRIPTION is null group by substr(EXAM_DATE,1,7)",
        '检查印象缺失': f"select '检查印象缺失' as 问题类型 ,count(1) as num ,substr(EXAM_DATE,1,7) as month from EXAM where  EXAM_DATE is not null and substr(EXAM_DATE,1,7)>='{btime}' and substr(EXAM_DATE,1,7)<='{etime}' and IMPRESSION is null group by substr(EXAM_DATE,1,7)",
        '检查时间在出入院时间之外': f""" select '检查时间在出入院时间之外' as 问题类型,count(1) as num ,substr(EXAM_DATE,1,7) as month from EXAM t1,overall t2 where 
                                        ( t1.EXAM_DATE is not null and t2.in_time is not null and t2.out_time is not null) 
                                        and t1.caseid = t2.caseid 
                                        and (t1.EXAM_DATE<t2.IN_TIME or t1.EXAM_DATE > t2.OUT_TIME ) 
                                        and (substr(t1.EXAM_DATE,1,7)>='{btime}' and substr(t1.EXAM_DATE,1,7)<='{etime}')
                                        group by substr(EXAM_DATE,1,7)
                                     """,
    }

    for bus in bus_dic:
        res = res.append(pd.read_sql(bus_dic[bus], con=engine))
    return res

# 四级第一张图更新
@app.callback(
    Output('exam_fourth_level_first_fig','figure'),
    Output('exam_fourth_level_first_fig_data', 'data'),

    Input('exam_fourth_level_first_fig_data', 'data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
)
def update_third_level_first_fig(exam_fourth_level_first_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if exam_fourth_level_first_fig_data is None:
            exam_fourth_level_first_fig_data = {}
            exam_fourth_level_first_fig = get_fourth_level_first_fig_date(engine, btime, etime)
            exam_fourth_level_first_fig_data['exam_fourth_level_first_fig'] = exam_fourth_level_first_fig.to_json( orient='split', date_format='iso')
            exam_fourth_level_first_fig_data['hosname'] = db_con_url['hosname']
            exam_fourth_level_first_fig_data['btime'] = btime
            exam_fourth_level_first_fig_data['etime'] = etime
            exam_fourth_level_first_fig_data = json.dumps(exam_fourth_level_first_fig_data)
        else:
            exam_fourth_level_first_fig_data = json.loads(exam_fourth_level_first_fig_data)
            if db_con_url['hosname'] != exam_fourth_level_first_fig_data['hosname']:
                exam_fourth_level_first_fig = get_fourth_level_first_fig_date(engine, btime, etime)
                exam_fourth_level_first_fig_data['exam_fourth_level_first_fig'] = exam_fourth_level_first_fig.to_json(orient='split', date_format='iso')
                exam_fourth_level_first_fig_data['hosname'] = db_con_url['hosname']
                exam_fourth_level_first_fig_data['btime'] = btime
                exam_fourth_level_first_fig_data['etime'] = etime
                exam_fourth_level_first_fig_data = json.dumps(exam_fourth_level_first_fig_data)
            else:
                if exam_fourth_level_first_fig_data['btime'] != btime or exam_fourth_level_first_fig_data['etime'] != etime:
                    exam_fourth_level_first_fig = get_fourth_level_first_fig_date(engine, btime, etime)
                    exam_fourth_level_first_fig_data['exam_fourth_level_first_fig'] = exam_fourth_level_first_fig.to_json(orient='split', date_format='iso')
                    exam_fourth_level_first_fig_data['btime'] = btime
                    exam_fourth_level_first_fig_data['etime'] = etime
                    exam_fourth_level_first_fig_data = json.dumps(exam_fourth_level_first_fig_data)
                else:
                    exam_fourth_level_first_fig = pd.read_json( exam_fourth_level_first_fig_data['exam_fourth_level_first_fig'], orient='split')
                    exam_fourth_level_first_fig_data = dash.no_update

    exam_fourth_level_first_fig = exam_fourth_level_first_fig.sort_values(['month'])
    fig = px.line(exam_fourth_level_first_fig, x="month", y="num", color='问题类型', color_discrete_sequence=px.colors.qualitative.Dark24)

    fig.update_layout(
        margin=dict(l=30, r=30, t=30, b=30),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    fig.update_yaxes(title_text="问题数量", )
    fig.update_xaxes(title_text="月份", )
    return fig,exam_fourth_level_first_fig_data

# 下载四级图一明细
@app.callback(
    Output('exam_fourth_level_first_fig_detail', 'data'),
    Input('exam_fourth_level_first_fig_data_detail_down','n_clicks'),
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
            btime = count_time['btime'][0:7]
            etime = count_time['etime'][0:7]
            bus_dic = {
                '检查类别缺失': f"select * from EXAM where  EXAM_DATE is not null and substr(EXAM_DATE,1,7)>='{btime}' and substr(EXAM_DATE,1,7)<='{etime}' and EXAM_CLASS is null  ",
                '检查部位缺失': f"select * from EXAM where  EXAM_DATE is not null and substr(EXAM_DATE,1,7)>='{btime}' and substr(EXAM_DATE,1,7)<='{etime}' and EXAM_PARA is null  ",
                '检查所见缺失': f"select * from EXAM where  EXAM_DATE is not null and substr(EXAM_DATE,1,7)>='{btime}' and substr(EXAM_DATE,1,7)<='{etime}' and DESCRIPTION is null ",
                '检查印象缺失': f"select * from EXAM where  EXAM_DATE is not null and substr(EXAM_DATE,1,7)>='{btime}' and substr(EXAM_DATE,1,7)<='{etime}' and IMPRESSION is null  ",
                '检查时间在出入院时间之外': f""" select t1.* ,t2.in_time as 入院时间,t2.out_time as 出院时间 from EXAM t1,overall t2 where 
                                                    ( t1.EXAM_DATE is not null and t2.in_time is not null and t2.out_time is not null) 
                                                    and t1.caseid = t2.caseid 
                                                    and (t1.EXAM_DATE<t2.IN_TIME or t1.EXAM_DATE > t2.OUT_TIME ) 
                                                    and (substr(t1.EXAM_DATE,1,7)>='{btime}' and substr(t1.EXAM_DATE,1,7)<='{etime}') 
                                                 """,
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
            return dcc.send_bytes(data, f'{hosName}检查问题数据明细.xlsx')
        else:
            return dash.no_update

# # -----------------------------------------------------------------------------------------------------    四级图二   ----------------------------------------------------------------------------------------------------------------------
# 获取检查四级第二张图数据
def get_fourth_level_second_fig_date(engine,btime,etime):
    res = pd.read_sql(f"select EXAM_CLASS as 检查类别,count(1) as num ,substr(EXAM_DATE,1,7) as month from EXAM where EXAM_CLASS is not null and substr(EXAM_DATE,1,7)>='{btime}' and substr(EXAM_DATE,1,7)<='{etime}' group by substr(EXAM_DATE,1,7),EXAM_CLASS ",con=engine)
    return res

# 四级第一张图更新
@app.callback(
    Output('exam_fourth_level_second_fig','figure'),
    Output('exam_fourth_level_second_fig_data', 'data'),

    Input('exam_fourth_level_second_fig_data', 'data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
)
def update_third_level_first_fig(exam_fourth_level_second_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if exam_fourth_level_second_fig_data is None:
            exam_fourth_level_second_fig_data = {}
            exam_fourth_level_second_fig = get_fourth_level_second_fig_date(engine, btime, etime)
            exam_fourth_level_second_fig_data['exam_fourth_level_second_fig'] = exam_fourth_level_second_fig.to_json( orient='split', date_format='iso')
            exam_fourth_level_second_fig_data['hosname'] = db_con_url['hosname']
            exam_fourth_level_second_fig_data['btime'] = btime
            exam_fourth_level_second_fig_data['etime'] = etime
            exam_fourth_level_second_fig_data = json.dumps(exam_fourth_level_second_fig_data)
        else:
            exam_fourth_level_second_fig_data = json.loads(exam_fourth_level_second_fig_data)
            if db_con_url['hosname'] != exam_fourth_level_second_fig_data['hosname']:
                exam_fourth_level_second_fig = get_fourth_level_second_fig_date(engine, btime, etime)
                exam_fourth_level_second_fig_data['exam_fourth_level_second_fig'] = exam_fourth_level_second_fig.to_json(orient='split', date_format='iso')
                exam_fourth_level_second_fig_data['hosname'] = db_con_url['hosname']
                exam_fourth_level_second_fig_data['btime'] = btime
                exam_fourth_level_second_fig_data['etime'] = etime
                exam_fourth_level_second_fig_data = json.dumps(exam_fourth_level_second_fig_data)
            else:
                if exam_fourth_level_second_fig_data['btime'] != btime or exam_fourth_level_second_fig_data['etime'] != etime:
                    exam_fourth_level_second_fig = get_fourth_level_second_fig_date(engine, btime, etime)
                    exam_fourth_level_second_fig_data['exam_fourth_level_second_fig'] = exam_fourth_level_second_fig.to_json(orient='split', date_format='iso')
                    exam_fourth_level_second_fig_data['btime'] = btime
                    exam_fourth_level_second_fig_data['etime'] = etime
                    exam_fourth_level_second_fig_data = json.dumps(exam_fourth_level_second_fig_data)
                else:
                    exam_fourth_level_second_fig = pd.read_json( exam_fourth_level_second_fig_data['exam_fourth_level_second_fig'], orient='split')
                    exam_fourth_level_second_fig_data = dash.no_update

    exam_fourth_level_second_fig = exam_fourth_level_second_fig.sort_values(['month'])
    fig = px.bar(exam_fourth_level_second_fig, x="month", y="num", color='检查类别', color_discrete_sequence=px.colors.qualitative.Dark24)

    fig.update_layout(
        margin=dict(l=30, r=30, t=30, b=30),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    fig.update_yaxes(title_text="检查数量", )
    fig.update_xaxes(title_text="月份", )
    return fig,exam_fourth_level_second_fig_data


# # # -----------------------------------------------------------------------------------------------------    全部下载   ----------------------------------------------------------------------------------------------------------------------
# 页面数据统计结果下载
@app.callback(
    Output("down-rout-exam-temp", "data"),
    Input("rout-exam-temp-all-count-data-down", "n_clicks"),
    Input("rout_exam_temp_first_level_first_fig_data", "data"),
    Input("rout_exam_temp_first_level_second_fig_data", "data"),
    Input("temp_second_level_first_fig_data", "data"),
    Input("rout_third_level_first_fig_data", "data"),
    Input("rout_third_level_second_fig_data", "data"),
    Input("exam_fourth_level_first_fig_data", "data"),
    Input("exam_fourth_level_second_fig_data", "data"),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    prevent_initial_call=True,
)
def get_all_count_data(n_clicks, rout_exam_temp_first_level_first_fig_data,
                                rout_exam_temp_first_level_second_fig_data,
                                temp_second_level_first_fig_data,
                                rout_third_level_first_fig_data,
                                rout_third_level_second_fig_data,
                                exam_fourth_level_first_fig_data,
                                exam_fourth_level_second_fig_data,
                       db_con_url,count_time):
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
            if rout_exam_temp_first_level_first_fig_data is not None and rout_exam_temp_first_level_second_fig_data is not None and temp_second_level_first_fig_data is not None and \
                    rout_third_level_first_fig_data is not None and rout_third_level_second_fig_data is not None and exam_fourth_level_first_fig_data is not None and exam_fourth_level_second_fig_data is not None  :
                rout_exam_temp_first_level_first_fig_data = json.loads(rout_exam_temp_first_level_first_fig_data )
                rout_exam_temp_first_level_second_fig_data = json.loads(rout_exam_temp_first_level_second_fig_data )
                temp_second_level_first_fig_data = json.loads(temp_second_level_first_fig_data )
                rout_third_level_first_fig_data = json.loads(rout_third_level_first_fig_data )
                rout_third_level_second_fig_data = json.loads(rout_third_level_second_fig_data )
                exam_fourth_level_first_fig_data = json.loads(exam_fourth_level_first_fig_data )
                exam_fourth_level_second_fig_data = json.loads(exam_fourth_level_second_fig_data )
                if rout_exam_temp_first_level_first_fig_data['hosname'] == hosName  and \
                   rout_exam_temp_first_level_second_fig_data['hosname'] == hosName and \
                   temp_second_level_first_fig_data['hosname'] == hosName and temp_second_level_first_fig_data['btime'] == btime and temp_second_level_first_fig_data['etime'] == etime and \
                   rout_third_level_first_fig_data['hosname'] == hosName and rout_third_level_first_fig_data['btime'] == btime and rout_third_level_first_fig_data['etime'] == etime and\
                   rout_third_level_second_fig_data['hosname'] == hosName and rout_third_level_second_fig_data['btime'] == btime and rout_third_level_second_fig_data['etime'] == etime and \
                   exam_fourth_level_first_fig_data['hosname'] == hosName and exam_fourth_level_first_fig_data['btime'] == btime and exam_fourth_level_first_fig_data['etime'] == etime and \
                   exam_fourth_level_second_fig_data['hosname'] == hosName and exam_fourth_level_second_fig_data['btime'] == btime and exam_fourth_level_second_fig_data['etime'] == etime :
                    rout_exam_temp_first_level_first_fig_data = pd.read_json( rout_exam_temp_first_level_first_fig_data['rout_exam_temp_first_level_first_fig'], orient='split')
                    rout_exam_temp_first_level_first_fig_data = rout_exam_temp_first_level_first_fig_data[ (rout_exam_temp_first_level_first_fig_data['month'] >= btime) & (
                                    rout_exam_temp_first_level_first_fig_data['month'] <= etime)]
                    rout_exam_temp_first_level_second_fig_data = pd.read_json( rout_exam_temp_first_level_second_fig_data['rout_exam_temp_first_level_second_fig'], orient='split')
                    temp_second_level_first_fig_data = pd.read_json( temp_second_level_first_fig_data['temp_second_level_first_fig'], orient='split')
                    rout_third_level_first_fig_data = pd.read_json( rout_third_level_first_fig_data['rout_third_level_first_fig'], orient='split')
                    rout_third_level_second_fig_data = pd.read_json( rout_third_level_second_fig_data['rout_third_level_second_fig'], orient='split')
                    exam_fourth_level_first_fig_data = pd.read_json( exam_fourth_level_first_fig_data['exam_fourth_level_first_fig'], orient='split')
                    exam_fourth_level_second_fig_data = pd.read_json( exam_fourth_level_second_fig_data['exam_fourth_level_second_fig'], orient='split')

                    output = io.BytesIO()
                    writer = pd.ExcelWriter(output, engine='xlsxwriter')
                    rout_exam_temp_first_level_first_fig_data.to_excel(writer, sheet_name='各业务每月数量统计',index=False)
                    rout_exam_temp_first_level_second_fig_data.to_excel(writer, sheet_name='各业务时间缺失数量统计',index=False)
                    temp_second_level_first_fig_data.to_excel(writer, sheet_name='体温数据每月问题数量统计',index=False)
                    rout_third_level_first_fig_data.to_excel(writer, sheet_name='生化数据每月问题数量统计',index=False)
                    rout_third_level_second_fig_data.to_excel(writer, sheet_name='生化所有检验类型每月比例统计', index=False)
                    exam_fourth_level_first_fig_data.to_excel(writer, sheet_name='检查数据每月问题数量统计', index=False)
                    exam_fourth_level_second_fig_data.to_excel(writer, sheet_name='检查所有类别每月比例统计', index=False)
                    writer.save()
                    data = output.getvalue()
                    hosName = db_con_url['hosname']
                    return dcc.send_bytes(data, f'{hosName}_{now_time}体温—检验-检查.xlsx')

                else:
                    return dash.no_update
            else:
                return dash.no_update
        else:
            return dash.no_update

