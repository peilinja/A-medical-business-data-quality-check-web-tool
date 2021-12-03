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
# 获取手术业务明细一级第一张图数据
def get_first_lev_first_fig_date(engine,btime,etime):
    res_数据时间缺失及汇总 = pd.DataFrame(columns=['问题类型', 'num', 'month' ])
    # 问题类别、问题数据量统计、全数据统计
    bus_dic = {
               '手术名称缺失': f"select '手术名称缺失' as 问题类型 ,count(1) as num ,substr(BEGINTIME,1,7) as month from OPER2 where  BEGINTIME is not null and substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and OPER_NAME is null group by substr(BEGINTIME,1,7)",
               '手术切口等级缺失': f"select '手术切口等级缺失' as 问题类型 ,count(1) as num ,substr(BEGINTIME,1,7) as month from OPER2 where  BEGINTIME is not null and substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and WOUND_GRADE is null group by substr(BEGINTIME,1,7)",
               '手术麻醉方式缺失': f"select '手术麻醉方式缺失' as 问题类型 ,count(1) as num ,substr(BEGINTIME,1,7) as month from OPER2 where  BEGINTIME is not null and substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and ASA_METHOD is null group by substr(BEGINTIME,1,7)",
               '手术开始时间大于结束时间': f"select '手术开始时间大于结束时间' as 问题类型 ,count(1) as num ,substr(BEGINTIME,1,7) as month from OPER2 where  BEGINTIME is not null and substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and BEGINTIME > ENDTIME group by substr(BEGINTIME,1,7)",
               '手术时间在出入院时间之外': f""" select '手术时间在出入院时间之外' as 问题类型,count(1) as num ,substr(BEGINTIME,1,7) as month from OPER2 t1,overall t2 where 
                                    ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                    and t1.caseid = t2.caseid 
                                    and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME ) 
                                    and (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}')
                                    group by substr(BEGINTIME,1,7)
                                 """,
               }

    for bus in bus_dic: 
        res_数据时间缺失及汇总 = res_数据时间缺失及汇总.append(pd.read_sql(bus_dic[bus],con=engine))
    return res_数据时间缺失及汇总
# 更新抗菌药物-菌检出-药敏一级图一
@app.callback(
    Output('oper2_first_level_first_fig','figure'),
    Output('oper2_first_level_first_fig_data','data'),

    Input('oper2_first_level_first_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_first_level_first_fig(oper2_first_level_first_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if oper2_first_level_first_fig_data is None:
            oper2_first_level_first_fig = get_first_lev_first_fig_date(engine, btime, etime)
            oper2_first_level_first_fig_data = {}
            oper2_first_level_first_fig_data['oper2_first_level_first_fig'] = oper2_first_level_first_fig.to_json(
                orient='split', date_format='iso')
            oper2_first_level_first_fig_data['hosname'] = db_con_url['hosname']
            oper2_first_level_first_fig_data['btime'] = btime
            oper2_first_level_first_fig_data['etime'] = etime
            oper2_first_level_first_fig_data = json.dumps(oper2_first_level_first_fig_data)
        else:
            oper2_first_level_first_fig_data = json.loads(oper2_first_level_first_fig_data)
            if db_con_url['hosname'] != oper2_first_level_first_fig_data['hosname']:
                oper2_first_level_first_fig = get_first_lev_first_fig_date(engine, btime, etime)
                oper2_first_level_first_fig_data['oper2_first_level_first_fig'] = oper2_first_level_first_fig.to_json( orient='split', date_format='iso')
                oper2_first_level_first_fig_data['hosname'] = db_con_url['hosname']
                oper2_first_level_first_fig_data['btime'] = btime
                oper2_first_level_first_fig_data['etime'] = etime
                oper2_first_level_first_fig_data = json.dumps(oper2_first_level_first_fig_data)
            else:
                if oper2_first_level_first_fig_data['btime'] != btime or oper2_first_level_first_fig_data[ 'etime'] != etime:
                    oper2_first_level_first_fig = get_first_lev_first_fig_date(engine, btime, etime)
                    oper2_first_level_first_fig_data[ 'oper2_first_level_first_fig'] = oper2_first_level_first_fig.to_json(orient='split',  date_format='iso')
                    oper2_first_level_first_fig_data['btime'] = btime
                    oper2_first_level_first_fig_data['etime'] = etime
                    oper2_first_level_first_fig_data = json.dumps(oper2_first_level_first_fig_data)
                else:
                    oper2_first_level_first_fig = pd.read_json( oper2_first_level_first_fig_data['oper2_first_level_first_fig'], orient='split')
                    oper2_first_level_first_fig_data = dash.no_update
        print(oper2_first_level_first_fig)
        oper2_first_level_first_fig = oper2_first_level_first_fig.sort_values(['month'])
        fig = px.line(oper2_first_level_first_fig,x='month',y='num',color='问题类型',color_discrete_sequence=px.colors.qualitative.Dark24 )
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
        return fig, oper2_first_level_first_fig_data


# 下载一级图一明细
@app.callback(
    Output('oper2_first_level_first_fig_detail', 'data'),
    Input('oper2_first_level_first_fig_data_detail_down','n_clicks'),
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
                '手术名称缺失': f"select * from OPER2 where  BEGINTIME is not null and substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and OPER_NAME is null  ",
                '手术切口等级缺失': f"select * from OPER2 where  BEGINTIME is not null and substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and WOUND_GRADE is null ",
                '手术麻醉方式缺失': f"select * from OPER2 where  BEGINTIME is not null and substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and ASA_METHOD is null  ",
                '手术开始时间大于结束时间': f"select * from OPER2 where  BEGINTIME is not null and substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and BEGINTIME > ENDTIME  ",
                '手术时间在出入院时间之外': f""" select t1.*,t2.in_time as 入院时间,t2.out_time as 出院时间  from OPER2 t1,overall t2 where 
                                                ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                                and t1.caseid = t2.caseid 
                                                and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME ) 
                                                and (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}')
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
            return dcc.send_bytes(data, f'{hosName}手术问题数据明细.xlsx')
        else:
            return dash.no_update


# # -----------------------------------------------------------------------------------------------------    一级图二   ----------------------------------------------------------------------------------------------------------------------
# # 获取手术级第二张图数据
def get_first_lev_second_fig_date(engine,btime,etime):
    res = pd.read_sql(f"select substr(begintime,1,7) as 月份,WOUND_GRADE as 手术切口等级,count(1) as num from oper2 where substr(begintime,1,7)>='{btime}' and substr(begintime,1,7)<='{etime}' and WOUND_GRADE is not null group by substr(begintime,1,7),WOUND_GRADE",con=engine)
    return res
# 更新一级图二
@app.callback(
    Output('oper2_first_level_second_fig','figure'),
    Output('oper2_first_level_second_fig_data','data'),

    Input('oper2_first_level_second_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
)
def update_first_level_second_fig(oper2_first_level_second_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if oper2_first_level_second_fig_data is None:
            oper2_first_level_second_fig_data = {}
            oper2_first_level_second_fig = get_first_lev_second_fig_date(engine, btime, etime)
            oper2_first_level_second_fig_data['oper2_first_level_second_fig'] = oper2_first_level_second_fig.to_json( orient='split', date_format='iso')
            oper2_first_level_second_fig_data['hosname'] = db_con_url['hosname']
            oper2_first_level_second_fig_data['btime'] = btime
            oper2_first_level_second_fig_data['etime'] = etime
            oper2_first_level_second_fig_data = json.dumps(oper2_first_level_second_fig_data)
        else:
            oper2_first_level_second_fig_data = json.loads(oper2_first_level_second_fig_data)
            if db_con_url['hosname'] != oper2_first_level_second_fig_data['hosname']:
                oper2_first_level_second_fig = get_first_lev_second_fig_date(engine, btime, etime)
                oper2_first_level_second_fig_data['oper2_first_level_second_fig'] = oper2_first_level_second_fig.to_json( orient='split', date_format='iso')
                oper2_first_level_second_fig_data['hosname'] = db_con_url['hosname']
                oper2_first_level_second_fig_data['btime'] = btime
                oper2_first_level_second_fig_data['etime'] = etime
                oper2_first_level_second_fig_data = json.dumps(oper2_first_level_second_fig_data)
            else:
                if oper2_first_level_second_fig_data['btime'] != btime or oper2_first_level_second_fig_data[ 'etime'] != etime:
                    oper2_first_level_second_fig = get_first_lev_second_fig_date(engine, btime, etime)
                    oper2_first_level_second_fig_data[ 'oper2_first_level_second_fig'] = oper2_first_level_second_fig.to_json( orient='split', date_format='iso')
                    oper2_first_level_second_fig_data['btime'] = btime
                    oper2_first_level_second_fig_data['etime'] = etime
                    oper2_first_level_second_fig_data = json.dumps(oper2_first_level_second_fig_data)
                else:
                    oper2_first_level_second_fig = pd.read_json( oper2_first_level_second_fig_data['oper2_first_level_second_fig'], orient='split')
                    oper2_first_level_second_fig_data = dash.no_update

    oper2_first_level_second_fig = oper2_first_level_second_fig.sort_values(['月份'])
    fig = px.bar(oper2_first_level_second_fig, x="月份", y="num", color='手术切口等级',
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
    fig.update_yaxes(title_text="手术数量", )
    fig.update_xaxes(title_text="月份", )
    return fig, oper2_first_level_second_fig_data
#
# # # -----------------------------------------------------------------------------------------------------    二级图一   ----------------------------------------------------------------------------------------------------------------------
# 获取抗菌药物二级第一张图数据
def get_second_lev_first_fig_date(engine,btime,etime):

    res = pd.DataFrame(columns=['手术类型', 'num', 'month' ])

    bus_dic = {'手术':  f" select 'oper2手术' as 手术类型,count(1) as num, substr(begintime,1,7) as month from oper2 where substr(begintime,1,7)>='{btime}' and substr(begintime,1,7)<='{etime}' and WOUND_GRADE is not null group by substr(begintime,1,7)  ",
               '手术来源': f" select 'oper2raw'||source as 手术类型,count(1) as num, substr(begintime,1,7) as month from oper2raw where substr(begintime,1,7)>='{btime}' and substr(begintime,1,7)<='{etime}' and WOUND_GRADE is not null group by substr(begintime,1,7) ,source ",
               }
    for bus in bus_dic:
        res = res.append(pd.read_sql(bus_dic[bus],con=engine))
    return res

# 更新二级图一
@app.callback(
    Output('oper2_second_level_first_fig','figure'),
    Output('oper2_second_level_first_fig_data','data'),

    Input('oper2_second_level_first_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_first_level_third_fig(oper2_second_level_first_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if oper2_second_level_first_fig_data is None:
            oper2_second_level_first_fig = get_second_lev_first_fig_date(engine, btime, etime)
            oper2_second_level_first_fig_data={}
            oper2_second_level_first_fig_data['oper2_second_level_first_fig'] = oper2_second_level_first_fig.to_json(orient='split', date_format='iso')
            oper2_second_level_first_fig_data['hosname'] = db_con_url['hosname']
            oper2_second_level_first_fig_data['btime'] = btime
            oper2_second_level_first_fig_data['etime'] = etime
            oper2_second_level_first_fig_data = json.dumps(oper2_second_level_first_fig_data)
        else:
            oper2_second_level_first_fig_data = json.loads(oper2_second_level_first_fig_data)
            if db_con_url['hosname'] != oper2_second_level_first_fig_data['hosname']:
                oper2_second_level_first_fig = get_second_lev_first_fig_date(engine, btime, etime)
                oper2_second_level_first_fig_data['oper2_second_level_first_fig'] = oper2_second_level_first_fig.to_json(orient='split',date_format='iso')
                oper2_second_level_first_fig_data['hosname'] = db_con_url['hosname']
                oper2_second_level_first_fig_data['btime'] = btime
                oper2_second_level_first_fig_data['etime'] = etime
                oper2_second_level_first_fig_data = json.dumps(oper2_second_level_first_fig_data)
            else:
                if oper2_second_level_first_fig_data['btime'] != btime or oper2_second_level_first_fig_data['etime'] != etime:
                    oper2_second_level_first_fig = get_second_lev_first_fig_date(engine, btime, etime)
                    oper2_second_level_first_fig_data['oper2_second_level_first_fig'] = oper2_second_level_first_fig.to_json(orient='split',date_format='iso')
                    oper2_second_level_first_fig_data['btime'] = btime
                    oper2_second_level_first_fig_data['etime'] = etime
                    oper2_second_level_first_fig_data = json.dumps(oper2_second_level_first_fig_data)
                else:
                    oper2_second_level_first_fig = pd.read_json(oper2_second_level_first_fig_data['oper2_second_level_first_fig'], orient='split')
                    oper2_second_level_first_fig_data = dash.no_update

    oper2 = oper2_second_level_first_fig[oper2_second_level_first_fig['手术类型'] == 'oper2手术']
    oper2 = oper2.sort_values(['month'])
    oper2raw = oper2_second_level_first_fig[oper2_second_level_first_fig['手术类型'] != 'oper2手术']
    oper2raw = oper2raw.sort_values(['month'])
    fig = go.Figure()

    color_index = 0
    for bus in oper2raw['手术类型'].drop_duplicates():
        temp = oper2raw[oper2raw['手术类型'] == bus].reset_index(drop=True)
        fig.add_trace(go.Bar(x=temp['month'], y=temp['num'], name=bus,marker_color=px.colors.qualitative.Dark24[color_index]))
        color_index = color_index + 1
        if color_index == 3:
            color_index = color_index + 1
    fig.update_layout(barmode='relative')
    fig.add_trace(go.Scatter(x=oper2['month'], y=oper2['num'], name='oper2手术',marker_color=px.colors.qualitative.Dark24[3]))
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
    fig.update_yaxes(title_text="手术数量")
    fig.update_xaxes(title_text="月份")
    return fig,oper2_second_level_first_fig_data

# # # -----------------------------------------------------------------------------------------------------    二级图二   ----------------------------------------------------------------------------------------------------------------------
# 获取手术二级第二张图数据
def get_second_level_second_fig_date(engine,btime,etime):
    res = pd.DataFrame(columns=['手术类型', 'num', 'month'])

    bus_dic = {
        '手术': f" select '手术' as 手术类型,count(1) as num , substr(begintime,1,7) as month from oper2 where not regexp_like(oper_name,'造影|支架|球囊扩张|内膜剥脱|经导管|经皮|胃镜|肠镜|引流|气管切开|中心静脉|锁骨下静脉|导尿') and substr(begintime,1,7)>='{btime}' and substr(begintime,1,7)<='{etime}' group by substr(begintime,1,7)  ",
        '穿刺手术': f" select '穿刺手术' as 手术类型,count(1) as num , substr(begintime,1,7) as month from oper2 where regexp_like(oper_name,'造影|支架|球囊扩张|内膜剥脱|经导管|经皮') and substr(begintime,1,7)>='{btime}' and substr(begintime,1,7)<='{etime}' group by substr(begintime,1,7)  ",
        '胃肠镜': f" select '胃肠镜' as 手术类型,count(1) as num , substr(begintime,1,7) as month from oper2 where regexp_like(oper_name,'胃镜|肠镜') and substr(begintime,1,7)>='{btime}' and substr(begintime,1,7)<='{etime}' group by substr(begintime,1,7)  ",
        '引流': f" select '引流' as 手术类型,count(1) as num , substr(begintime,1,7) as month from oper2 where regexp_like(oper_name,'引流') and substr(begintime,1,7)>='{btime}' and substr(begintime,1,7)<='{etime}' group by substr(begintime,1,7)  ",
        '三管': f" select '三管' as 手术类型,count(1) as num , substr(begintime,1,7) as month from oper2 where regexp_like(oper_name,'气管切开|中心静脉|锁骨下静脉|导尿') and substr(begintime,1,7)>='{btime}' and substr(begintime,1,7)<='{etime}' group by substr(begintime,1,7)  ",

        }
    for bus in bus_dic:
        res = res.append(pd.read_sql(bus_dic[bus], con=engine))
    return res


# 更新二级图
@app.callback(
    Output('oper2_second_level_second_fig','figure'),
    Output('oper2_second_level_second_fig_data','data'),

    Input('oper2_second_level_second_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_second_level_fig(oper2_second_level_second_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if oper2_second_level_second_fig_data is None:
            oper2_second_level_second_fig_data = {}
            oper2_second_level_second_fig = get_second_level_second_fig_date(engine, btime, etime)
            oper2_second_level_second_fig_data['oper2_second_level_second_fig'] = oper2_second_level_second_fig.to_json(orient='split', date_format='iso')
            oper2_second_level_second_fig_data['hosname'] = db_con_url['hosname']
            oper2_second_level_second_fig_data['btime'] = btime
            oper2_second_level_second_fig_data['etime'] = etime
            oper2_second_level_second_fig_data = json.dumps(oper2_second_level_second_fig_data)
        else:
            oper2_second_level_second_fig_data = json.loads(oper2_second_level_second_fig_data)
            if db_con_url['hosname'] != oper2_second_level_second_fig_data['hosname']:
                oper2_second_level_second_fig = get_second_level_second_fig_date(engine, btime, etime)
                oper2_second_level_second_fig_data['oper2_second_level_second_fig'] = oper2_second_level_second_fig.to_json(orient='split',date_format='iso')
                oper2_second_level_second_fig_data['hosname'] = db_con_url['hosname']
                oper2_second_level_second_fig_data['btime'] = btime
                oper2_second_level_second_fig_data['etime'] = etime
                oper2_second_level_second_fig_data = json.dumps(oper2_second_level_second_fig_data)
            else:
                if oper2_second_level_second_fig_data['btime'] != btime or oper2_second_level_second_fig_data['etime'] != etime:
                    oper2_second_level_second_fig = get_second_level_second_fig_date(engine, btime, etime)
                    oper2_second_level_second_fig_data['oper2_second_level_second_fig'] = oper2_second_level_second_fig.to_json(orient='split',date_format='iso')
                    oper2_second_level_second_fig_data['btime'] = btime
                    oper2_second_level_second_fig_data['etime'] = etime
                    oper2_second_level_second_fig_data = json.dumps(oper2_second_level_second_fig_data)
                else:
                    oper2_second_level_second_fig = pd.read_json(oper2_second_level_second_fig_data['oper2_second_level_second_fig'], orient='split')
                    oper2_second_level_second_fig_data = dash.no_update

    oper2_second_level_second_fig = oper2_second_level_second_fig.sort_values(['month'])
    fig = px.bar(oper2_second_level_second_fig,x='month',y='num',color='手术类型',color_discrete_sequence=px.colors.qualitative.Dark24)
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
    fig.update_yaxes(title_text="手术数量", )
    fig.update_xaxes(title_text="月份", )
    return fig,oper2_second_level_second_fig_data
#
# # -----------------------------------------------------------------------------------------------------    二级图三   ----------------------------------------------------------------------------------------------------------------------
# 获取抗菌药物二级第三张图数据
def get_second_level_third_fig_date(engine,btime,etime):
    res = pd.read_sql(
        f" select ASA_METHOD as 手术麻醉方式,count(1) as num , substr(BEGINTIME,1,7) as 月份 from oper2 where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and ASA_METHOD is not null group by substr(BEGINTIME,1,7),ASA_METHOD ",
        con=engine)

    return res

# 三级第一张图更新
@app.callback(
    Output('oper2_second_level_third_fig','figure'),
    Output('oper2_second_level_third_fig_data', 'data'),

    Input('oper2_second_level_third_fig_data', 'data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
)
def update_third_level_first_fig(oper2_second_level_third_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if oper2_second_level_third_fig_data is None:
            oper2_second_level_third_fig_data = {}
            oper2_second_level_third_fig = get_second_level_third_fig_date(engine, btime, etime)
            oper2_second_level_third_fig_data['oper2_second_level_third_fig'] = oper2_second_level_third_fig.to_json( orient='split', date_format='iso')
            oper2_second_level_third_fig_data['hosname'] = db_con_url['hosname']
            oper2_second_level_third_fig_data['btime'] = btime
            oper2_second_level_third_fig_data['etime'] = etime
            oper2_second_level_third_fig_data = json.dumps(oper2_second_level_third_fig_data)
        else:
            oper2_second_level_third_fig_data = json.loads(oper2_second_level_third_fig_data)
            if db_con_url['hosname'] != oper2_second_level_third_fig_data['hosname']:
                oper2_second_level_third_fig = get_second_level_third_fig_date(engine, btime, etime)
                oper2_second_level_third_fig_data['oper2_second_level_third_fig'] = oper2_second_level_third_fig.to_json(orient='split', date_format='iso')
                oper2_second_level_third_fig_data['hosname'] = db_con_url['hosname']
                oper2_second_level_third_fig_data['btime'] = btime
                oper2_second_level_third_fig_data['etime'] = etime
                oper2_second_level_third_fig_data = json.dumps(oper2_second_level_third_fig_data)
            else:
                if oper2_second_level_third_fig_data['btime'] != btime or oper2_second_level_third_fig_data['etime'] != etime:
                    oper2_second_level_third_fig = get_second_level_third_fig_date(engine, btime, etime)
                    oper2_second_level_third_fig_data['oper2_second_level_third_fig'] = oper2_second_level_third_fig.to_json(orient='split', date_format='iso')
                    oper2_second_level_third_fig_data['btime'] = btime
                    oper2_second_level_third_fig_data['etime'] = etime
                    oper2_second_level_third_fig_data = json.dumps(oper2_second_level_third_fig_data)
                else:
                    oper2_second_level_third_fig = pd.read_json( oper2_second_level_third_fig_data['oper2_second_level_third_fig'], orient='split')
                    oper2_second_level_third_fig_data = dash.no_update

    oper2_second_level_third_fig = oper2_second_level_third_fig.sort_values(['月份'])
    fig = px.bar(oper2_second_level_third_fig, x="月份", y="num", color='手术麻醉方式', color_discrete_sequence=px.colors.qualitative.Dark24)

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
    fig.update_yaxes(title_text="手术数量", )
    fig.update_xaxes(title_text="月份", )
    return fig,oper2_second_level_third_fig_data




# # # -----------------------------------------------------------------------------------------------------    全部下载   ----------------------------------------------------------------------------------------------------------------------
# 页面数据统计结果下载
@app.callback(
    Output("down-oper2", "data"),
    Input("oper2-all-count-data-down", "n_clicks"),
    Input("oper2_first_level_first_fig_data", "data"),
    Input("oper2_first_level_second_fig_data", "data"),
    Input("oper2_second_level_first_fig_data", "data"),
    Input("oper2_second_level_second_fig_data", "data"),
    Input("oper2_second_level_third_fig_data", "data"),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    prevent_initial_call=True,
)
def get_all_count_data(n_clicks, oper2_first_level_first_fig_data,
                                oper2_first_level_second_fig_data,
                                oper2_second_level_first_fig_data,
                                oper2_second_level_second_fig_data,
                                oper2_second_level_third_fig_data,
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
            if oper2_first_level_first_fig_data is not None and oper2_first_level_second_fig_data is not None and oper2_second_level_first_fig_data is not None and \
                    oper2_second_level_second_fig_data is not None and oper2_second_level_third_fig_data is not None  :
                oper2_first_level_first_fig_data = json.loads(oper2_first_level_first_fig_data )
                oper2_first_level_second_fig_data = json.loads(oper2_first_level_second_fig_data )
                oper2_second_level_first_fig_data = json.loads(oper2_second_level_first_fig_data )
                oper2_second_level_second_fig_data = json.loads(oper2_second_level_second_fig_data )
                oper2_second_level_third_fig_data = json.loads(oper2_second_level_third_fig_data )
                if oper2_first_level_first_fig_data['hosname'] == hosName  and \
                   oper2_first_level_second_fig_data['hosname'] == hosName and \
                   oper2_second_level_first_fig_data['hosname'] == hosName and \
                   oper2_second_level_second_fig_data['hosname'] == hosName and \
                   oper2_second_level_third_fig_data['hosname'] == hosName :
                    oper2_first_level_first_fig = pd.read_json( oper2_first_level_first_fig_data['oper2_first_level_first_fig'], orient='split')
                    oper2_first_level_second_fig = pd.read_json( oper2_first_level_second_fig_data['oper2_first_level_second_fig'], orient='split')
                    oper2_second_level_first_fig = pd.read_json( oper2_second_level_first_fig_data['oper2_second_level_first_fig'], orient='split')
                    oper2_second_level_second_fig = pd.read_json( oper2_second_level_second_fig_data['oper2_second_level_second_fig'], orient='split')
                    oper2_second_level_third_fig = pd.read_json( oper2_second_level_third_fig_data['oper2_second_level_third_fig'], orient='split')

                    output = io.BytesIO()
                    writer = pd.ExcelWriter(output, engine='xlsxwriter')
                    oper2_first_level_first_fig.to_excel(writer, sheet_name='手术每月问题数据',index=False)
                    oper2_first_level_second_fig.to_excel(writer, sheet_name='手术每月切口等级数量占比',index=False)
                    oper2_second_level_first_fig.to_excel(writer, sheet_name='手术每月手术来源数据',index=False)
                    oper2_second_level_second_fig.to_excel(writer, sheet_name='手术类别每月占比',index=False)
                    oper2_second_level_third_fig.to_excel(writer, sheet_name='手术麻醉方式每月占比', index=False)
                    writer.save()
                    data = output.getvalue()
                    hosName = db_con_url['hosname']
                    return dcc.send_bytes(data, f'{hosName}_{now_time}手术.xlsx')

                else:
                    return dash.no_update
            else:
                return dash.no_update
        else:
            return dash.no_update

