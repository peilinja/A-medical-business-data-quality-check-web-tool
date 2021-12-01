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


def discriminated_antis(all_antis):
    try:
        df_抗菌药物 = pd.read_csv(r'./抗菌药物字典.csv')
    except:
        df_抗菌药物 = pd.read_csv(r'./抗菌药物字典.csv', encoding='gbk')
    def isanti(x):
        df_抗菌药物['药品'] = x.抗菌药物
        df1 = df_抗菌药物[df_抗菌药物['规则等级']==1]
        if x.抗菌药物 in list(df1['匹配规则'].values):
            return df1[df1['匹配规则']==x.抗菌药物].reset_index(drop=True).loc[0]['抗菌药物通用名']
        else:
            df2 = df_抗菌药物[df_抗菌药物['规则等级']==2]
            df2['是否匹配'] = df2.apply(lambda y: y.抗菌药物通用名 if re.match(y.匹配规则, y.药品) else np.nan, axis=1)
            df2['匹配长度'] = df2.apply(lambda y: 0 if pd.isnull(y.是否匹配) else len( y.匹配规则 ), axis=1)
            if df2[~df2['是否匹配'].isnull()].shape[0]==0:
                df3 = df_抗菌药物[df_抗菌药物['规则等级']==3]
                df3['是否匹配'] = df3.apply(lambda y: y.抗菌药物通用名 if re.match(y.匹配规则, y.药品) else np.nan, axis=1)
                df3['匹配长度'] = df3.apply(lambda y: 0 if pd.isnull(y.是否匹配) else len( y.匹配规则 ), axis=1)
                if df3[~df3['是否匹配'].isnull()].shape[0]==0:
                    df4 = df_抗菌药物[df_抗菌药物['规则等级']==4]
                    df4['是否匹配'] = df4.apply(lambda y: y.抗菌药物通用名 if re.match(y.匹配规则, y.药品) else np.nan, axis=1)
                    df4['匹配长度'] = df4.apply(lambda y: 0 if pd.isnull(y.是否匹配) else len( y.匹配规则 ), axis=1)
                    if df4[~df4['是否匹配'].isnull()].shape[0]==0:
                        return np.nan
                    else:
                        return df4[~df4['是否匹配'].isnull()][['抗菌药物通用名','匹配长度']].drop_duplicates().sort_values(by=['匹配长度'], ascending=False).reset_index(drop=True)['抗菌药物通用名'].loc[0]#返回正则匹配成功且匹配长度最长
                else:
                    return df3[~df3['是否匹配'].isnull()][['抗菌药物通用名','匹配长度']].drop_duplicates().sort_values(by=['匹配长度'], ascending=False).reset_index(drop=True)['抗菌药物通用名'].loc[0]#返回正则匹配成功且匹配长度最长
            else:
                return df2[~df2['是否匹配'].isnull()][['抗菌药物通用名','匹配长度']].drop_duplicates().sort_values(by=['匹配长度'], ascending=False).reset_index(drop=True)['抗菌药物通用名'].loc[0]#返回正则匹配成功且匹配长度最长
    all_antis['抗菌药物通用名'] = all_antis.apply(isanti, axis=1)
    return all_antis




# -----------------------------------------------------------------------------------------------------    一级图一   ----------------------------------------------------------------------------------------------------------------------
# 获取抗菌药物-菌检出-药敏一级第一张图数据
def get_first_lev_first_fig_date(engine):
    res_数据时间缺失及汇总 = pd.DataFrame(columns=['业务类型', 'num', 'month' ])
    # 问题类别、问题数据量统计、全数据统计
    bus_dic = {
               '给药': "select '给药' as 业务类型 ,count(1) as num ,substr(BEGINTIME,1,7) as month from ANTIBIOTICS where  BEGINTIME is not null group by substr(BEGINTIME,1,7)",
               '菌检出': " select '菌检出' as 业务类型 , count(1) as num ,substr(REQUESTTIME,1,7) as month from BACTERIA where  REQUESTTIME is not null group by substr(REQUESTTIME,1,7) ",
               '药敏': " select '药敏' as 业务类型 , count(1) as num ,substr(REQUESTTIME,1,7) as month from DRUGSUSCEPTIBILITY where  REQUESTTIME is not null group by substr(REQUESTTIME,1,7) ",
               }

    for bus in bus_dic: 
        res_数据时间缺失及汇总 = res_数据时间缺失及汇总.append(pd.read_sql(bus_dic[bus],con=engine)) 
        print('抗菌药物-菌检出-药敏一级图一',bus)
    return res_数据时间缺失及汇总
# 更新抗菌药物-菌检出-药敏一级图一
@app.callback(
    Output('anti_bar_drug_first_level_first_fig','figure'),
    Output('anti_bar_drug_first_level_first_fig_data','data'),

    Input('anti_bar_drug_first_level_first_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_first_level_first_fig(anti_bar_drug_first_level_first_fig_data,db_con_url,count_time):
    if db_con_url is None :
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        engine = create_engine(db_con_url['db'])
        if anti_bar_drug_first_level_first_fig_data is None:
            anti_bar_drug_first_level_first_fig_data = {}
            anti_bar_drug_first_level_first_fig = get_first_lev_first_fig_date(engine)
            anti_bar_drug_first_level_first_fig_data['anti_bar_drug_first_level_first_fig'] = anti_bar_drug_first_level_first_fig.to_json(orient='split', date_format='iso')
            anti_bar_drug_first_level_first_fig_data['hosname'] = db_con_url['hosname']
            anti_bar_drug_first_level_first_fig_data['btime'] = btime
            anti_bar_drug_first_level_first_fig_data['etime'] = etime
            anti_bar_drug_first_level_first_fig_data = json.dumps(anti_bar_drug_first_level_first_fig_data)
        else:
            anti_bar_drug_first_level_first_fig_data = json.loads(anti_bar_drug_first_level_first_fig_data)
            if db_con_url['hosname'] != anti_bar_drug_first_level_first_fig_data['hosname']:
                anti_bar_drug_first_level_first_fig = get_first_lev_first_fig_date(engine)
                anti_bar_drug_first_level_first_fig_data['anti_bar_drug_first_level_first_fig'] = anti_bar_drug_first_level_first_fig.to_json(orient='split',date_format='iso')
                anti_bar_drug_first_level_first_fig_data['hosname'] = db_con_url['hosname']
                anti_bar_drug_first_level_first_fig_data = json.dumps(anti_bar_drug_first_level_first_fig_data)
            else:
                anti_bar_drug_first_level_first_fig = pd.read_json(anti_bar_drug_first_level_first_fig_data['anti_bar_drug_first_level_first_fig'], orient='split')
                anti_bar_drug_first_level_first_fig_data = dash.no_update
            #
        anti_bar_drug_first_level_first_fig = anti_bar_drug_first_level_first_fig[(anti_bar_drug_first_level_first_fig['month']>=btime) & (anti_bar_drug_first_level_first_fig['month']<=etime)]
        anti_bar_drug_first_level_first_fig = anti_bar_drug_first_level_first_fig.sort_values(['month','业务类型'])
        fig1 = px.line(anti_bar_drug_first_level_first_fig, x='month', y='num', color='业务类型',
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
        return fig1,anti_bar_drug_first_level_first_fig_data

# # -----------------------------------------------------------------------------------------------------    一级图二   ----------------------------------------------------------------------------------------------------------------------
# # 获取抗菌药物-菌检出-药敏一级第二张图数据
def get_first_lev_second_fig_date(engine,btime,etime):
    res_数据关键字缺失及汇总 = pd.DataFrame(columns=['业务类型', '科室', '科室名称', 'num'])
    bus_dic = {'8种耐药菌检出': f""" select '8种耐药菌检出' as 业务类型, t1.dept as 科室,t2.label as 科室名称,t1.num from
                                (select dept,count(1) as num from BACTERIA where BACTERIA in ('大肠埃希菌', '鲍曼不动杆菌', '肺炎克雷伯菌', '金黄色葡萄球菌', '铜绿假单胞菌', '屎肠球菌', '粪肠球菌') 
                                and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and dept is not null
                                group by dept) t1,s_departments t2
                                where t1.dept=t2.code(+) order by t1.num desc
                            """,
               "限制级特殊级抗菌药物使用" : f"""select '限制级特殊级抗菌药物使用' as 业务类型,t1.dept as 科室,t2.label as 科室名称,t1.num from
                                        (select dept,count(1) as num from ANTIBIOTICS where ALEVEL in ('限制类', '特殊类') 
                                        and substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and dept is not null
                                        group by dept) t1,s_departments t2
                                        where t1.dept=t2.code(+) order by t1.num desc
                                        """,
               '药敏结果为耐药': f""" select '药敏结果为耐药' as 业务类型,t1.dept as 科室,t2.label as 科室名称,t1.num from
                                    (select dept,count(1) as num from DRUGSUSCEPTIBILITY where  SUSCEPTIBILITY like '%耐药%'
                                    and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' and dept is not null
                                    group by dept) t1,s_departments t2
                                    where t1.dept=t2.code(+) order by t1.num desc
                                """
               }
    for bus in bus_dic:
        temp = pd.read_sql(bus_dic[bus],con=engine)
        temp = temp[0:8]
        res_数据关键字缺失及汇总 = res_数据关键字缺失及汇总.append(temp)
         
    return res_数据关键字缺失及汇总
# 更新一级图二
@app.callback(
    Output('anti_bar_drug_first_level_second_fig','figure'),
    Output('anti_bar_drug_first_level_second_fig_data','data'),
    # Output('rank_month_choice','min'),
    # Output('rank_month_choice','max'),
    # Output('rank_month_choice','value'),
    # Output('rank_month_choice','marks'),

    Input('anti_bar_drug_first_level_second_fig_data','data'),
    # Input('rank_month_choice','value'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # Input('rank_month_choice','marks'),
    # prevent_initial_call=True
)
# def update_first_level_second_fig(anti_bar_drug_first_level_second_fig_data,rank_month_choice,db_con_url,count_time,marks):
def update_first_level_second_fig(anti_bar_drug_first_level_second_fig_data,db_con_url,count_time):


    # def unixTimeMillis(dt):
    #     return int(time.mktime(dt.timetuple()))
    #
    # def unixToDatetime(unix):
    #     return pd.to_datetime(unix, unit='s')
    #
    # def getMarks(start, end, Nth=100):
    #     result = {}
    #     for i, date in enumerate(daterange):
    #         result[unixTimeMillis(date)] = str(date.strftime('%Y-%m'))


    if db_con_url is None :
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]

        min = dash.no_update
        max = dash.no_update
        value = dash.no_update
        marks = dash.no_update

        if anti_bar_drug_first_level_second_fig_data is None:
            anti_bar_drug_first_level_second_fig_data = {}
            first_level_second_fig_data = get_first_lev_second_fig_date(engine,btime,etime)
            anti_bar_drug_first_level_second_fig_data['first_level_second_fig_data'] = first_level_second_fig_data.to_json(orient='split', date_format='iso')
            anti_bar_drug_first_level_second_fig_data['hosname'] = db_con_url['hosname']
            anti_bar_drug_first_level_second_fig_data['btime'] = btime
            anti_bar_drug_first_level_second_fig_data['etime'] = etime
            anti_bar_drug_first_level_second_fig_data = json.dumps(anti_bar_drug_first_level_second_fig_data)

            # end_date = datetime(int(etime[0:4]), int(etime[5:7]), 1)
            # start_date = datetime(int(btime[0:4]), int(btime[5:7]), 1)
            # daterange = pd.date_range(start=btime+'-01', periods=((end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)), freq='M')
            # min = unixTimeMillis(daterange.min())
            # max = unixTimeMillis(daterange.max())
            # value = [unixTimeMillis(daterange.min()), unixTimeMillis(daterange.max())]
            # marks = getMarks(daterange.min(), daterange.max())

        else:
            anti_bar_drug_first_level_second_fig_data = json.loads(anti_bar_drug_first_level_second_fig_data)
            if db_con_url['hosname'] != anti_bar_drug_first_level_second_fig_data['hosname']:
                first_level_second_fig_data = get_first_lev_second_fig_date(engine, btime, etime)
                anti_bar_drug_first_level_second_fig_data['first_level_second_fig_data'] = first_level_second_fig_data.to_json(orient='split',date_format='iso')
                anti_bar_drug_first_level_second_fig_data['hosname'] = db_con_url['hosname']
                anti_bar_drug_first_level_second_fig_data['btime'] = btime
                anti_bar_drug_first_level_second_fig_data['etime'] = etime
                anti_bar_drug_first_level_second_fig_data = json.dumps( anti_bar_drug_first_level_second_fig_data)

                # end_date = datetime(int(etime[0:4]), int(etime[5:7]), 1)
                # start_date = datetime(int(btime[0:4]), int(btime[5:7]), 1)
                # daterange = pd.date_range(start=btime + '-01', periods=( (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)), freq='M')
                # min = unixTimeMillis(daterange.min())
                # max = unixTimeMillis(daterange.max())
                # value = [unixTimeMillis(daterange.min()), unixTimeMillis(daterange.max())]
                # print(value)
                # marks = getMarks(daterange.min(), daterange.max())
            else:
                if anti_bar_drug_first_level_second_fig_data['btime'] != btime or anti_bar_drug_first_level_second_fig_data['etime'] != etime:
                # if rank_month_choice is not None and len(rank_month_choice)>0:
                #     print(rank_month_choice)
                #     btime1 = time.gmtime(rank_month_choice[0])
                #     etime1 = time.gmtime(rank_month_choice[1])
                #     btime = f"{btime1.tm_year}-0{btime1.tm_mon}" if btime1.tm_mon<10 else f"{btime1.tm_year}-{btime1.tm_mon}"
                #     etime = f"{etime1.tm_year}-0{etime1.tm_mon}" if etime1.tm_mon<10 else f"{etime1.tm_year}-{etime1.tm_mon}"
                #     print(btime,etime)
                    first_level_second_fig_data = get_first_lev_second_fig_date(engine, btime, etime)
                    anti_bar_drug_first_level_second_fig_data[ 'first_level_second_fig_data'] = first_level_second_fig_data.to_json(orient='split', date_format='iso')
                    anti_bar_drug_first_level_second_fig_data['btime'] = btime
                    anti_bar_drug_first_level_second_fig_data['etime'] = etime
                    anti_bar_drug_first_level_second_fig_data = json.dumps(anti_bar_drug_first_level_second_fig_data)
                else:
                    first_level_second_fig_data = pd.read_json(anti_bar_drug_first_level_second_fig_data['first_level_second_fig_data'], orient='split')
                    anti_bar_drug_first_level_second_fig_data = dash.no_update

    # print("一级第二张图数据：")
    # print(rank_month_choice)
    # print(marks)
    bar = first_level_second_fig_data[first_level_second_fig_data['业务类型']=='8种耐药菌检出']
    anti = first_level_second_fig_data[first_level_second_fig_data['业务类型']=='限制级特殊级抗菌药物使用']
    drug = first_level_second_fig_data[first_level_second_fig_data['业务类型']=='药敏结果为耐药']

    bar = bar.sort_values(['num'], ascending=True)
    anti = anti.sort_values(['num'], ascending=True)
    drug = drug.sort_values(['num'], ascending=True)

    fig  = make_subplots(rows=1,cols=3)
    fig.add_trace(
        go.Bar(x=anti['num'], y=anti['科室名称'], orientation='h', name='给药', marker_color=px.colors.qualitative.Dark24[1]),
        row=1, col=2
    )
    fig.add_trace(
        go.Bar(x=drug['num'], y=drug['科室名称'], orientation='h', name='药敏', marker_color=px.colors.qualitative.Dark24[2]),
        row=1, col=3,
    )
    fig.add_trace(
        go.Bar(x=bar['num'],y=bar['科室名称'],orientation='h',name='菌检出', marker_color=px.colors.qualitative.Dark24[0]),
        row=1,col=1
    )
    # 设置水平图例及位置
    fig.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ))

    return fig,anti_bar_drug_first_level_second_fig_data
    # return fig,anti_bar_drug_first_level_second_fig_data,min ,max ,value ,marks

# # -----------------------------------------------------------------------------------------------------    二级图一   ----------------------------------------------------------------------------------------------------------------------
# 获取抗菌药物二级第一张图数据
def get_second_lev_first_fig_date(engine,btime,etime):
    
    res_数据科室信息缺失及汇总 = pd.DataFrame(columns=['业务类型', 'num', 'month' ])

    bus_dic = {'用药目的':  f" select '用药目的缺失' as 业务类型,count(1) as num ,substr(BEGINTIME,1,7) as month  from ANTIBIOTICS where (substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}') group by substr(BEGINTIME,1,7) ",
               '药物等级': f" select '药物等级缺失' as 业务类型,count(1) as num ,substr(BEGINTIME,1,7) as month from ANTIBIOTICS t1 where (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}') group by substr(BEGINTIME,1,7) ",
               '医嘱开始时间大于结束时间': f" select '医嘱开始时间大于结束时间' as 业务类型,count(1) as num ,substr(BEGINTIME,1,7) as month  from ANTIBIOTICS t1 where (substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}') and BEGINTIME is not null and ENDTIME is not null and BEGINTIME>endtime  group by substr(BEGINTIME,1,7) ",
               '医嘱时间在出入院时间之外' : f""" select '医嘱时间在出入院时间之外' as 业务类型,count(1) as num ,substr(BEGINTIME,1,7) as month from ANTIBIOTICS t1,overall t2 where 
                                    ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                    and t1.caseid = t2.caseid 
                                    and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME ) 
                                    and (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}')
                                    group by substr(BEGINTIME,1,7)
                                 """,
               }
    for bus in bus_dic:
        res_数据科室信息缺失及汇总 = res_数据科室信息缺失及汇总.append(pd.read_sql(bus_dic[bus],con=engine)) 
    return res_数据科室信息缺失及汇总

# 更新二级图一
@app.callback(
    Output('anti_second_level_first_fig','figure'),
    Output('anti_second_level_first_fig_data','data'),
    
    Input('anti_second_level_first_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_first_level_third_fig(anti_second_level_first_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if anti_second_level_first_fig_data is None:
            anti_second_level_first_fig = get_second_lev_first_fig_date(engine, btime, etime)
            anti_second_level_first_fig_data={}
            anti_second_level_first_fig_data['anti_second_level_first_fig'] = anti_second_level_first_fig.to_json(orient='split', date_format='iso')
            anti_second_level_first_fig_data['hosname'] = db_con_url['hosname']
            anti_second_level_first_fig_data['btime'] = btime
            anti_second_level_first_fig_data['etime'] = etime
            anti_second_level_first_fig_data = json.dumps(anti_second_level_first_fig_data)
        else:
            anti_second_level_first_fig_data = json.loads(anti_second_level_first_fig_data)
            if db_con_url['hosname'] != anti_second_level_first_fig_data['hosname']:
                anti_second_level_first_fig = get_second_lev_first_fig_date(engine, btime, etime)
                anti_second_level_first_fig_data['anti_second_level_first_fig'] = anti_second_level_first_fig.to_json(orient='split',date_format='iso')
                anti_second_level_first_fig_data['hosname'] = db_con_url['hosname']
                anti_second_level_first_fig_data['btime'] = btime
                anti_second_level_first_fig_data['etime'] = etime
                anti_second_level_first_fig_data = json.dumps(anti_second_level_first_fig_data)
            else:
                if anti_second_level_first_fig_data['btime'] != btime or anti_second_level_first_fig_data['etime'] != etime:
                    anti_second_level_first_fig = get_second_lev_first_fig_date(engine, btime, etime)
                    anti_second_level_first_fig_data['anti_second_level_first_fig'] = anti_second_level_first_fig.to_json(orient='split',date_format='iso')
                    anti_second_level_first_fig_data['btime'] = btime
                    anti_second_level_first_fig_data['etime'] = etime
                    anti_second_level_first_fig_data = json.dumps(anti_second_level_first_fig_data)
                else:
                    anti_second_level_first_fig = pd.read_json(anti_second_level_first_fig_data['anti_second_level_first_fig'], orient='split')
                    anti_second_level_first_fig_data = dash.no_update

    fig_概览一级_科室映射缺失 = go.Figure()

    bus_opts = anti_second_level_first_fig[['业务类型']].drop_duplicates().reset_index(drop=True)
    # res_数据科室信息缺失及汇总 = anti_second_level_first_fig.sort_values(['month','业务类型'])
    print(anti_second_level_first_fig)
    for tem,bus in bus_opts.iterrows():
        print(tem,)
        print(bus,)
        temp = anti_second_level_first_fig[anti_second_level_first_fig['业务类型']==bus['业务类型']]
        print(temp)
        temp = temp.sort_values(['month'])
        if temp.shape[0]>0:
            fig_概览一级_科室映射缺失.add_trace(
                go.Scatter(x=temp['month'], y=temp['num'], name=bus['业务类型'] ,marker_color=px.colors.qualitative.Dark24[tem]  )
            )

    fig_概览一级_科室映射缺失.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )

    )
    fig_概览一级_科室映射缺失.update_yaxes(title_text="问题数量")
    fig_概览一级_科室映射缺失.update_xaxes(title_text="月份")
    return fig_概览一级_科室映射缺失,anti_second_level_first_fig_data

# 下载二级图一明细
@app.callback(
    Output('anti_second_level_first_fig_date_detail', 'data'),
    Input('anti_second_level_first_fig_data_detail_down','n_clicks'),
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
                '用药目的缺失': f" select *  from ANTIBIOTICS where (substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}')   ",
                '药物等级缺失': f" select t1.* from ANTIBIOTICS t1 where (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}') ",
                '医嘱开始时间大于结束时间': f" select t1.*  from ANTIBIOTICS t1 where (substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}') and BEGINTIME is not null and ENDTIME is not null and BEGINTIME>endtime   ",
                '医嘱时间在出入院时间之外': f""" select t1.* from ANTIBIOTICS t1,overall t2 where 
                                                ( t1.BEGINTIME is not null and t1.ENDTIME is not null and t2.in_time is not null and t2.out_time is not null) 
                                                and t1.caseid = t2.caseid 
                                                and (t1.BEGINTIME<t2.IN_TIME or t1.BEGINTIME > t2.OUT_TIME or t1.ENDTIME<t2.IN_TIME or t1.ENDTIME > t2.OUT_TIME ) 
                                                and (substr(t1.BEGINTIME,1,7)>='{btime}' and substr(t1.BEGINTIME,1,7)<='{etime}') group by substr(BEGINTIME,1,7)
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
            return dcc.send_bytes(data, f'{hosName}抗菌药物问题数据明细.xlsx')
        else:
            return dash.no_update









# # -----------------------------------------------------------------------------------------------------    二级图二   ----------------------------------------------------------------------------------------------------------------------
# 获取抗菌药物二级第二张图数据
def get_second_level_second_fig_date(engine,btime,etime):
    res_业务逻辑问题数据汇总 = pd.read_sql(f" select ANAME as 抗菌药物,count(1) as num , substr(BEGINTIME,1,7) as 月份 from antibiotics where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' group by substr(BEGINTIME,1,7),ANAME ",con=engine)

    return res_业务逻辑问题数据汇总


# 更新二级图
@app.callback(
    Output('anti_second_level_second_fig','figure'),
    Output('anti_second_level_second_fig_data','data'),

    Input('anti_second_level_second_fig_data','data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    # prevent_initial_call=True
)
def update_second_level_fig(anti_second_level_second_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if anti_second_level_second_fig_data is None:
            anti_second_level_second_fig_data = {}
            anti_second_level_second_fig = get_second_level_second_fig_date(engine, btime, etime)
            anti_second_level_second_fig_data['anti_second_level_second_fig'] = anti_second_level_second_fig.to_json(orient='split', date_format='iso')
            anti_second_level_second_fig_data['hosname'] = db_con_url['hosname']
            anti_second_level_second_fig_data['btime'] = btime
            anti_second_level_second_fig_data['etime'] = etime
            anti_second_level_second_fig_data = json.dumps(anti_second_level_second_fig_data)
        else:
            anti_second_level_second_fig_data = json.loads(anti_second_level_second_fig_data)
            if db_con_url['hosname'] != anti_second_level_second_fig_data['hosname']:
                anti_second_level_second_fig = get_second_level_second_fig_date(engine, btime, etime)
                anti_second_level_second_fig_data['anti_second_level_second_fig'] = anti_second_level_second_fig.to_json(orient='split',date_format='iso')
                anti_second_level_second_fig_data['hosname'] = db_con_url['hosname']
                anti_second_level_second_fig_data['btime'] = btime
                anti_second_level_second_fig_data['etime'] = etime
                anti_second_level_second_fig_data = json.dumps(anti_second_level_second_fig_data)
            else:
                if anti_second_level_second_fig_data['btime'] != btime or anti_second_level_second_fig_data['etime'] != etime:
                    anti_second_level_second_fig = get_second_level_second_fig_date(engine, btime, etime)
                    anti_second_level_second_fig_data['anti_second_level_second_fig'] = anti_second_level_second_fig.to_json(orient='split',date_format='iso')
                    anti_second_level_second_fig_data['btime'] = btime
                    anti_second_level_second_fig_data['etime'] = etime
                    anti_second_level_second_fig_data = json.dumps(anti_second_level_second_fig_data)
                else:
                    anti_second_level_second_fig = pd.read_json(anti_second_level_second_fig_data['anti_second_level_second_fig'], orient='split')
                    anti_second_level_second_fig_data = dash.no_update

    antis_dict = discriminated_antis(anti_second_level_second_fig[['抗菌药物']].drop_duplicates())
    anti_second_level_second_fig = anti_second_level_second_fig.merge(antis_dict,on='抗菌药物',how='left')
    anti_second_level_second_fig['抗菌药物通用名'] = np.where(anti_second_level_second_fig['抗菌药物通用名'].isnull(),anti_second_level_second_fig['抗菌药物'],anti_second_level_second_fig['抗菌药物通用名'])
    anti_second_level_second_fig = anti_second_level_second_fig.sort_values(['月份'])
    fig = px.bar(anti_second_level_second_fig, x="月份", y="num", color='抗菌药物通用名' ,color_discrete_sequence=px.colors.qualitative.Dark24)

    fig.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        #title=f"{btime}--{etime}",
    )
    fig.update_yaxes(title_text="医嘱数量", )
    fig.update_xaxes(title_text="月份", )
    return fig,anti_second_level_second_fig_data

# -----------------------------------------------------------------------------------------------------    二级图三   ----------------------------------------------------------------------------------------------------------------------
# 获取抗菌药物二级第三张图数据
def get_second_level_third_fig_date(engine,btime,etime):
    res_业务逻辑问题数据汇总 = pd.read_sql(
        f" select ALEVEL as 抗菌药物等级,count(1) as num , substr(BEGINTIME,1,7) as 月份 from antibiotics where substr(BEGINTIME,1,7)>='{btime}' and substr(BEGINTIME,1,7)<='{etime}' and ALEVEL is not null group by substr(BEGINTIME,1,7),ALEVEL ",
        con=engine)

    return res_业务逻辑问题数据汇总

# 三级第一张图更新
@app.callback(
    Output('anti_second_level_third_fig','figure'),
    Output('anti_second_level_third_fig_data', 'data'),

    Input('anti_second_level_third_fig_data', 'data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
)
def update_third_level_first_fig(anti_second_level_third_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if anti_second_level_third_fig_data is None:
            anti_second_level_third_fig_data = {}
            anti_second_level_third_fig = get_second_level_third_fig_date(engine, btime, etime)
            anti_second_level_third_fig_data['anti_second_level_third_fig'] = anti_second_level_third_fig.to_json( orient='split', date_format='iso')
            anti_second_level_third_fig_data['hosname'] = db_con_url['hosname']
            anti_second_level_third_fig_data['btime'] = btime
            anti_second_level_third_fig_data['etime'] = etime
            anti_second_level_third_fig_data = json.dumps(anti_second_level_third_fig_data)
        else:
            anti_second_level_third_fig_data = json.loads(anti_second_level_third_fig_data)
            if db_con_url['hosname'] != anti_second_level_third_fig_data['hosname']:
                anti_second_level_third_fig = get_second_level_third_fig_date(engine, btime, etime)
                anti_second_level_third_fig_data['anti_second_level_third_fig'] = anti_second_level_third_fig.to_json(orient='split', date_format='iso')
                anti_second_level_third_fig_data['hosname'] = db_con_url['hosname']
                anti_second_level_third_fig_data['btime'] = btime
                anti_second_level_third_fig_data['etime'] = etime
                anti_second_level_third_fig_data = json.dumps(anti_second_level_third_fig_data)
            else:
                if anti_second_level_third_fig_data['btime'] != btime or anti_second_level_third_fig_data['etime'] != etime:
                    anti_second_level_third_fig = get_second_level_third_fig_date(engine, btime, etime)
                    anti_second_level_third_fig_data['anti_second_level_third_fig'] = anti_second_level_third_fig.to_json(orient='split', date_format='iso')
                    anti_second_level_third_fig_data['btime'] = btime
                    anti_second_level_third_fig_data['etime'] = etime
                    anti_second_level_third_fig_data = json.dumps(anti_second_level_third_fig_data)
                else:
                    anti_second_level_third_fig = pd.read_json( anti_second_level_third_fig_data['anti_second_level_third_fig'], orient='split')
                    anti_second_level_third_fig_data = dash.no_update

    anti_second_level_third_fig = anti_second_level_third_fig.sort_values(['月份'])
    fig = px.bar(anti_second_level_third_fig, x="月份", y="num", color='抗菌药物等级', color_discrete_sequence=px.colors.qualitative.Dark24)

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
    fig.update_yaxes(title_text="医嘱数量", )
    fig.update_xaxes(title_text="月份", )
    return fig,anti_second_level_third_fig_data



# # -----------------------------------------------------------------------------------------------------    三级图一   ----------------------------------------------------------------------------------------------------------------------
# # 获取菌检出三级第一张图数据
def get_third_level_first_fig_date(engine,btime,etime):
    res = pd.read_sql(f"""select  substr(REQUESTTIME,1,7) as month,BACTERIA as 菌,count(1) as num from BACTERIA where BACTERIA in ('大肠埃希菌', '鲍曼不动杆菌', '肺炎克雷伯菌', '金黄色葡萄球菌', '铜绿假单胞菌', '屎肠球菌', '粪肠球菌') 
                        and substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}' 
                        group by BACTERIA, substr(REQUESTTIME,1,7)
                        """,con=engine)
    return res
# 三级第一张图更新
@app.callback(
    Output('bar_third_level_first_fig', 'figure'),
    Output('bar_third_level_first_fig_data', 'data'), 

    Input('bar_third_level_first_fig_data', 'data'), 
    Input("db_con_url", "data"),
    Input("count_time", "data"),
)
def update_third_level_first_fig(bar_third_level_first_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if bar_third_level_first_fig_data is None:
            bar_third_level_first_fig_data = {}
            bar_third_level_first_fig = get_third_level_first_fig_date(engine, btime, etime)
            bar_third_level_first_fig_data['bar_third_level_first_fig'] = bar_third_level_first_fig.to_json( orient='split', date_format='iso')
            bar_third_level_first_fig_data['hosname'] = db_con_url['hosname']
            bar_third_level_first_fig_data['btime'] = btime
            bar_third_level_first_fig_data['etime'] = etime
            bar_third_level_first_fig_data = json.dumps(bar_third_level_first_fig_data)
        else:
            bar_third_level_first_fig_data = json.loads(bar_third_level_first_fig_data)
            if db_con_url['hosname'] != bar_third_level_first_fig_data['hosname']:
                bar_third_level_first_fig = get_third_level_first_fig_date(engine, btime, etime)
                bar_third_level_first_fig_data['bar_third_level_first_fig'] = bar_third_level_first_fig.to_json(orient='split', date_format='iso')
                bar_third_level_first_fig_data['hosname'] = db_con_url['hosname']
                bar_third_level_first_fig_data['btime'] = btime
                bar_third_level_first_fig_data['etime'] = etime
                bar_third_level_first_fig_data = json.dumps(bar_third_level_first_fig_data)
            else:
                if bar_third_level_first_fig_data['btime'] != btime or bar_third_level_first_fig_data['etime'] != etime:
                    bar_third_level_first_fig = get_third_level_first_fig_date(engine, btime, etime)
                    bar_third_level_first_fig_data['bar_third_level_first_fig'] = bar_third_level_first_fig.to_json(orient='split', date_format='iso')
                    bar_third_level_first_fig_data['btime'] = btime
                    bar_third_level_first_fig_data['etime'] = etime
                    bar_third_level_first_fig_data = json.dumps(bar_third_level_first_fig_data)
                else:
                    bar_third_level_first_fig = pd.read_json( bar_third_level_first_fig_data['bar_third_level_first_fig'], orient='split')
                    bar_third_level_first_fig_data = dash.no_update



    bar_third_level_first_fig = bar_third_level_first_fig.sort_values(['month' ])
    print(bar_third_level_first_fig)
    fig1 = px.line(bar_third_level_first_fig, x='month', y= 'num' , color= '菌', color_discrete_sequence=px.colors.qualitative.Dark24)

    fig1.update_layout(
        margin=dict(l=30, r=30, t=30, b=30),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    fig1.update_yaxes(title_text= '菌检出数量', )
    fig1.update_xaxes(title_text= '月份', )

    return fig1,bar_third_level_first_fig_data



# # -----------------------------------------------------------------------------------------------------    三级图二   ----------------------------------------------------------------------------------------------------------------------
# # 获取菌检出三级第二张图数据
def get_third_level_second_fig_date(engine,btime,etime):
    res_信息缺失及汇总 = pd.DataFrame(columns=['业务类型', 'num', 'month'])

    bus_dic = {
        '菌检出类型': f" select '菌检出类型缺失' as 业务类型,count(1) as num ,substr(REQUESTTIME,1,7) as month  from BACTERIA where (substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}') and BTYPE is null group by substr(REQUESTTIME,1,7) ",
        '院内外': f" select '院内外标识缺失' as 业务类型,count(1) as num ,substr(REQUESTTIME,1,7) as month from BACTERIA t1 where (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}') and OUTSIDE is null  group by substr(REQUESTTIME,1,7) ",
        '标本缺失': f" select '标本缺失' as 业务类型,count(1) as num ,substr(REQUESTTIME,1,7) as month from BACTERIA t1 where (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}') and SPECIMEN is null  group by substr(REQUESTTIME,1,7) ",
        '检验项目': f" select '检验项目缺失' as 业务类型,count(1) as num ,substr(REQUESTTIME,1,7) as month from BACTERIA t1 where (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}') and SUBJECT is null  group by substr(REQUESTTIME,1,7) ",
        '申请时间大于报告时间': f" select '菌检出申请时间大于报告时间' as 业务类型,count(1) as num ,substr(REQUESTTIME,1,7) as month  from BACTERIA t1 where (substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}') and REQUESTTIME is not null and REPORTTIME is not null and REQUESTTIME>REPORTTIME  group by substr(REQUESTTIME,1,7) ",
        '申请时间在出入院时间之外': f""" select '菌检出申请时间在出入院时间之外' as 业务类型,count(1) as num ,substr(REQUESTTIME,1,7) as month from BACTERIA t1,overall t2 where 
                                        ( t1.REQUESTTIME is not null  and t2.in_time is not null and t2.out_time is not null) 
                                        and t1.caseid = t2.caseid 
                                        and (t1.REQUESTTIME<t2.IN_TIME or t1.REQUESTTIME > t2.OUT_TIME  ) 
                                        and (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}')
                                        group by substr(REQUESTTIME,1,7)
                                     """,
        }
    for bus in bus_dic:
        res_信息缺失及汇总 = res_信息缺失及汇总.append(pd.read_sql(bus_dic[bus], con=engine))
    return res_信息缺失及汇总
# 三级第二张图更新
@app.callback(
    Output('bar_third_level_second_fig', 'figure'),
    Output('bar_third_level_second_fig_data', 'data'),

    Input('bar_third_level_second_fig_data', 'data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
)
def update_third_level_second_fig(bar_third_level_second_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if bar_third_level_second_fig_data is None:
            bar_third_level_second_fig_data = {}
            bar_third_level_second_fig = get_third_level_second_fig_date(engine, btime, etime)
            bar_third_level_second_fig_data['bar_third_level_second_fig'] = bar_third_level_second_fig.to_json( orient='split', date_format='iso')
            bar_third_level_second_fig_data['hosname'] = db_con_url['hosname']
            bar_third_level_second_fig_data['btime'] = btime
            bar_third_level_second_fig_data['etime'] = etime
            bar_third_level_second_fig_data = json.dumps(bar_third_level_second_fig_data)
        else:
            bar_third_level_second_fig_data = json.loads(bar_third_level_second_fig_data)
            if db_con_url['hosname'] != bar_third_level_second_fig_data['hosname']:
                bar_third_level_second_fig = get_third_level_second_fig_date(engine, btime, etime)
                bar_third_level_second_fig_data['bar_third_level_second_fig'] = bar_third_level_second_fig.to_json(orient='split', date_format='iso')
                bar_third_level_second_fig_data['hosname'] = db_con_url['hosname']
                bar_third_level_second_fig_data['btime'] = btime
                bar_third_level_second_fig_data['etime'] = etime
                bar_third_level_second_fig_data = json.dumps(bar_third_level_second_fig_data)
            else:
                if bar_third_level_second_fig_data['btime'] != btime or bar_third_level_second_fig_data['etime'] != etime:
                    bar_third_level_second_fig = get_third_level_second_fig_date(engine, btime, etime)
                    bar_third_level_second_fig_data['bar_third_level_second_fig'] = bar_third_level_second_fig.to_json(orient='split', date_format='iso')
                    bar_third_level_second_fig_data['btime'] = btime
                    bar_third_level_second_fig_data['etime'] = etime
                    bar_third_level_second_fig_data = json.dumps(bar_third_level_second_fig_data)
                else:
                    bar_third_level_second_fig = pd.read_json( bar_third_level_second_fig_data['bar_third_level_second_fig'], orient='split')
                    bar_third_level_second_fig_data = dash.no_update



    bar_third_level_second_fig = bar_third_level_second_fig.sort_values(['month' ])

    fig1 = px.line(bar_third_level_second_fig, x='month', y= 'num' , color= '业务类型', color_discrete_sequence=px.colors.qualitative.Dark24)

    fig1.update_layout(
        margin=dict(l=30, r=30, t=30, b=30),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    fig1.update_yaxes(title_text= '数据缺失数量', )
    fig1.update_xaxes(title_text= '月份', )

    return fig1,bar_third_level_second_fig_data


# 下载三级图二明细
@app.callback(
    Output('bar_third_level_second_fig_data_detail', 'data'),
    Input('bar_third_level_second_fig_data_detail_down','n_clicks'),
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
                '菌检出类型缺失': f" select * from BACTERIA where (substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}') and BTYPE is null ",
                '院内外标识缺失': f" select t1.* from BACTERIA t1 where (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}') and OUTSIDE is null ",
                '标本缺失': f" select t1.* from BACTERIA t1 where (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}') and SPECIMEN is null  ",
                '检验项目缺失': f" select t1.*  from BACTERIA t1 where (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}') and SUBJECT is null ",
                '申请时间大于报告时间': f" select t1.*  from BACTERIA t1 where (substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}') and REQUESTTIME is not null and REPORTTIME is not null and REQUESTTIME>REPORTTIME",
                '申请时间在出入院时间之外': f""" select t1.* from BACTERIA t1,overall t2 where 
                                                    ( t1.REQUESTTIME is not null  and t2.in_time is not null and t2.out_time is not null) 
                                                    and t1.caseid = t2.caseid 
                                                    and (t1.REQUESTTIME<t2.IN_TIME or t1.REQUESTTIME > t2.OUT_TIME  ) 
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
            return dcc.send_bytes(data, f'{hosName}菌检出问题数据明细.xlsx')
        else:
            return dash.no_update


# # -----------------------------------------------------------------------------------------------------    四级图一   ----------------------------------------------------------------------------------------------------------------------
# # 获取药敏四级第一张图数据
def get_fourth_level_first_fig_date(engine,btime,etime):
    res_信息缺失及汇总 = pd.DataFrame(columns=['业务类型', 'num', 'month'])

    bus_dic = {
        '药敏结果': f" select '药敏结果缺失' as 业务类型,count(1) as num ,substr(REQUESTTIME,1,7) as month  from DRUGSUSCEPTIBILITY where (substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}') and SUSCEPTIBILITY is null group by substr(REQUESTTIME,1,7) ",
        '标本缺失': f" select '标本缺失' as 业务类型,count(1) as num ,substr(REQUESTTIME,1,7) as month from DRUGSUSCEPTIBILITY t1 where (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}') and SPECIMEN is null  group by substr(REQUESTTIME,1,7) ",
        '申请时间大于报告时间': f" select '药敏申请时间大于报告时间' as 业务类型,count(1) as num ,substr(REQUESTTIME,1,7) as month  from DRUGSUSCEPTIBILITY t1 where (substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}') and REQUESTTIME is not null and REPORTTIME is not null and REQUESTTIME>REPORTTIME  group by substr(REQUESTTIME,1,7) ",
        '申请时间在出入院时间之外': f""" select '药敏申请时间在出入院时间之外' as 业务类型,count(1) as num ,substr(REQUESTTIME,1,7) as month from DRUGSUSCEPTIBILITY t1,overall t2 where 
                                        ( t1.REQUESTTIME is not null  and t2.in_time is not null and t2.out_time is not null) 
                                        and t1.caseid = t2.caseid 
                                        and (t1.REQUESTTIME<t2.IN_TIME or t1.REQUESTTIME > t2.OUT_TIME  ) 
                                        and (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}')
                                        group by substr(REQUESTTIME,1,7)
                                     """,
        }
    for bus in bus_dic:
        res_信息缺失及汇总 = res_信息缺失及汇总.append(pd.read_sql(bus_dic[bus], con=engine))
    return res_信息缺失及汇总
# 四级第一张图更新
@app.callback(
    Output('drug_fourth_level_first_fig', 'figure'),
    Output('drug_fourth_level_first_fig_data', 'data'),

    Input('drug_fourth_level_first_fig_data', 'data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
)
def update_third_level_second_fig(drug_fourth_level_first_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if drug_fourth_level_first_fig_data is None:
            drug_fourth_level_first_fig_data = {}
            drug_fourth_level_first_fig = get_fourth_level_first_fig_date(engine, btime, etime)
            drug_fourth_level_first_fig_data['drug_fourth_level_first_fig'] = drug_fourth_level_first_fig.to_json( orient='split', date_format='iso')
            drug_fourth_level_first_fig_data['hosname'] = db_con_url['hosname']
            drug_fourth_level_first_fig_data['btime'] = btime
            drug_fourth_level_first_fig_data['etime'] = etime
            drug_fourth_level_first_fig_data = json.dumps(drug_fourth_level_first_fig_data)
        else:
            drug_fourth_level_first_fig_data = json.loads(drug_fourth_level_first_fig_data)
            if db_con_url['hosname'] != drug_fourth_level_first_fig_data['hosname']:
                drug_fourth_level_first_fig = get_fourth_level_first_fig_date(engine, btime, etime)
                drug_fourth_level_first_fig_data['drug_fourth_level_first_fig'] = drug_fourth_level_first_fig.to_json(orient='split', date_format='iso')
                drug_fourth_level_first_fig_data['hosname'] = db_con_url['hosname']
                drug_fourth_level_first_fig_data['btime'] = btime
                drug_fourth_level_first_fig_data['etime'] = etime
                drug_fourth_level_first_fig_data = json.dumps(drug_fourth_level_first_fig_data)
            else:
                if drug_fourth_level_first_fig_data['btime'] != btime or drug_fourth_level_first_fig_data['etime'] != etime:
                    drug_fourth_level_first_fig = get_fourth_level_first_fig_date(engine, btime, etime)
                    drug_fourth_level_first_fig_data['drug_fourth_level_first_fig'] = drug_fourth_level_first_fig.to_json(orient='split', date_format='iso')
                    drug_fourth_level_first_fig_data['btime'] = btime
                    drug_fourth_level_first_fig_data['etime'] = etime
                    drug_fourth_level_first_fig_data = json.dumps(drug_fourth_level_first_fig_data)
                else:
                    drug_fourth_level_first_fig = pd.read_json( drug_fourth_level_first_fig_data['drug_fourth_level_first_fig'], orient='split')
                    drug_fourth_level_first_fig_data = dash.no_update



    drug_fourth_level_first_fig = drug_fourth_level_first_fig.sort_values(['month' ])

    fig1 = px.line(drug_fourth_level_first_fig, x='month', y= 'num' , color= '业务类型', color_discrete_sequence=px.colors.qualitative.Dark24)

    fig1.update_layout(
        margin=dict(l=30, r=30, t=30, b=30),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    fig1.update_yaxes(title_text= '数据缺失数量', )
    fig1.update_xaxes(title_text= '月份', )

    return fig1,drug_fourth_level_first_fig_data



# 下载四级图一明细
@app.callback(
    Output('drug_fourth_level_first_fig_data_detail', 'data'),
    Input('drug_fourth_level_first_fig_data_detail_down','n_clicks'),
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
                '药敏结果缺失': f" select * from DRUGSUSCEPTIBILITY where (substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}') and SUSCEPTIBILITY is null ",
                '标本缺失': f" select t1.* from DRUGSUSCEPTIBILITY t1 where (substr(t1.REQUESTTIME,1,7)>='{btime}' and substr(t1.REQUESTTIME,1,7)<='{etime}') and SPECIMEN is null ",
                '申请时间大于报告时间': f" select t1.* from DRUGSUSCEPTIBILITY t1 where (substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}') and REQUESTTIME is not null and REPORTTIME is not null and REQUESTTIME>REPORTTIME ",
                '申请时间在出入院时间之外': f""" select t1.* from DRUGSUSCEPTIBILITY t1,overall t2 where 
                                                    ( t1.REQUESTTIME is not null  and t2.in_time is not null and t2.out_time is not null) 
                                                    and t1.caseid = t2.caseid 
                                                    and (t1.REQUESTTIME<t2.IN_TIME or t1.REQUESTTIME > t2.OUT_TIME  ) 
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
            return dcc.send_bytes(data, f'{hosName}药敏问题数据明细.xlsx')
        else:
            return dash.no_update




# # -----------------------------------------------------------------------------------------------------    四级图二   ----------------------------------------------------------------------------------------------------------------------
# # 获取药敏四级第二张图数据
def get_fourth_level_second_fig_date(engine,btime,etime):
    res = pd.read_sql(f"""select count(1) as num,substr(REQUESTTIME,1,7) as month from (
                            select * from bacteria where (caseid,testno) not in (select caseid,testno from drugsusceptibility) and bacteria !='无菌' and bacteria not like '%酵母%' and bacteria not like '%念珠%' and bacteria not like '%真菌%'
                            ) t1 where substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{btime}' group by substr(REQUESTTIME,1,7)
                        """,con=engine)
    return res
# 四级第二张图更新
@app.callback(
    Output('drug_fourth_level_second_fig', 'figure'),
    Output('drug_fourth_level_second_fig_data', 'data'),

    Input('drug_fourth_level_second_fig_data', 'data'),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
)
def update_third_level_second_fig(drug_fourth_level_second_fig_data,db_con_url,count_time):
    if db_con_url is None:
        return dash.no_update
    else:
        db_con_url = json.loads(db_con_url)
        count_time = json.loads(count_time)
        engine = create_engine(db_con_url['db'])
        btime = count_time['btime'][0:7]
        etime = count_time['etime'][0:7]
        if drug_fourth_level_second_fig_data is None:
            drug_fourth_level_second_fig_data = {}
            drug_fourth_level_second_fig = get_fourth_level_first_fig_date(engine, btime, etime)
            drug_fourth_level_second_fig_data['drug_fourth_level_second_fig'] = drug_fourth_level_second_fig.to_json( orient='split', date_format='iso')
            drug_fourth_level_second_fig_data['hosname'] = db_con_url['hosname']
            drug_fourth_level_second_fig_data['btime'] = btime
            drug_fourth_level_second_fig_data['etime'] = etime
            drug_fourth_level_second_fig_data = json.dumps(drug_fourth_level_second_fig_data)
        else:
            drug_fourth_level_second_fig_data = json.loads(drug_fourth_level_second_fig_data)
            if db_con_url['hosname'] != drug_fourth_level_second_fig_data['hosname']:
                drug_fourth_level_second_fig = get_fourth_level_first_fig_date(engine, btime, etime)
                drug_fourth_level_second_fig_data['drug_fourth_level_second_fig'] = drug_fourth_level_second_fig.to_json(orient='split', date_format='iso')
                drug_fourth_level_second_fig_data['hosname'] = db_con_url['hosname']
                drug_fourth_level_second_fig_data['btime'] = btime
                drug_fourth_level_second_fig_data['etime'] = etime
                drug_fourth_level_second_fig_data = json.dumps(drug_fourth_level_second_fig_data)
            else:
                if drug_fourth_level_second_fig_data['btime'] != btime or drug_fourth_level_second_fig_data['etime'] != etime:
                    drug_fourth_level_second_fig = get_fourth_level_first_fig_date(engine, btime, etime)
                    drug_fourth_level_second_fig_data['drug_fourth_level_second_fig'] = drug_fourth_level_second_fig.to_json(orient='split', date_format='iso')
                    drug_fourth_level_second_fig_data['btime'] = btime
                    drug_fourth_level_second_fig_data['etime'] = etime
                    drug_fourth_level_second_fig_data = json.dumps(drug_fourth_level_second_fig_data)
                else:
                    drug_fourth_level_second_fig = pd.read_json( drug_fourth_level_second_fig_data['drug_fourth_level_second_fig'], orient='split')
                    drug_fourth_level_second_fig_data = dash.no_update



    drug_fourth_level_second_fig = drug_fourth_level_second_fig.sort_values(['month'])

    fig1 = px.line(drug_fourth_level_second_fig, x='month', y= 'num' , color_discrete_sequence=px.colors.qualitative.Dark24)

    fig1.update_layout(
        margin=dict(l=30, r=30, t=30, b=30),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    fig1.update_yaxes(title_text= '有菌检出无药敏数据量', )
    fig1.update_xaxes(title_text= '月份', )

    return fig1,drug_fourth_level_second_fig_data



# 下载四级图二明细
@app.callback(
    Output('drug_fourth_level_second_fig_data_detail', 'data'),
    Input('drug_fourth_level_second_fig_data_detail_down','n_clicks'),
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
                '有菌检出结果无药敏结果数据': f""" select t1.* from (
                            select * from bacteria where (caseid,testno) not in (select caseid,testno from drugsusceptibility) and bacteria !='无菌' and bacteria not like '%酵母%' and bacteria not like '%念珠%' and bacteria not like '%真菌%'
                            ) t1 where substr(REQUESTTIME,1,7)>='{btime}' and substr(REQUESTTIME,1,7)<='{etime}'  
                                                 """,
            }
            output = io.BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            for key in bus_dic.keys():
                try:
                    temp = pd.read_sql(bus_dic[key], con=engine)
                    print(key)
                    print(temp)
                    if temp.shape[0] > 0:
                        temp.to_excel(writer, sheet_name=key)
                except:
                    error_df = pd.DataFrame(['明细数据获取出错'], columns=[key])
                    error_df.to_excel(writer, sheet_name=key)
            writer.save()
            data = output.getvalue()
            hosName = db_con_url['hosname']
            return dcc.send_bytes(data, f'{hosName}有菌检出结果无药敏结果数据明细.xlsx')
        else:
            return dash.no_update

















#
# # -----------------------------------------------------------------------------------------------------    全部下载   ----------------------------------------------------------------------------------------------------------------------
# 页面数据统计结果下载
@app.callback(
    Output("down-anti-bar-drug", "data"),
    Input("anti-all-count-data-down", "n_clicks"),
    Input("anti_bar_drug_first_level_first_fig_data", "data"),
    Input("anti_bar_drug_first_level_second_fig_data", "data"),
    Input("anti_second_level_first_fig_data", "data"),
    Input("anti_second_level_second_fig_data", "data"),
    Input("anti_second_level_third_fig_data", "data"),
    Input("bar_third_level_first_fig_data", "data"),
    Input("bar_third_level_second_fig_data", "data"),
    Input("drug_fourth_level_first_fig_data", "data"),
    Input("drug_fourth_level_second_fig_data", "data"),
    Input("db_con_url", "data"),
    Input("count_time", "data"),
    prevent_initial_call=True,
)
def get_all_count_data(n_clicks, anti_bar_drug_first_level_first_fig_data,
                                anti_bar_drug_first_level_second_fig_data,
                                anti_second_level_first_fig_data,
                                anti_second_level_second_fig_data,
                                anti_second_level_third_fig_data,
                                bar_third_level_first_fig_data,
                                bar_third_level_second_fig_data,
                                drug_fourth_level_first_fig_data,
                                drug_fourth_level_second_fig_data,
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
            if anti_bar_drug_first_level_first_fig_data is not None and anti_bar_drug_first_level_second_fig_data is not None and anti_second_level_first_fig_data is not None and \
                    anti_second_level_second_fig_data is not None and anti_second_level_third_fig_data is not None and bar_third_level_first_fig_data is not None and \
                    bar_third_level_second_fig_data is not None and drug_fourth_level_first_fig_data is not None and drug_fourth_level_second_fig_data is not None :
                anti_bar_drug_first_level_first_fig_data = json.loads(anti_bar_drug_first_level_first_fig_data )
                anti_bar_drug_first_level_second_fig_data = json.loads(anti_bar_drug_first_level_second_fig_data )
                anti_second_level_first_fig_data = json.loads(anti_second_level_first_fig_data )
                anti_second_level_second_fig_data = json.loads(anti_second_level_second_fig_data )
                anti_second_level_third_fig_data = json.loads(anti_second_level_third_fig_data )
                bar_third_level_first_fig_data = json.loads(bar_third_level_first_fig_data )
                bar_third_level_second_fig_data = json.loads(bar_third_level_second_fig_data )
                drug_fourth_level_first_fig_data = json.loads(drug_fourth_level_first_fig_data )
                drug_fourth_level_second_fig_data = json.loads(drug_fourth_level_second_fig_data )
                if anti_bar_drug_first_level_first_fig_data['hosname'] == hosName and anti_bar_drug_first_level_first_fig_data['btime'] == btime and  anti_bar_drug_first_level_first_fig_data['etime'] == etime and \
                   anti_bar_drug_first_level_second_fig_data['hosname'] == hosName and  anti_bar_drug_first_level_second_fig_data['btime'] == btime and  anti_bar_drug_first_level_second_fig_data['etime'] == etime and \
                   anti_second_level_first_fig_data['hosname'] == hosName and  anti_second_level_first_fig_data['btime'] == btime and  anti_second_level_first_fig_data['etime'] == etime and \
                   anti_second_level_second_fig_data['hosname'] == hosName and  anti_second_level_second_fig_data['btime'] == btime and  anti_second_level_second_fig_data['etime'] == etime and \
                   anti_second_level_third_fig_data['hosname'] == hosName and  anti_second_level_third_fig_data['btime'] == btime and  anti_second_level_third_fig_data['etime'] == etime and \
                   bar_third_level_first_fig_data['hosname'] == hosName and  bar_third_level_first_fig_data['btime'] == btime and  bar_third_level_first_fig_data['etime'] == etime and \
                   bar_third_level_second_fig_data['hosname'] == hosName and  bar_third_level_second_fig_data['btime'] == btime and  bar_third_level_second_fig_data['etime'] == etime and \
                   drug_fourth_level_first_fig_data['hosname'] == hosName and  drug_fourth_level_first_fig_data['btime'] == btime and  drug_fourth_level_first_fig_data['etime'] == etime and \
                   drug_fourth_level_second_fig_data['hosname'] == hosName and  drug_fourth_level_second_fig_data['btime'] == btime and  drug_fourth_level_second_fig_data['etime'] == etime and \
                   anti_second_level_second_fig_data['hosname'] == hosName and  anti_second_level_second_fig_data['btime'] == btime and  anti_second_level_second_fig_data['etime'] == etime   :
                    anti_bar_drug_first_level_first_fig = pd.read_json(anti_bar_drug_first_level_first_fig_data['anti_bar_drug_first_level_first_fig'], orient='split')
                    anti_bar_drug_first_level_first_fig = anti_bar_drug_first_level_first_fig[(anti_bar_drug_first_level_first_fig['month'] >= btime) & (anti_bar_drug_first_level_first_fig['month'] <= etime)]

                    anti_bar_drug_first_level_second_fig  = pd.read_json( anti_bar_drug_first_level_second_fig_data['first_level_second_fig_data'], orient='split')

                    anti_second_level_first_fig = pd.read_json( anti_second_level_first_fig_data['anti_second_level_first_fig'], orient='split')
                    anti_second_level_second_fig = pd.read_json( anti_second_level_second_fig_data['anti_second_level_second_fig'], orient='split')
                    antis_dict = discriminated_antis(anti_second_level_second_fig[['抗菌药物']].drop_duplicates())
                    anti_second_level_second_fig = anti_second_level_second_fig.merge(antis_dict, on='抗菌药物', how='left')
                    anti_second_level_second_fig['抗菌药物通用名'] = np.where(anti_second_level_second_fig['抗菌药物通用名'].isnull(),
                                                                       anti_second_level_second_fig['抗菌药物'],
                                                                       anti_second_level_second_fig['抗菌药物通用名'])

                    anti_second_level_third_fig = pd.read_json( anti_second_level_third_fig_data['anti_second_level_third_fig'], orient='split')
                    bar_third_level_first_fig = pd.read_json( bar_third_level_first_fig_data['bar_third_level_first_fig'], orient='split')
                    bar_third_level_second_fig = pd.read_json( bar_third_level_second_fig_data['bar_third_level_second_fig'], orient='split')
                    drug_fourth_level_first_fig = pd.read_json( drug_fourth_level_first_fig_data['drug_fourth_level_first_fig'], orient='split')
                    drug_fourth_level_second_fig = pd.read_json( drug_fourth_level_second_fig_data['drug_fourth_level_second_fig'], orient='split')
                    output = io.BytesIO()
                    writer = pd.ExcelWriter(output, engine='xlsxwriter')
                    anti_bar_drug_first_level_first_fig.to_excel(writer, sheet_name='抗菌药物_菌检出_药敏每月业务数据量',index=False)
                    anti_bar_drug_first_level_second_fig.to_excel(writer, sheet_name='抗菌药物_菌检出_药敏业务数据量科室排行',index=False)
                    anti_second_level_first_fig.to_excel(writer, sheet_name='抗菌药物问题数据每月分布',index=False)
                    anti_second_level_second_fig.to_excel(writer, sheet_name='抗菌药物使用比例每月分布',index=False)
                    anti_second_level_third_fig.to_excel(writer, sheet_name='抗菌药物等级使用比例每月分布', index=False)
                    bar_third_level_first_fig.to_excel(writer, sheet_name='八种重点菌检出量每月分布', index=False)
                    bar_third_level_second_fig.to_excel(writer, sheet_name='菌检出问题数据每月分布', index=False)
                    drug_fourth_level_first_fig.to_excel(writer, sheet_name='药敏问题数据每月分布', index=False)
                    drug_fourth_level_second_fig.to_excel(writer, sheet_name='菌检出无药敏数据每月分布', index=False)
                    writer.save()
                    data = output.getvalue()
                    hosName = db_con_url['hosname']
                    return dcc.send_bytes(data, f'{hosName}_{now_time}抗菌药物_菌检出_药敏.xlsx')

                else:
                    return dash.no_update
            else:
                return dash.no_update
        else:
            return dash.no_update

