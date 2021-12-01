import json

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
from dash.exceptions import PreventUpdate


# 数据库连接验证
@app.callback(
    [Output("db_con_url", "data"),
     Output("error_msg", "children"),
     Output("error_msg", "style"),
     Output('dbIp', 'style'),
     Output('dbUser', 'style'),
     Output('dbPwd', 'style'),
     Output('dbPort', 'style'),
     Output('dbOrcl', 'style'),
     Output('hosName', 'style'),
     ],

    Input("dbConn","n_clicks"),
    State('dbIp','value'),
    State('dbUser','value'),
    State('dbPwd','value'),
    State('dbPort','value'),
    State('dbOrcl','value'),
    State('hosName','value'),
    prevent_initial_call=True
)
def db_con_isValid(nclicks,dbIp,dbUser,dbPwd,dbPort,dbOrcl,hosName):
    if nclicks is not None and nclicks>0:
        print(nclicks)
        lis_style = ['',[],'',{},{},{},{},{},{}]
        vild_lis = [dbIp, dbUser, dbPwd, dbPort, dbOrcl, hosName ]
        error_msg = ["数据库IP为空", "数据库用户为空", "数据库密码为空", "数据库端口为空",   "数据库实例名为空",   "医院名称为空"]
        for i in range(len(vild_lis)):
            if vild_lis[i] is None or len(vild_lis[i]) == 0:
                if len(lis_style[1]) == 0:
                        lis_style[1] = [html.Li(error_msg[i])]
                else:
                    lis_style[1].append(html.Li(error_msg[i]))
                lis_style[2] = {'color': '#9F3A38', 'border-color': '#9F3A38'}
                lis_style[i+3] = {'color': '#9F3A38', 'border-color': '#9F3A38'}
        if len(lis_style[1])>0:
            lis_style[0] = dash.no_update
            # lis_style[-1] = dash.no_update
            # lis_style[-2] = dash.no_update
            return lis_style


        try:
            engine = create_engine(f'oracle+cx_oracle://{dbUser}:{dbPwd}@{dbIp}:{dbPort}/{dbOrcl}', echo=False, encoding='UTF-8')
            res = pd.read_sql('select 1 from dual',con=engine)

        except:
            if len(lis_style[1]) == 0:
                lis_style[1] = [html.Li('数据库连接失败，请检查配置是否有误。')]
            lis_style[2] = {'color': '#9F3A38', 'border-color': '#9F3A38'}
            lis_style[0] = dash.no_update
            return lis_style
        lis_style[0] = json.dumps({'db': f'oracle+cx_oracle://{dbUser}:{dbPwd}@{dbIp}:{dbPort}/{dbOrcl}', 'dbuser': dbUser,'dbpwd': dbPwd, 'hosname': hosName})
        lis_style[1] = [html.Li('连接成功！')]
        lis_style[2] = {'color': '#339933', 'border-color': '#339933'}
        return lis_style
    else:
        return dash.no_update

@app.callback(
    Output("count_time", "data"),
    Output("btime", "value"),
    Output("etime", "value"),

    Input("data-chioce-sub","n_clicks"),
    Input("db_con_url","data"),
    Input("count_time", "data"),
    State("btime","value"),
    State("etime","value"),
    prevent_initial_call=True
)
def change_count_time(change_date_click,db_con_url,count_time,btime,etime):
    print(change_date_click)
    ctx = dash.callback_context

    if not ctx.triggered:
        raise PreventUpdate
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        print(button_id,btime,etime,type(btime),type(etime))
        if button_id == "data-chioce-sub":
            if btime is None or etime is None or btime == '' or etime =='':
                raise PreventUpdate
            else:
                if btime>etime:
                    # raise PreventUpdate
                    return dash.no_update,json.loads(count_time)['btime'],json.loads(count_time)['etime']
                else:
                    return json.dumps({'btime':btime,'etime':etime}),btime,etime
        else:
            try:
                engine = create_engine( json.loads(db_con_url).get('db'), echo=False, encoding='UTF-8' )
                btime = pd.read_sql("select min(in_time) as btime from overall ", con=engine)['btime'][0][0:10]
                etime = str(datetime.now())[0:10]
                return json.dumps({'btime':btime,'etime':etime}),btime,etime
            except:
                raise PreventUpdate