import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc

import pandas as pd
import plotly.express as px
from callbacks import dbconn_callbacks

db_conn_page =   dbc.Col(
                [
                    html.Div([
                             dbc.Row(html.Br()),
                             dbc.Row(html.Br()),
                             dbc.Row(
                                [dbc.Col(width=1),dbc.Col(html.H3("数据库连接",style={'letter-spacing': '0.05rem','font-weight': '600'}), width=10), dbc.Col(width=1),],
                                justify="center",
                            ),
                            dbc.Row(html.Br()),
                            dbc.Row(
                                [
                                    dbc.Col(width=2),
                                    dbc.Col([
                                        dbc.Row([
                                            dbc.Col(dbc.Label("数据库IP：",style={'margin-bottom':'0rem','font-weight': '550','font-size': '20px',}),width=4,style={'display':'flex','align-items':'center','justify-content': 'end'}),
                                            dbc.Col(dbc.Input(id = 'dbIp',placeholder="请填写要连接的数据库ip...",type="text"),width=4),
                                            dbc.Col(width=4),
                                        ],justify='center'),
                                        dbc.Row(html.Br()),
                                        dbc.Row([
                                            dbc.Col(dbc.Label("连接用户：",style={'margin-bottom':'0rem','font-weight': '550','font-size': '20px',}),width=4,style={'display':'flex','align-items':'center','justify-content': 'end'}),
                                            dbc.Col(dbc.Input(id = 'dbUser',placeholder="请填写数据库用户名...",type="text"),width=4),
                                            dbc.Col(width=4),
                                        ],justify='center'),
                                        dbc.Row(html.Br()),
                                        dbc.Row([
                                            dbc.Col(dbc.Label("用户密码：",style={'margin-bottom':'0rem','font-weight': '550','font-size': '20px',}),width=4,style={'display':'flex','align-items':'center','justify-content': 'end'}),
                                            dbc.Col(dbc.Input(id = 'dbPwd',placeholder="请填写数据库密码...",type="password"),width=4),
                                            dbc.Col(width=4),
                                        ],justify='center'),
                                        dbc.Row(html.Br()),
                                        dbc.Row([
                                            dbc.Col(dbc.Label("数据库端口：",style={'margin-bottom':'0rem','font-weight': '550','font-size': '20px',}),width=4,style={'display':'flex','align-items':'center','justify-content': 'end'}),
                                            dbc.Col(dbc.Input(id = 'dbPort',placeholder="请填写数据库端口...",type="text"),width=4),
                                            dbc.Col(width=4),
                                        ],justify='center'),
                                        dbc.Row(html.Br()),
                                        dbc.Row([
                                            dbc.Col(dbc.Label("数据库实例：",style={'margin-bottom':'0rem','font-weight': '550','font-size': '20px',}),width=4,style={'display':'flex','align-items':'center','justify-content': 'end'}),
                                            dbc.Col(dbc.Input(id = 'dbOrcl',placeholder="请填写数据库实例...",type="text"),width=4),
                                            dbc.Col(width=4),
                                        ],justify='center'),
                                        dbc.Row(html.Br()),
                                        dbc.Row([
                                            dbc.Col(dbc.Label("医院名称：",style={'margin-bottom':'0rem','font-weight': '550','font-size': '20px',}),width=4,style={'display':'flex','align-items':'center','justify-content': 'end'}),
                                            dbc.Col(dbc.Input(id = 'hosName',placeholder="请填写医院名称...",type="text"),width=4),
                                            dbc.Col(width=4),
                                        ],justify='center'),
                                        dbc.Row(html.Br()),
                                        dbc.Row(html.Br()),
                                        dbc.Row([
                                            dbc.Col(
                                                dbc.Row([
                                                    dbc.Col(width=5),
                                                    dbc.Col(dbc.Button('连接',id = 'dbConn',style={'letter-spacing': '0.4rem','font-weight': '550','font-size': '18px'}),width=2),
                                                    dbc.Col(width=5)
                                                ],justify="center"
                                                ),width=11,),
                                            dbc.Col(width=1),
                                        ],justify='center'),
                                        dbc.Row(html.Br()),
                                        dbc.Row([dbc.Col(width=2),dbc.Col(dbc.Spinner(html.Ul(id='error_msg')),width=8),dbc.Col(width=2)],justify="center"),
                                    ], width=8,),
                                    dbc.Col(width=2),],
                                justify="center",
                            ),
                        ])
            ] ,width=12
        )


