import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc

import pandas as pd
import plotly.express as px






data_temp_page = html.Div(
            [
                # 最上方系统log栏
                dbc.NavbarSimple(
                    brand="数据质量明细",
                    brand_href="#",
                    brand_style={"font-family": "Roboto",
                                 "font-size": "x-large",
                                 "font-weight": "600",
                                 "letter-spacing": "2.5px",
                                 "color": "#606060",
                                 },
                    #
                    # color= "#5b625b",
                    color="#F9F9F9",
                    dark=True,
                    fluid=True,
                    # sticky = "top",
                    style={"box-shadow": "0 2px 5px 0 rgb(0 0 0 / 26%)", "margin-bottom": "5px"},
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Row(
                                [
                                    dbc.ListGroup(
                                        [
                                            dbc.ListGroupItem("数据库连接", href="/dash/db_connect"),
                                            dbc.ListGroupItem("数据明细概况", href="/dash/data_general_situation"),
                                            dbc.ListGroupItem("患者人数", href="/dash/data_overall" ),
                                            dbc.ListGroupItem("手术", href="/dash/data_oper2" ),
                                            dbc.ListGroupItem("体温", href="/dash/data_temp" ,color="primary"),
                                            dbc.ListGroupItem("入出转", href="/dash/data_adt" ),
                                            dbc.ListGroupItem("菌检出", href="/dash/data_bar" ),
                                            dbc.ListGroupItem("药敏", href="/dash/data_drug" ),
                                            dbc.ListGroupItem("抗菌药物", href="/dash/data_anti" ),
                                            dbc.ListGroupItem("生化", href="/dash/data_rout" ),
                                            dbc.ListGroupItem("检查", href="/dash/data_exam" ),
                                        ], flush=True,
                                        style={
                                            'padding': '1.5rem 1rem',
                                            'text-align': 'center',
                                            'letter-spacing': '0.05rem',
                                            'font-weight': '800',
                                            'width': '100%',
                                            'height': '65%',
                                        }
                                    ),
                                    dbc.ListGroup(
                                        [
                                            dbc.ListGroupItem(
                                                [
                                                    dbc.Label(html.B("统计时段:", style={'font-size': 'large'}),
                                                              id="count-time",
                                                              style={'display': 'flex', 'margin-left': '-15px'}),
                                                    dbc.Row(
                                                        [
                                                            dbc.Label('开始时间：', style={'font-size': 'smaller'}),
                                                            dcc.Input(type='date', id='btime'),
                                                        ], justify='center',
                                                        style={'display': 'flex', 'align-items': 'center',
                                                               'margin-top': '20px'}
                                                    ),
                                                    dbc.Row(
                                                        [
                                                            dbc.Label('结束时间：', style={'font-size': 'smaller'}),
                                                            dcc.Input(type='date', id='etime'),
                                                        ], justify='center',
                                                        style={'display': 'flex', 'align-items': 'center',
                                                               'margin-top': '5px'}
                                                    ),
                                                ], style={'background-color': 'aliceblue'}
                                            ),
                                            dbc.ListGroupItem(dbc.Button('提交', id='sub-btn'),
                                                              style={'background-color': 'aliceblue'})

                                        ], flush=True,
                                        style={
                                            'padding': '1.5rem 1rem',
                                            'text-align': 'center',
                                            'letter-spacing': '0.05rem',
                                            'font-weight': '800',
                                            'width': '100%',
                                            'height': '40%',
                                        }
                                    ),
                                ], style={"height": '100%',
                                          'background-color': 'aliceblue',
                                          "box-shadow": "0 2px 5px 0 rgb(0 0 0 / 26%)",
                                          "margin": "0px 2px 0px -2px",
                                          "display": 'flex'
                                          }
                            )
                            , className='col-sm-2 col-md-2 sidebar',
                            style={"height": '860px', }),
                        dbc.Col('bbbb', className='col-sm-10 col-sm-offset-3 col-md-10 col-md-offset-2 main',
                                style={"box-shadow": "0 2px 5px 0 rgb(0 0 0 / 26%)", }),
                    ]
                    , className="container-fluid", )
            ], style={'width': '100%', 'height': '100%'}
        )