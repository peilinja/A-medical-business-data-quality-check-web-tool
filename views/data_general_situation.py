import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc

import pandas as pd
import plotly.express as px
from callbacks import data_general_situation_callbacks



#
# data_general_situation_page =  dbc.Col(
#                                 # id='data-general-situation-show'
#                             [
#                                 # html.Div(id='data-general-situation-show'),
#                                 dcc.Loading(
#                                     html.Div(id='data-general-situation-show'), type='default', fullscreen=False
#                                 )
#                             ]
#                                 ,width=12
#                         )
bus_opts = [{'label': bus,'value' : bus } for bus in ['入院人数','抗菌药物医嘱数','手术台数','菌检出结果记录数','药敏结果记录数','体温测量数','入出转记录数','常规结果记录数','影像检查记录数','治疗记录数'] ]

data_general_situation_page =  dbc.Col(dbc.Col(
                                [
                                    dbc.Row(
                                        [
                                            dbc.Col(html.H3('数据缺失情况概览'), width=2),
                                            dbc.Col(
                                                dbc.Button
                                                    (
                                                    [
                                                        html.I(className="bi bi-box-arrow-in-down", ),
                                                        html.Font(' DOWNLOAD', style={'font-color': 'black'})
                                                    ],
                                                    id='all-count-data-down'
                                                ), width=2
                                            ),
                                            dbc.Col(dcc.Download(id='down-all-count-data'))
                                        ], style={'margin-bottom': '1%'}
                                    ),
                                    # 概览一级三张图
                                    dbc.Row(
                                        [
                                            # 一级图一:first_level_first_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col('各业务时间缺失数量占比', style={"letter-spacing": "1.2px"}, width=10),
                                                            dbc.Col(
                                                                html.Button(
                                                                    html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                                                    id="first_level_first_fig_data_detail_down",
                                                                    style={'border': 'aliceblue', 'background-color': 'inherit'}
                                                                ), style={'display': 'flex', 'justify-content': 'end'}),
                                                            dcc.Download(id='first_level_first_fig_data_detail')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dcc.Loading(dcc.Graph(id='first_level_first_fig', )),
                                                    html.Hr(),
                                                ], class_name='card shadow ', style={'width': '33%', 'margin-right': '1.5%'}
                                            ),
                                            # 一级图二:first_level_second_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col('各业务关键字缺失数量占比', width=10),
                                                            dbc.Col(
                                                                html.Button(
                                                                    html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                                                    id="first_level_second_fig_data_detail_down",
                                                                    style={'border': 'aliceblue', 'background-color': 'inherit'}
                                                                ), style={'display': 'flex', 'justify-content': 'end'}
                                                            ),
                                                            dcc.Download(id='first_level_second_fig_data_detail')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dcc.Loading(dcc.Graph(id='first_level_second_fig', )),
                                                    html.Hr(),
                                                ], class_name='card shadow ', style={'width': '33%', 'margin-right': '1.5%'}
                                            ),
                                            # 一级图三:first_level_third_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col('科室映射缺失数量占比', width=10),
                                                            dbc.Col(
                                                                html.Button(
                                                                    html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                                                    id="first_level_third_fig_data_detail_down",
                                                                    style={'border': 'aliceblue', 'background-color': 'inherit'})
                                                                , style={'display': 'flex', 'justify-content': 'end'}),
                                                            dcc.Download(id='first_level_third_fig_data_detail')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dcc.Loading(dcc.Graph(id='first_level_third_fig')),
                                                    html.Hr(),
                                                ], class_name='card shadow ', style={'width': '33%'}
                                            ),
                                        ], style={'margin-bottom': '2%'}  # ,justify='center'
                                    ),

                                    dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),
                                    dbc.Row(html.H3('业务数据--逻辑问题数量概览'), style={'margin-bottom': '1%'}),
                                    # 二级图
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                [
                                                    # 二级图一 ： second_level_fig
                                                    dbc.Row(
                                                        [
                                                            dbc.Col("全院数据逻辑问题", width=10),
                                                            dbc.Col(
                                                                html.Button(
                                                                    html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                                                    id="second_level_fig_data_detail_down",
                                                                    style={'border': 'aliceblue', 'background-color': 'inherit'})
                                                                , style={'display': 'flex', 'justify-content': 'end'}),
                                                            dcc.Download(id='second_level_fig_date_detail')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
                                                    ),
                                                    dbc.Row(dcc.Loading(dcc.Graph(id='second_level_fig', ))),

                                                ], class_name='card shadow ', width=12
                                            )
                                        ], style={'margin-bottom': '1%'}
                                    ),
                                    dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),
                                    dbc.Row(html.H3('业务数据--时间分布趋势概览'), style={'margin-bottom': '1%'}),
                                    # 概览三级图
                                    dbc.Row(
                                        [
                                            # 三级图一：third_level_first_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col("全院数据时间趋势图--全业务", width=2),
                                                            dbc.Col(dcc.Dropdown(
                                                                # "请选择窗口期：",
                                                                id='third_level_first_window_choice',
                                                                options=[{"label": f"{window}月", "value": window} for window in [5, 10]],
                                                                value=5,
                                                                style={'text-align': 'center'}
                                                            ), width=3, ),
                                                            # dbc.Col(dcc.RadioItems(id='third_level_first_date_type_choice',options=[{'label': date_type ,'value': date_type } for date_type in ['week','month']],value='month'), width=2),
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dbc.Row(dcc.Loading(dcc.Graph(id='third_level_first_fig', ))),
                                                    html.Hr(),
                                                ], class_name='card shadow ', width=12,
                                            ),
                                        ], style={'margin-bottom': '1%'}
                                    ),
                                    dbc.Row(
                                        [
                                            # 三级图二：third_level_second_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col("全院数据时间趋势图--单业务:", width=2),
                                                            dbc.Col(dcc.Dropdown(
                                                                options = bus_opts,
                                                                value = bus_opts[0].get('label'),
                                                                id='third_level_second_business_choice',
                                                                style={'text-align': 'center'}
                                                            ), width=3, ),
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dbc.Row([
                                                        dbc.Col("科室选择:", width=1, style={'text-align': 'right'}),
                                                        dbc.Col(
                                                            dcc.Dropdown(
                                                                id='third_level_second_dept_choice',
                                                                # options=opt,
                                                                # value= '全部科室',
                                                                multi=True
                                                            ) , width=11),
                                                    ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center',
                                                              'margin': '1% 1% 1% 1%'}),
                                                    dcc.Loading(dbc.Row(dcc.Graph(id='third_level_second_fig', ))),
                                                    html.Hr(),
                                                ], class_name='card shadow ', width=12
                                            )
                                        ], style={'margin-bottom': '2%'}
                                    ),

                                    dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),

                                ], width=12
                        ), width=12)