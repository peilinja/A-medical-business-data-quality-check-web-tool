import time

import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc

import pandas as pd
import plotly.express as px
from callbacks import data_anti_bar_drug_callbacks




data_anti_page = dbc.Col(dbc.Col(
                                [
                                    dbc.Row(
                                        [
                                            dbc.Col(html.H3('数据分布总览'), width=2),
                                            dbc.Col(
                                                dbc.Button
                                                    (
                                                    [
                                                        html.I(className="bi bi-box-arrow-in-down", ),
                                                        html.Font(' DOWNLOAD', style={'font-color': 'black'})
                                                    ],
                                                    id='anti-all-count-data-down'
                                                ), width=2
                                            ),
                                            dbc.Col(dcc.Download(id='down-anti-bar-drug'))
                                        ], style={'margin-bottom': '1%'}
                                    ),
                                    # 药-菌-药敏一级两张图
                                    dbc.Row(
                                        [
                                            # 一级图一:anti_bar_drug_first_level_first_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col('各业务量--时间变化趋势图', style={"letter-spacing": "1.2px"}, width=10),
                                                            # dbc.Col(
                                                            #     html.Button(
                                                            #         html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                                            #         id="anti_bar_drug_first_level_first_fig_data_detail_down",
                                                            #         style={'border': 'aliceblue', 'background-color': 'inherit'}
                                                            #     ), style={'display': 'flex', 'justify-content': 'end'}),
                                                            # dcc.Download(id='anti_bar_drug_first_level_first_fig')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dcc.Loading(dcc.Graph(id='anti_bar_drug_first_level_first_fig', )),
                                                    html.Hr(),
                                                ], class_name='card shadow ', style={'width': '49%', 'margin-right': '2%'}
                                            ),
                                            # 一级图二:anti_bar_drug_first_level_second_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col('各业务科室排行', width=3),
                                                            dbc.Col(
                                                                # dcc.RangeSlider(
                                                                #     id = "rank_month_choice",
                                                                #     # min= unixTimeMillis(daterange.min()),
                                                                #     # max= unixTimeMillis(daterange.max()),
                                                                #     # value=[unixTimeMillis(daterange.min()),
                                                                #     #          unixTimeMillis(daterange.max())],
                                                                #     # marks=getMarks(daterange.min(),daterange.max())
                                                                # ), style={'display': 'flex', 'justify-content': 'end'},width=9
                                                            ),
                                                            dcc.Download(id='anti_bar_drug_first_level_second_fig')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dcc.Loading(dcc.Graph(id='anti_bar_drug_first_level_second_fig', )),
                                                    # dcc.RangeSlider(id = "rank_month_choice",),
                                                    html.Hr(),
                                                ], class_name='card shadow ', style={'width': '33%', 'margin-right': '1.5%'}
                                            )
                                        ], style={'margin-bottom': '2%'}  # ,justify='center'
                                    ),

                                    dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),
                                    dbc.Row(html.H3('抗菌药物明细概览'), style={'margin-bottom': '1%'}),
                                    # 药-菌-药敏二级三张图
                                    dbc.Row(
                                        [
                                            # 二级图一 ： anti_second_level_first_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col('抗菌药物问题数据--时间变化', style={"letter-spacing": "1.2px"}, width=10),
                                                            dbc.Col(
                                                                html.Button(
                                                                    html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                                                    id="anti_second_level_first_fig_data_detail_down",
                                                                    style={'border': 'aliceblue', 'background-color': 'inherit'}
                                                                ), style={'display': 'flex', 'justify-content': 'end'}),
                                                            dcc.Download(id='anti_second_level_first_fig_date_detail')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dcc.Loading(dcc.Graph(id='anti_second_level_first_fig', )),
                                                    html.Hr(),
                                                ], class_name='card shadow ',
                                            ),

                                        ], style={'margin-bottom': '2%'}  # ,justify='center'
                                    ),
                                    dbc.Row(
                                        [
                                            # 二级图二:anti_second_level_second_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col('抗菌药物使用比例--时间分布', width=10),
                                                            # dbc.Col(
                                                            #     html.Button(
                                                            #         html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                                            #         id="anti_second_level_second_fig_data_detail_down",
                                                            #         style={'border': 'aliceblue', 'background-color': 'inherit'}
                                                            #     ), style={'display': 'flex', 'justify-content': 'end'}
                                                            # ),
                                                            # dcc.Download(id='anti_second_level_second_fig_data_detail')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dcc.Loading(dcc.Graph(id='anti_second_level_second_fig', )),
                                                    html.Hr(),
                                                ], class_name='card shadow ', style={'width': '49%', 'margin-right': '2%'}
                                            ),
                                            # 二级图三:anti_second_level_third_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col('抗菌药物等级使用比例--时间分布', width=10),
                                                            # dbc.Col(
                                                            #     html.Button(
                                                            #         html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                                            #         id="anti_second_level_third_fig_data_detail_down",
                                                            #         style={'border': 'aliceblue', 'background-color': 'inherit'}
                                                            #     ), style={'display': 'flex', 'justify-content': 'end'}
                                                            # ),
                                                            # dcc.Download(id='anti_second_level_third_fig_data_detail')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dcc.Loading(dcc.Graph(id='anti_second_level_third_fig', )),
                                                    html.Hr(),
                                                ], class_name='card shadow ', style={'width': '49%'}
                                            )
                                        ], style={'margin-bottom': '2%'} ),
                                    dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),
                                    dbc.Row(html.H3('菌检出明细概览'), style={'margin-bottom': '1%'}),
                                    # 概览三级图
                                    dbc.Row(
                                        [
                                            # 三级图一：bar_third_level_first_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col("八种重点菌检出量--时间变化", width=10),
                                                            # dbc.Col(
                                                            #     html.Button(
                                                            #         html.I(className="bi bi-box-arrow-in-down",
                                                            #                style={'color': 'blue'}),
                                                            #         id="bar_third_level_first_fig_data_detail_down",
                                                            #         style={'border': 'aliceblue',
                                                            #                'background-color': 'inherit'}
                                                            #     ), style={'display': 'flex', 'justify-content': 'end'}
                                                            # ),
                                                            # dcc.Download(id='bar_third_level_first_fig_data_detail')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dcc.Loading(dcc.Graph(id='bar_third_level_first_fig', )),
                                                    html.Hr(),
                                                ], class_name='card shadow ', style={'width': '49%', 'margin-right': '2%'}
                                            ),
                                            # 三级图二：bar_third_level_second_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col("菌检出问题数据--时间分布:", width=10),
                                                            dbc.Col(
                                                                html.Button(
                                                                    html.I(className="bi bi-box-arrow-in-down",
                                                                           style={'color': 'blue'}),
                                                                    id="bar_third_level_second_fig_data_detail_down",
                                                                    style={'border': 'aliceblue',
                                                                           'background-color': 'inherit'}
                                                                ), style={'display': 'flex', 'justify-content': 'end'}
                                                            ),
                                                            dcc.Download(id='bar_third_level_second_fig_data_detail')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dcc.Loading(dcc.Graph(id='bar_third_level_second_fig', )),
                                                    html.Hr(),
                                                ], class_name='card shadow ', style={'width': '33%', 'margin-right': '1.5%'}
                                            )
                                        ], style={'margin-bottom': '2%'}  # ,justify='center'
                                    ),

                                    dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),
                                    dbc.Row(html.H3('药敏明细概览'), style={'margin-bottom': '1%'}),
                                    # 概览四级图
                                    dbc.Row(
                                        [
                                            # 四级图一：drug_fourth_level_first_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col("药敏问题数据--时间分布", width=10),
                                                            dbc.Col(
                                                                html.Button(
                                                                    html.I(className="bi bi-box-arrow-in-down",
                                                                           style={'color': 'blue'}),
                                                                    id="drug_fourth_level_first_fig_data_detail_down",
                                                                    style={'border': 'aliceblue',
                                                                           'background-color': 'inherit'}
                                                                ), style={'display': 'flex', 'justify-content': 'end'}
                                                            ),
                                                            dcc.Download(id='drug_fourth_level_first_fig_data_detail')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dcc.Loading(dcc.Graph(id='drug_fourth_level_first_fig', )),
                                                    html.Hr(),
                                                ], class_name='card shadow ', style={'width': '49%', 'margin-right': '2%'}
                                            ),
                                            # 四级图二：drug_fourth_level_second_fig
                                            dbc.Col(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col("菌检出无药敏数据--时间分布:", width=10),
                                                            dbc.Col(
                                                                html.Button(
                                                                    html.I(className="bi bi-box-arrow-in-down",
                                                                           style={'color': 'blue'}),
                                                                    id="drug_fourth_level_second_fig_data_detail_down",
                                                                    style={'border': 'aliceblue',
                                                                           'background-color': 'inherit'}
                                                                ), style={'display': 'flex', 'justify-content': 'end'}
                                                            ),
                                                            dcc.Download(id='drug_fourth_level_second_fig_data_detail')
                                                        ],
                                                        class_name='card-header py-3',
                                                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                                                    ),
                                                    dcc.Loading(dcc.Graph(id='drug_fourth_level_second_fig', )),
                                                    html.Hr(),
                                                ], class_name='card shadow ', style={'width': '33%', 'margin-right': '1.5%'}
                                            )
                                        ], style={'margin-bottom': '2%'}  # ,justify='center'
                                    ),
                                    dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),

                                ], width=12
                        ), width=12)