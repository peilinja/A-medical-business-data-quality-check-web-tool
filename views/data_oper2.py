import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc

import pandas as pd
import plotly.express as px

from callbacks import data_oper2_callbacks




data_oper2_page = dbc.Col(
    dbc.Col(
        [
            dbc.Row(
                [
                    dbc.Col(html.H3('手术数据分布总览'), width=2),
                    dbc.Col(
                        dbc.Button
                            (
                                [
                                    html.I(className="bi bi-box-arrow-in-down", ),
                                    html.Font(' DOWNLOAD', style={'font-color': 'black'})
                                ],
                                id='oper2-all-count-data-down'
                            ), width=2
                    ),
                    dbc.Col(dcc.Download(id='down-oper2'))
                ], style={'margin-bottom': '1%'}
            ),
            # 手术一级两张图
            dbc.Row(
                [
                    # 一级图一:oper2_first_level_first_fig
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col('问题数据--时间分布', style={"letter-spacing": "1.2px"}, width=10),
                                    dbc.Col(
                                        html.Button(
                                            html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                            id="oper2_first_level_first_fig_data_detail_down",
                                            style={'border': 'aliceblue', 'background-color': 'inherit'}
                                        ), style={'display': 'flex', 'justify-content': 'end'}),
                                    dcc.Download(id='oper2_first_level_first_fig_detail')
                                ],
                                class_name='card-header py-3',
                                style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                            ),
                            dcc.Loading(dcc.Graph(id='oper2_first_level_first_fig', )),
                            html.Hr(),
                        ], class_name='card shadow ', style={'width': '49%', 'margin-right': '2%'}
                    ),
                    # 一级图二:oper2_first_level_second_fig
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col('手术切口等级数量占比--时间分布', width=10),
                                ],
                                class_name='card-header py-3',
                                style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                            ),
                            dcc.Loading(dcc.Graph(id='oper2_first_level_second_fig', )),
                            html.Hr(),
                        ], class_name='card shadow ', style={'width': '33%', 'margin-right': '1.5%'}
                    )
                ], style={'margin-bottom': '2%'}
            ),

            dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),
            dbc.Row(html.H3('手术明细概览'), style={'margin-bottom': '1%'}),
            # 手术二级图
            dbc.Row(
                [
                    # 二级图一 ： oper2_second_level_first_fig
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col('手术数量--时间统计', style={"letter-spacing": "1.2px"}, width=10),
                                ],
                                class_name='card-header py-3',
                                style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                            ),
                            dcc.Loading(dcc.Graph(id='oper2_second_level_first_fig', )),
                            html.Hr(),
                        ], class_name='card shadow ',
                    ),

                ], style={'margin-bottom': '2%'}  # ,justify='center'
            ),
            dbc.Row(
                [
                    # 二级图二:oper2_second_level_second_fig
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col('手术类别比例--时间分布', width=10),
                                ],
                                class_name='card-header py-3',
                                style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                            ),
                            dcc.Loading(dcc.Graph(id='oper2_second_level_second_fig', )),
                            html.Hr(),
                        ], class_name='card shadow ', style={'width': '49%', 'margin-right': '2%'}
                    ),
                    # 二级图三:oper2_second_level_third_fig
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col('手术麻醉方式比例--时间分布', width=10),
                                ],
                                class_name='card-header py-3',
                                style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                            ),
                            dcc.Loading(dcc.Graph(id='oper2_second_level_third_fig', )),
                            html.Hr(),
                        ], class_name='card shadow ', style={'width': '49%'}
                    )
                ], style={'margin-bottom': '2%'}),

            dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),
        ],width=12
    ),width=12
)