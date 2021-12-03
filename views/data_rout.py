import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc

import pandas as pd
import plotly.express as px

from callbacks import data_rout_callbacks




data_rout_page =  dbc.Col(
    dbc.Col(
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
                                id='rout-exam-temp-all-count-data-down'
                            ), width=2
                    ),
                    dbc.Col(dcc.Download(id='down-rout-exam-temp'))
                ], style={'margin-bottom': '1%'}
            ),
            # 生化一级两张图
            dbc.Row(
                [
                    # 一级图一:rout_exam_temp_first_level_first_fig
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col('各业务数据数量--时间分布', style={"letter-spacing": "1.2px"}, width=10),
                                    #
                                ],
                                class_name='card-header py-3',
                                style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                            ),
                            dcc.Loading(dcc.Graph(id='rout_exam_temp_first_level_first_fig', )),
                            html.Hr(),
                        ], class_name='card shadow ', style={'width': '49%', 'margin-right': '2%'}
                    ),
                    # 一级图二:rout_exam_temp_first_level_second_fig
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col('各业务全数据时间缺失数量统计', width=10),
                                    dbc.Col(
                                        html.Button(
                                            html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                            id="rout_exam_temp_first_level_second_fig_data_detail_down",
                                            style={'border': 'aliceblue', 'background-color': 'inherit'}
                                        ), style={'display': 'flex', 'justify-content': 'end'}),
                                    dcc.Download(id='rout_exam_temp_first_level_second_fig_detail')
                                ],
                                class_name='card-header py-3',
                                style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                            ),
                            dcc.Loading(dcc.Graph(id='rout_exam_temp_first_level_second_fig', )),
                            html.Hr(),
                        ], class_name='card shadow ', style={'width': '33%', 'margin-right': '1.5%'}
                    )
                ], style={'margin-bottom': '2%'}
            ),

            dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),
            dbc.Row(html.H3('体温明细概览'), style={'margin-bottom': '1%'}),
            # 体温二级图
            dbc.Row(
                [
                    # 二级图一 ： temp_second_level_first_fig
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col('体温问题数据数量--时间分布', style={"letter-spacing": "1.2px"}, width=10),
                                    dbc.Col(
                                        html.Button(
                                            html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                            id="temp_second_level_first_fig_data_detail_down",
                                            style={'border': 'aliceblue', 'background-color': 'inherit'}
                                        ), style={'display': 'flex', 'justify-content': 'end'}),
                                    dcc.Download(id='temp_second_level_first_fig_detail')
                                ],
                                class_name='card-header py-3',
                                style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                            ),
                            dcc.Loading(dcc.Graph(id='temp_second_level_first_fig', )),
                            html.Hr(),
                        ], class_name='card shadow '#, style={'width': '49%', 'margin-right': '2%'}
                    ),
                    # 二级图二 ： temp_second_level_second_fig
                    # dbc.Col(
                    #     [
                    #         dbc.Row(
                    #             [
                    #                 dbc.Col('体温问题数据数量--时间分布', style={"letter-spacing": "1.2px"}, width=10),
                    #             ],
                    #             class_name='card-header py-3',
                    #             style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                    #         ),
                    #         dcc.Loading(dcc.Graph(id='temp_second_level_second_fig', )),
                    #         html.Hr(),
                    #     ], class_name='card shadow ', style={'width': '49%'}
                    # ),
                ], style={'margin-bottom': '2%'}  # ,justify='center'
            ),
            dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),
            dbc.Row(html.H3('生化明细概览'), style={'margin-bottom': '1%'}),
            dbc.Row(
                [
                    # 三级图一:rout_third_level_first_fig
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col('生化问题数据--时间分布', width=10),
                                    dbc.Col(
                                        html.Button(
                                            html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                            id="rout_third_level_first_fig_data_detail_down",
                                            style={'border': 'aliceblue', 'background-color': 'inherit'}
                                        ), style={'display': 'flex', 'justify-content': 'end'}),
                                    dcc.Download(id='rout_third_level_first_fig_detail')
                                ],
                                class_name='card-header py-3',
                                style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                            ),
                            dcc.Loading(dcc.Graph(id='rout_third_level_first_fig', )),
                            html.Hr(),
                        ], class_name='card shadow ', style={'width': '49%', 'margin-right': '2%'}
                    ),
                    # 三级图二:rout_third_level_second_fig
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col('生化所有检验类型比例--时间分布', width=10),
                                ],
                                class_name='card-header py-3',
                                style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                            ),
                            dcc.Loading(dcc.Graph(id='rout_third_level_second_fig', )),
                            html.Hr(),
                        ], class_name='card shadow ', style={'width': '49%'}
                    )
                ], style={'margin-bottom': '2%'}),

            dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),
            dbc.Row(html.H3('检查明细概览'), style={'margin-bottom': '1%'}),
            dbc.Row(
                [
                    # 四级图一:exam_fourth_level_first_fig
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col('检查问题数据--时间分布', width=10),
                                    dbc.Col(
                                        html.Button(
                                            html.I(className="bi bi-box-arrow-in-down", style={'color': 'blue'}),
                                            id="exam_fourth_level_first_fig_data_detail_down",
                                            style={'border': 'aliceblue', 'background-color': 'inherit'}
                                        ), style={'display': 'flex', 'justify-content': 'end'}),
                                    dcc.Download(id='exam_fourth_level_first_fig_detail')
                                ],
                                class_name='card-header py-3',
                                style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                            ),
                            dcc.Loading(dcc.Graph(id='exam_fourth_level_first_fig', )),
                            html.Hr(),
                        ], class_name='card shadow ', style={'width': '49%', 'margin-right': '2%'}
                    ),
                    # 四级图二:exam_fourth_level_second_fig
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col('检查类别比例--时间分布', width=10),
                                ],
                                class_name='card-header py-3',
                                style={'display': 'flex', 'align-items': 'center', 'justify-content': 'start'}
                            ),
                            dcc.Loading(dcc.Graph(id='exam_fourth_level_second_fig', )),
                            html.Hr(),
                        ], class_name='card shadow ', style={'width': '49%'}
                    )
                ], style={'margin-bottom': '2%'}),

            dbc.Row(dbc.Col(html.Hr(), style={'margin-bottom': '2%'}, width=12), ),
        ],width=12
    ),width=12
)