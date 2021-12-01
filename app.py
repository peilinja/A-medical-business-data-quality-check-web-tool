import dash
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

from server import app

from views.db_connect import db_conn_page
from views.data_general_situation import data_general_situation_page
from views.data_overall import data_overall_page
from views.data_oper2 import data_oper2_page
from views.data_temp import data_temp_page
from views.data_adt import data_adt_page
from views.data_bar import data_bar_page
from views.data_drug import data_drug_page
from views.data_anti import data_anti_page
from views.data_rout import data_rout_page
from views.data_exam import data_exam_page

app.layout = html.Div(
    [
        # 存储当前连接用户信息
        dcc.Store(id='db_con_url', storage_type='session'),
        # 存储当前统计时段信息
        dcc.Store(id='count_time', storage_type='session'),

        # 概览页面数据存储
        dcc.Store(id='general_situation_first_level_first_fig_data', storage_type='session'),
        dcc.Store(id='general_situation_first_level_second_fig_data', storage_type='session'),
        dcc.Store(id='general_situation_first_level_third_fig_data', storage_type='session'),
        dcc.Store(id='general_situation_secod_level_fig_data', storage_type='session'),
        dcc.Store(id='general_situation_third_level_first_fig_data', storage_type='session'),
        dcc.Store(id='general_situation_third_level_second_fig_data', storage_type='session'),
        dcc.Store(id='general_situation_third_level_second_fig_data1', storage_type='session'),

        # 抗菌药物、菌检出、药敏页面数据存储
        dcc.Store(id='anti_bar_drug_first_level_first_fig_data', storage_type='session'),
        dcc.Store(id='anti_bar_drug_first_level_second_fig_data', storage_type='session'),
        dcc.Store(id='anti_second_level_first_fig_data', storage_type='session'),
        dcc.Store(id='anti_second_level_second_fig_data', storage_type='session'),
        dcc.Store(id='anti_second_level_third_fig_data', storage_type='session'),
        dcc.Store(id='bar_third_level_first_fig_data', storage_type='session'),
        dcc.Store(id='bar_third_level_second_fig_data', storage_type='session'),
        dcc.Store(id='drug_fourth_level_first_fig_data', storage_type='session'),
        dcc.Store(id='drug_fourth_level_second_fig_data', storage_type='session'),
        # dcc.Store(id='bar_third_level_second_fig_data', storage_type='session'),

        # 监听url变化
        dcc.Location(id='url'),
        # html.Div(id = 'page-content',)
        html.Div(
            [
                # 最上方系统log栏
                dbc.NavbarSimple(
                    brand="数据质量明细",
                    brand_href="/dash/",
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
                    style={"box-shadow": "0 2px 5px 0 rgb(0 0 0 / 26%)", "margin-bottom": "5px"},
                ),
                dbc.Row(
                    [
                        # 左侧菜单栏
                        dbc.Col(
                            dbc.Row(
                                [
                                    dbc.ListGroup(
                                        [
                                            dbc.ListGroupItem("数据库连接",id='db_connect', href="/dash/db_connect",color="primary"),
                                            dbc.ListGroupItem("数据明细概况", id='data_general_situation', href="/dash/data_general_situation"),
                                            dbc.ListGroupItem("抗菌药物/菌检出/药敏", id='data_anti', href="/dash/data_anti"),
                                            # dbc.ListGroupItem("患者人数",id='data_overall', href="/dash/data_overall" ),
                                            # dbc.ListGroupItem("手术",id='data_oper2', href="/dash/data_oper2" ),
                                            # dbc.ListGroupItem("体温",id='data_temp', href="/dash/data_temp" ),
                                            # dbc.ListGroupItem("入出转",id='data_adt', href="/dash/data_adt" ),
                                            # dbc.ListGroupItem("菌检出",id='data_bar', href="/dash/data_bar" ),
                                            # dbc.ListGroupItem("药敏",id='data_drug', href="/dash/data_drug" ),
                                            # dbc.ListGroupItem("生化",id='data_rout', href="/dash/data_rout" ),
                                            # dbc.ListGroupItem("检查",id='data_exam', href="/dash/data_exam" ),
                                        ], flush=True,
                                        style={
                                            'padding': '1.5rem 1rem',
                                            'text-align': 'center',
                                            'letter-spacing': '0.05rem',
                                            'font-weight': '800',
                                            'width': '100%',
                                            'height': '10%',
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
                                                            dbc.Col(dbc.Label('开始时间：', style={'font-size': 'smaller'}),width=4,style={'margin-left':'0px','margin-right':'0px','padding-left':'0px','padding-right':'0px',}),
                                                            dbc.Col(dcc.Input(type='date', id='btime',style={'text-align':'center'}),width=8,style={'margin-left':'0px','margin-right':'0px','padding-left':'0px','padding-right':'0px',}),
                                                        ], justify='center',
                                                        style={'display': 'flex', 'align-items': 'center',
                                                               'margin-top': '20px'}
                                                    ),
                                                    dbc.Row(
                                                        [
                                                            dbc.Col(dbc.Label('结束时间：', style={'font-size': 'smaller'}),width=4,style={'margin-left':'0px','margin-right':'0px','padding-left':'0px','padding-right':'0px',}),
                                                            dbc.Col(dcc.Input(type='date', id='etime',style={'text-align':'center'}),width=8,style={'margin-left':'0px','margin-right':'0px','padding-left':'0px','padding-right':'0px',}),
                                                        ], justify='center',
                                                        style={'display': 'flex', 'align-items': 'center', 'margin-top': '5px'}
                                                    ),
                                                ], style={'background-color': 'aliceblue'}
                                            ),
                                            dbc.ListGroupItem(dbc.Button('提交', id='data-chioce-sub'),
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
                            ), className='col-sm-2 col-md-2 sidebar',
                            style={"height": '860px', }),
                            # style={"height": '500px', }),
                        # 右侧展示栏
                        dbc.Col(
                             [
                                 dbc.Row(html.Br()),
                                 # dbc.Row(html.Br()),
                                 dbc.Row(id = 'data-show',style={"justify":"center"})
                             ], className='col-sm-10 col-sm-offset-3 col-md-10 col-md-offset-2 main',
                                style={"box-shadow": "0 2px 5px 0 rgb(0 0 0 / 26%)", }
                        ),
                    ], className="container-fluid", )
            ]
        )
    ]
)


# 路由总控
@app.callback(
    [Output('data-show', 'children'),
    Output('db_connect', 'color'),
    Output('data_general_situation', 'color'),
    Output('data_anti', 'color'),  ],
    Input('url', 'pathname')
)
def render_page_content(pathname):
    color_dic={ 'rount_page':'',
                'db_connect':'',
               'data_general_situation':'',
               'data_anti':'',
               }
    if pathname is None:
        pathname = "/dash/"
    if pathname.endswith("/dash/") or pathname.endswith("/db_connect"):
        color_dic['rount_page'] = db_conn_page
        color_dic['db_connect'] = 'primary'
        return list(color_dic.values())
    elif pathname.endswith("/data_general_situation") :
        color_dic['rount_page'] = data_general_situation_page
        color_dic['data_general_situation'] = 'primary'
        return list(color_dic.values())

    elif pathname.endswith("/data_anti") :
        color_dic['rount_page'] = data_anti_page
        color_dic['data_anti'] = 'primary'
        return list(color_dic.values())
    else:
        return html.H1('您访问的页面不存在！')


















# # 路由总控
# @app.callback(
#     # Output('page-content', 'children'),
#     [Output('data-show', 'children'),
#     Output('db_connect', 'color'),
#     Output('data_general_situation', 'color'),
#     Output('data_overall', 'color'),
#     Output('data_oper2', 'color'),
#     Output('data_temp', 'color'),
#     Output('data_adt', 'color'),
#     Output('data_bar', 'color'),
#     Output('data_drug', 'color'),
#     Output('data_anti', 'color'),
#     Output('data_rout', 'color'),
#     Output('data_exam', 'color')],
#     Input('url', 'pathname')
# )
# def render_page_content(pathname):
#     color_dic={ 'rount_page':'',
#                 'db_connect':'',
#                'data_general_situation':'',
#                'data_overall':'',
#                'data_oper2':'',
#                'data_temp':'',
#                'data_adt':'',
#                'data_bar':'',
#                'data_drug':'',
#                'data_anti':'',
#                'data_rout':'',
#                'data_exam':'',
#                }
#     if pathname is None:
#         pathname = "/dash/"
#     if pathname.endswith("/dash/") or pathname.endswith("/db_connect"):
#         color_dic['rount_page'] = db_conn_page
#         color_dic['db_connect'] = 'primary'
#         return list(color_dic.values())
#     elif pathname.endswith("/data_general_situation") :
#         color_dic['rount_page'] = data_general_situation_page
#         color_dic['data_general_situation'] = 'primary'
#         return list(color_dic.values())
#     elif pathname.endswith("/data_overall"):
#         color_dic['rount_page'] = data_overall_page
#         color_dic['data_overall'] = 'primary'
#         return list(color_dic.values())
#     elif pathname.endswith("/data_oper2"):
#         color_dic['rount_page'] = data_oper2_page
#         color_dic['data_oper2'] = 'primary'
#         return list(color_dic.values())
#     elif pathname.endswith("/data_temp") :
#         color_dic['rount_page'] = data_temp_page
#         color_dic['data_temp'] = 'primary'
#         return list(color_dic.values())
#     elif pathname.endswith("/data_adt") :
#         color_dic['rount_page'] = data_adt_page
#         color_dic['data_adt'] = 'primary'
#         return list(color_dic.values())
#     elif pathname.endswith("/data_bar") :
#         color_dic['rount_page'] = data_bar_page
#         color_dic['data_bar'] = 'primary'
#         return list(color_dic.values())
#     elif pathname.endswith("/data_drug") :
#         color_dic['rount_page'] = data_drug_page
#         color_dic['data_drug'] = 'primary'
#         return list(color_dic.values())
#     elif pathname.endswith("/data_anti") :
#         color_dic['rount_page'] = data_anti_page
#         color_dic['data_anti'] = 'primary'
#         return list(color_dic.values())
#     elif pathname.endswith("/data_rout") :
#         color_dic['rount_page'] = data_rout_page
#         color_dic['data_rout'] = 'primary'
#         return list(color_dic.values())
#     elif pathname.endswith("/data_exam") :
#         color_dic['rount_page'] = data_exam_page
#         color_dic['data_exam'] = 'primary'
#         return list(color_dic.values())
#     else:
#         return html.H1('您访问的页面不存在！')


if __name__ == '__main__':
    # app.run_server(host='10.0.68.111',debug=False,port=8081, workers = 10)
    app.run_server(host='10.0.23.105',debug=False,port=8081)
    # app.run_server(host='10.0.68.111',debug=True,port=8081)