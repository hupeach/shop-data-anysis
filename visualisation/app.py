import sys
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import pymysql
import uuid
import time

app = Flask(__name__)
CORS(app)

# 数据库配置
DB_CONFIG = {
    'host': 'master',
    'user': 'root',
    'password': '123456',
    'database': 'echarts',
    'charset': 'utf8mb4'
}


# 获取数据库连接
def get_db():
    return pymysql.connect(**DB_CONFIG)


# 创建用户表
def init_db():
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            sql = """
            CREATE TABLE IF NOT EXISTS user (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(80) UNIQUE NOT NULL,
                password VARCHAR(120) NOT NULL
            )
            """
            cursor.execute(sql)
        conn.commit()
    finally:
        conn.close()


# 初始化数据库
init_db()


def reduce_prov(name):
    # 处理省份不匹配问题
    if name == "新疆":
        name = "新疆维吾尔自治区"
    elif name == "广西":
        name = "广西回族自治区"
    elif name == "宁夏":
        name = "宁夏回族自治区"
    elif name in ["内蒙古", "西藏"]:
        name = name + "自治区"
    elif name in ["北京", "天津", "重庆", "上海"]:
        name = name + "市"
    elif name in ["香港", "澳门"]:
        name = name + "特别行政区"
    else:
        name = name + "省"
    return name


# 页面路由
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/chart/annual_trend', methods=['GET'])
def get_annual_trend():
    conn = get_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = """
                SELECT order_date_year, order_quantity, order_growth_rate,
                       sales_amount, sales_growth_rate 
                FROM annual_trend
                ORDER BY order_date_year
            """
            cursor.execute(sql)
            data = cursor.fetchall()

            # 数据处理
            years = []
            order_quantities = []
            order_growth_rates = []
            sales_amounts = []
            sales_growth_rates = []

            for row in data:
                years.append(str(row['order_date_year']))
                order_quantities.append(int(row['order_quantity']))
                order_growth_rates.append(float(row['order_growth_rate']))
                sales_amounts.append(float(row['sales_amount']))
                sales_growth_rates.append(float(row['sales_growth_rate']))

            return jsonify({
                'chart1': {
                    'title': '年度订单分析',
                    'xAxis': years,
                    'series': [
                        {
                            'name': '订单数量',
                            'type': 'bar',
                            'data': order_quantities,
                            'yAxisIndex': 0
                        },
                        {
                            'name': '订单增长率',
                            'type': 'line',
                            'data': order_growth_rates,
                            'yAxisIndex': 1
                        }
                    ]
                },
                'chart2': {
                    'title': '年度销售额分析',
                    'xAxis': years,
                    'series': [
                        {
                            'name': '销售额',
                            'type': 'bar',
                            'data': sales_amounts,
                            'yAxisIndex': 0
                        },
                        {
                            'name': '销售额增长率',
                            'type': 'line',
                            'data': sales_growth_rates,
                            'yAxisIndex': 1
                        }
                    ]
                }
            })
    except Exception as e:
        print(e)
        return jsonify({'error': '获取数据失败'}), 500
    finally:
        conn.close()


@app.route('/api/chart/monthly_sales', methods=['GET'])
def get_monthly_sales():
    conn = get_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = """
                SELECT order_date_year, order_date_month, total_sales
                FROM monthly_sales
                ORDER BY order_date_year, order_date_month
            """
            cursor.execute(sql)
            data = cursor.fetchall()

            # 数据处理
            years = sorted(list(set(str(row['order_date_year']) for row in data)))
            months = ['1月', '2月', '3月', '4月', '5月', '6月',
                      '7月', '8月', '9月', '10月', '11月', '12月']

            # 按年份组织销售数据
            series_data = []
            for year in years:
                year_data = []
                for month in range(1, 13):
                    sales = next(
                        (row['total_sales'] for row in data
                         if str(row['order_date_year']) == year and row['order_date_month'] == month),
                        0
                    )
                    year_data.append(float(sales))

                series_data.append({
                    'name': f'{year}年',
                    'type': 'line',
                    'smooth': True,
                    'data': year_data
                })

            return jsonify({
                'title': '月度销售额趋势对比',
                'xAxis': months,
                'legend': [f'{year}年' for year in years],
                'series': series_data
            })
    except Exception as e:
        print(e)
        return jsonify({'error': '获取数据失败'}), 500
    finally:
        conn.close()


@app.route('/api/chart/product_analysis', methods=['GET'])
def get_product_analysis():
    conn = get_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = """
                SELECT subcategory, order_quantity, average_discount_rate
                FROM product_analysis
            """
            cursor.execute(sql)
            data = cursor.fetchall()

            # 数据处理
            subcategories = []
            quantities = []
            discount_rates = []
            for row in data:
                subcategories.append(row['subcategory'])
                quantities.append(int(row['order_quantity']))
                discount_rates.append(float(row['average_discount_rate']))
            return jsonify({
                'title': '产品子类别订单量分析',
                'yAxis': subcategories,
                'series': [{
                    'name': '订单量',
                    'type': 'bar',
                    'data': quantities
                }]
            })
    except Exception as e:
        print(e)
        return jsonify({'error': '获取数据失败'}), 500
    finally:
        conn.close()


@app.route('/api/chart/discount_bar_analysis', methods=['GET'])
def get_discount_bar_analysis():
    conn = get_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = """
                SELECT subcategory, average_discount_rate
                FROM product_analysis
            """
            cursor.execute(sql)
            data = cursor.fetchall()

            # 数据处理
            subcategories = []
            discount_rates = []

            for row in data:
                subcategories.append(row['subcategory'])
                # 将折扣率转换为百分比并保留两位小数
                discount_rates.append(round(float(row['average_discount_rate']) * 100, 2))

            return jsonify({
                'title': '产品子类别平均折扣率分析',
                'xAxis': subcategories,
                'series': [{
                    'name': '平均折扣率',
                    'type': 'bar',
                    'data': discount_rates,
                    'itemStyle': {
                        'borderRadius': [5, 5, 0, 0]
                    },
                    'label': {
                        'show': True,
                        'position': 'top',
                        'formatter': '{c}%'
                    }
                }]
            })
    except Exception as e:
        print(e)
        return jsonify({'error': '获取数据失败'}), 500
    finally:
        conn.close()


@app.route('/api/chart/client_analysis', methods=['GET'])
def get_client_analysis():
    conn = get_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = """
                SELECT segment, order_quantity
                FROM client_analysis
                ORDER BY order_quantity DESC
            """
            cursor.execute(sql)
            data = cursor.fetchall()

            # 数据处理
            segment = []
            order_quantity = []
            arr = []
            for row in data:
                arr.append({"name": row['segment'], "value": int(row['order_quantity'])})

            return jsonify({
                'series': [{
                    'data': arr
                }]
            })
    except Exception as e:
        print(e)
        return jsonify({'error': '获取数据失败'}), 500
    finally:
        conn.close()


@app.route('/api/chart/return_analysis', methods=['GET'])
def get_return_analysis():
    conn = get_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = """
                SELECT a.order_date_year, profit, profit_growth_rate,
                       return_quantity, return_rate 
                FROM annual_trend a
                JOIN return_analysis b
                ON a.order_date_year=b.order_date_year
                ORDER BY a.order_date_year
            """
            cursor.execute(sql)
            data = cursor.fetchall()

            # 数据处理
            years = []
            profit = []
            profit_growth_rate = []
            return_quantity = []
            return_rate = []

            for row in data:
                years.append(str(row['order_date_year']))
                profit.append(float(row['profit']))
                profit_growth_rate.append(float(row['profit_growth_rate']))
                return_quantity.append(int(row['return_quantity']))
                return_rate.append(float(row['return_rate']))

            return jsonify({
                'chart1': {
                    'xAxis': years,
                    'series': [
                        {
                            'name': '利润',
                            'type': 'bar',
                            'data': profit,
                            'yAxisIndex': 0
                        },
                        {
                            'name': '利润增长率',
                            'type': 'line',
                            'data': profit_growth_rate,
                            'yAxisIndex': 1
                        }
                    ]
                },
                'chart2': {
                    'xAxis': years,
                    'series': [
                        {
                            'name': '退货量',
                            'type': 'bar',
                            'data': return_quantity,
                            'yAxisIndex': 0
                        },
                        {
                            'name': '退货率',
                            'type': 'line',
                            'data': return_rate,
                            'yAxisIndex': 1
                        }
                    ]
                }
            })
    except Exception as e:
        print(e)
        return jsonify({'error': '获取数据失败'}), 500
    finally:
        conn.close()


@app.route('/api/chart/return_by_category', methods=['GET'])
def get_return_by_category():
    conn = get_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = """
                SELECT subcategory, return_quantity
                FROM product_return_analysis
                ORDER BY return_quantity DESC
            """
            cursor.execute(sql)
            data = cursor.fetchall()

            # 数据处理
            pie_data = []
            for row in data:
                pie_data.append({
                    'name': row['subcategory'],
                    'value': int(row['return_quantity'])
                })

            return jsonify({
                'title': '产品子类别退货量分布',
                'series': [{
                    'name': '退货量',
                    'type': 'pie',
                    'radius': ['40%', '70%'],  # 环形图
                    'avoidLabelOverlap': True,
                    'itemStyle': {
                        'borderRadius': 10,
                        'borderColor': '#fff',
                        'borderWidth': 2
                    },
                    'label': {
                        'show': True,
                        'formatter': '{b}: {c} ({d}%)'  # 显示名称、数值和百分比
                    },
                    'emphasis': {
                        'label': {
                            'show': True,
                            'fontSize': '16',
                            'fontWeight': 'bold'
                        }
                    },
                    'data': pie_data
                }]
            })
    except Exception as e:
        print(e)
        return jsonify({'error': '获取数据失败'}), 500
    finally:
        conn.close()


@app.route('/register')
def register_page():
    return render_template('register.html')


@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')


# API 路由
@app.route('/api/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({'error': '用户名和密码不能为空'}), 400

    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # 检查用户名是否存在
            cursor.execute('SELECT id FROM user WHERE username = %s', (username,))
            if cursor.fetchone():
                return jsonify({'error': '用户名已存在'}), 400

            # 创建新用户
            cursor.execute(
                'INSERT INTO user (username, password) VALUES (%s, %s)',
                (username, password)
            )
        conn.commit()
        return jsonify({'message': '注册成功'}), 201
    except Exception as e:
        print(e)
        conn.rollback()
        return jsonify({'error': '注册失败'}), 500
    finally:
        conn.close()


# 存储登录会话
active_sessions = {}


@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    conn = get_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute('SELECT * FROM user WHERE username = %s', (username,))
            user = cursor.fetchone()

            if user and user['password'] == password:
                # 检查是否已经登录
                for session_id, session in list(active_sessions.items()):
                    if session['username'] == username:
                        # 清除旧会话
                        del active_sessions[session_id]

                # 创建新会话
                session_id = str(uuid.uuid4())
                active_sessions[session_id] = {
                    'username': username,
                    'timestamp': time.time()
                }

                return jsonify({
                    'message': '登录成功',
                    'username': username,
                    'token': session_id
                }), 200

            return jsonify({'error': '用户名或密码错误'}), 401
    finally:
        conn.close()


# 添加会话检查接口
@app.route('/api/check_session', methods=['POST'])
def check_session():
    token = request.json.get('token')
    if token in active_sessions:
        active_sessions[token]['timestamp'] = time.time()
        return jsonify({'valid': True}), 200
    return jsonify({'valid': False}), 401


# 添加登出接口
@app.route('/api/logout', methods=['POST'])
def logout():
    token = request.json.get('token')
    if token in active_sessions:
        del active_sessions[token]
    return jsonify({'message': '登出成功'}), 200


@app.route('/api/chart/category_analysis', methods=['GET'])
def get_category_analysis():
    conn = get_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = """
                SELECT category, 
                       SUM(sales_amount_sum) as amount_num,
                       SUM(profit_sum) as profit_num 
                FROM product_analysis
                GROUP BY category
                ORDER BY amount_num DESC
            """
            cursor.execute(sql)
            data = cursor.fetchall()

            # 数据处理
            categories = []
            amount_nums = []
            profit_nums = []

            for row in data:
                categories.append(row['category'])
                amount_nums.append(float(row['amount_num']))
                profit_nums.append(float(row['profit_num']))

            return jsonify({
                'title': '类别销售额与利润分析',
                'xAxis': categories,
                'series': [
                    {
                        'name': '销售额',
                        'type': 'bar',
                        'data': amount_nums,
                        'itemStyle': {
                            'borderRadius': [5, 5, 0, 0]
                        },
                        'label': {
                            'show': True,
                            'position': 'top'
                        }
                    },
                    {
                        'name': '利润',
                        'type': 'bar',
                        'data': profit_nums,
                        'itemStyle': {
                            'borderRadius': [5, 5, 0, 0]
                        },
                        'label': {
                            'show': True,
                            'position': 'top'
                        }
                    }
                ]
            })
    except Exception as e:
        print(e)
        return jsonify({'error': '获取数据失败'}), 500
    finally:
        conn.close()


@app.route('/api/chart/china_map', methods=['GET'])
def get_china_map():
    conn = get_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = """
                SELECT province, sales_amount_sum
                FROM prov_sales
            """
            cursor.execute(sql)
            data = cursor.fetchall()

            # 数据处理
            map_data = []
            for row in data:
                # 使用reduce_prov函数处理省份名称
                province = reduce_prov(row['province'])
                map_data.append({
                    'name': province,
                    'value': round(float(row['sales_amount_sum']), 2)
                })

            return jsonify({
                'title': '全国销售额分布',
                'series': [{
                    'name': '销售额',
                    'type': 'map',
                    'map': 'china',
                    'roam': True,  # 开启鼠标缩放和平移漫游
                    'label': {
                        'show': True,  # 显示省份名称
                        'fontSize': 8
                    },
                    'emphasis': {  # 高亮状态
                        'itemStyle': {
                            'areaColor': '#66ccff'
                        }
                    },
                    'data': map_data
                }]
            })
    except Exception as e:
        print(e)
        return jsonify({'error': '获取数据失败'}), 500
    finally:
        conn.close()


@app.route('/api/chart/predict', methods=['GET'])
def get_predict():
    conn = get_db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = """
                SELECT name, price,predict
                FROM t_predict
                WHERE predict>0 LIMIT 20
            """
            cursor.execute(sql)
            data = cursor.fetchall()

            # 数据处理
            x_data = []
            y1_data = []
            y2_data = []
            for row in data:
                x_data.append(row["name"])
                y1_data.append(row["price"])
                y2_data.append(row["predict"])

            return jsonify({
                "title": {
                    "text": "实际价格与预测价格对比"
                },
                "tooltip": {
                    "trigger": "axis"
                },
                "legend": {
                    "data": ["实际价格", "预测价格"]
                },
                "grid": {
                    "left": "3%",
                    "right": "4%",
                    "bottom": "3%",
                    "containLabel": True
                },
                "xAxis": {
                    "type": "category",
                    "boundaryGap": False,
                    "data": x_data
                },
                "yAxis": {
                    "type": "value"
                },
                "series": [
                    {
                        "name": "实际价格",
                        "type": "line",
                        "data": y1_data
                    },
                    {
                        "name": "预测价格",
                        "type": "line",
                        "data": y2_data
                    }
                ]
            })
    except Exception as e:
        print(e)
        return jsonify({'error': '获取数据失败'}), 500
    finally:
        conn.close()


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        app.run(debug=True, host="127.0.0.1", port=5173)
    elif sys.argv[1] == "pro":
        app.run(debug=True, host="master", port=5173)
