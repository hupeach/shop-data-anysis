// 检查登录状态
if (localStorage.getItem('isLoggedIn') !== 'true') {
    window.location.href = '/';
}

// 显示用户名
document.getElementById('username-display').textContent = 
    localStorage.getItem('username');

// 初始化图表
const chartDom = document.getElementById('chartContainer');
const myChart = echarts.init(chartDom);

// 侧边栏切换
document.getElementById('toggleSidebar').addEventListener('click', function() {
    const sidebar = document.getElementById('sidebar');
    const content = document.querySelector('.content');
    
    sidebar.classList.toggle('collapsed');
    content.classList.toggle('expanded');
    
    // 重新调整图表大小
    setTimeout(() => {
        myChart.resize();
    }, 300); // 等待过渡动画完成
});

// 监听窗口大小变化
window.addEventListener('resize', function() {
    myChart.resize();
});

// 切换图表
async function renderCategoryAnalysis() {
    try {
        const response = await fetch('/api/chart/category_analysis');
        const data = await response.json();

        const option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                }
            },
            legend: {
                data: ['销售额', '利润'],
                top: '30px'
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                containLabel: true
            },
            xAxis: {
                type: 'category',
                data: data.xAxis,
                axisLabel: {
                    interval: 0,
                    rotate: 30
                }
            },
            yAxis: {
                type: 'value',
                name: '金额'
            },
            series: data.series
        };
        myChart.setOption(option);
    } catch (error) {
        console.error('获取数据失败:', error);
    }
}

// 渲染折线图
async function renderLineChart() {
    try {
        const response = await fetch('/api/chart/line');
        const data = await response.json();

        const option = {
            title: {
                text: data.title
            },
            xAxis: {
                type: 'category',
                data: data.xAxis
            },
            yAxis: {
                type: 'value'
            },
            series: [{
                data: data.series[0].data,
                type: 'line',
                smooth: true
            }]
        };
        myChart.setOption(option);
    } catch (error) {
        console.error('获取折线图数据失败:', error);
    }
}

// 渲染饼图
async function renderClientAnalysis() {
    try {
        const response = await fetch('/api/chart/client_analysis');
        const data = await response.json();

        const option = {
            title: {
                text: data.title
            },
            tooltip: {
                trigger: 'item'
            },
            series: [{
                type: 'pie',
                radius: '50%',
                data: data.series[0].data
            }]
        };
        myChart.setOption(option);
    } catch (error) {
        console.error('获取饼图数据失败:', error);
    }
}




// 渲染饼图
async function renderAnnual() {
    try {
        const response = await fetch('/api/chart/annual_trend');
        const data = await response.json();

        const option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'cross'
                }
            },
            grid: [{
                top: '10%',
                bottom: '55%'
            }, {
                top: '60%',
                bottom: '5%'
            }],
            xAxis: [{
                type: 'category',
                data: data.chart1.xAxis,
                gridIndex: 0
            }, {
                type: 'category',
                data: data.chart2.xAxis,
                gridIndex: 1
            }],
            yAxis: [{
                type: 'value',
                name: '订单数量',
                gridIndex: 0
            }, {
                type: 'value',
                name: '增长率(%)',
                position: 'right',
                gridIndex: 0
            }, {
                type: 'value',
                name: '销售额',
                gridIndex: 1
            }, {
                type: 'value',
                name: '增长率(%)',
                position: 'right',
                gridIndex: 1
            }],
            legend: [{
                data: ['订单数量', '订单增长率'],
                top: '3%'
            }, {
                data: ['销售额', '销售额增长率'],
                top: '53%'
            }],
            series: [
                {
                    name: '订单数量',
                    type: 'bar',
                    data: data.chart1.series[0].data,
                    xAxisIndex: 0,
                    yAxisIndex: 0
                },
                {
                    name: '订单增长率',
                    type: 'line',
                    data: data.chart1.series[1].data,
                    xAxisIndex: 0,
                    yAxisIndex: 1
                },
                {
                    name: '销售额',
                    type: 'bar',
                    data: data.chart2.series[0].data,
                    xAxisIndex: 1,
                    yAxisIndex: 2
                },
                {
                    name: '销售额增长率',
                    type: 'line',
                    data: data.chart2.series[1].data,
                    xAxisIndex: 1,
                    yAxisIndex: 3
                }
            ]
        };
        myChart.setOption(option);
    } catch (error) {
        console.error('获取饼图数据失败:', error);
    }
}




// 渲染饼图
async function renderReturnAnalysis() {
    try {
        const response = await fetch('/api/chart/return_analysis');
        const data = await response.json();

        const option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'cross'
                }
            },
            grid: [{
                top: '10%',
                bottom: '55%'
            }, {
                top: '60%',
                bottom: '5%'
            }],
            xAxis: [{
                type: 'category',
                data: data.chart1.xAxis,
                gridIndex: 0
            }, {
                type: 'category',
                data: data.chart2.xAxis,
                gridIndex: 1
            }],
            yAxis: [{
                type: 'value',
                name: '利润',
                gridIndex: 0
            }, {
                type: 'value',
                name: '增长率(%)',
                position: 'right',
                gridIndex: 0
            }, {
                type: 'value',
                name: '退货量',
                gridIndex: 1
            }, {
                type: 'value',
                name: '退货率(%)',
                position: 'right',
                gridIndex: 1
            }],
            legend: [{
                data: ['利润', '利润增长率'],
                top: '3%'
            }, {
                data: ['退货量', '退货率'],
                top: '53%'
            }],
            series: [
                {
                    name: '利润',
                    type: 'bar',
                    data: data.chart1.series[0].data,
                    xAxisIndex: 0,
                    yAxisIndex: 0
                },
                {
                    name: '利润增长率',
                    type: 'line',
                    data: data.chart1.series[1].data,
                    xAxisIndex: 0,
                    yAxisIndex: 1
                },
                {
                    name: '退货量',
                    type: 'bar',
                    data: data.chart2.series[0].data,
                    xAxisIndex: 1,
                    yAxisIndex: 2
                },
                {
                    name: '退货率',
                    type: 'line',
                    data: data.chart2.series[1].data,
                    xAxisIndex: 1,
                    yAxisIndex: 3
                }
            ]
        };
        myChart.setOption(option);
    } catch (error) {
        console.error('获取饼图数据失败:', error);
    }
}



// 渲染饼图
async function renderMonthlySales() {
    try {
        const response = await fetch('/api/chart/monthly_sales');
        const data = await response.json();

        const option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'cross'
                }
            },
            legend: {
                data: data.legend,
                top: '30px'
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                containLabel: true
            },
            xAxis: {
                type: 'category',
                boundaryGap: false,
                data: data.xAxis,
                axisLabel: {
                    interval: 0
                }
            },
            yAxis: {
                type: 'value',
                name: '销售额'
            },
            series: data.series
        };
        myChart.setOption(option);
    } catch (error) {
        console.error('获取饼图数据失败:', error);
    }
}



// 渲染饼图
async function renderProductAnalysis() {
    try {
        const response = await fetch('/api/chart/product_analysis');
        const data = await response.json();

        const option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                }
            },
            grid: {
                left: '15%',
                right: '4%',
                bottom: '3%',
                containLabel: true
            },
            xAxis: {
                type: 'value',
                name: '订单量'
            },
            yAxis: {
                type: 'category',
                data: data.yAxis,
                axisLabel: {
                    interval: 0,
                    rotate: 0
                }
            },
            series: data.series
        };
        myChart.setOption(option);
    } catch (error) {
        console.error('获取饼图数据失败:', error);
    }
}



// 渲染饼图
async function renderReturnByCategory() {
    try {
        const response = await fetch('/api/chart/return_by_category');
        const data = await response.json();

        const option = {
            tooltip: {
                trigger: 'item',
                formatter: '{a} <br/>{b}: {c} ({d}%)'
            },
            legend: {
                orient: 'vertical',
                left: 'left',
                top: 'middle'
            },
            series: data.series
        };
        myChart.setOption(option);
    } catch (error) {
        console.error('获取饼图数据失败:', error);
    }
}

// 渲染饼图
async function renderDiscountAnalysis() {
    try {
        const response = await fetch('/api/chart/discount_bar_analysis');
        const data = await response.json();

        const option = {
            title: {
                text: data.title,
                left: 'center'
            },
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                }
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '15%',
                containLabel: true
            },
            xAxis: {
                type: 'category',
                data: data.xAxis,
                axisLabel: {
                    interval: 0,
                    rotate: 45
                },
                axisTick: {
                    alignWithLabel: true
                }
            },
            yAxis: {
                type: 'value',
                name: '折扣率(%)'
            },
            dataZoom: [
                {
                    type: 'inside',
                    xAxisIndex: [0],
                    zoomOnMouseWheel: true,  // 启用鼠标滚轮缩放
                    moveOnMouseMove: true,    // 启用鼠标拖动
                    moveOnMouseWheel: false   // 禁用按住 Ctrl 时的整体移动
                }
            ],
            series: data.series
        };
        myChart.setOption(option);
    } catch (error) {
        console.error('获取饼图数据失败:', error);
    }
}


// 渲染饼图
async function renderPredict() {
    try {
        const response = await fetch('/api/chart/predict');
        const option = await response.json();

        myChart.setOption(option);
    } catch (error) {
        console.error('获取饼图数据失败:', error);
    }
}


async function renderChinaMap() {
    try {
        const response = await fetch('/api/chart/china_map');
        const data = await response.json();

        const option = {
            title: {
                text: data.title,
            },
            tooltip: {
                trigger: 'item',
                formatter: '{b}<br/>销售额：{c}'
            },
            visualMap: {
                min: 0,
                max: Math.max(...data.series[0].data.map(item => item.value)),
                left: 'left',
                top: 'bottom',
                text: ['高', '低'],
                calculable: true,
                inRange: {
                    color: ['#e0ffff', '#006edd']
                }
            },
            series: data.series
        };
        myChart.setOption(option);
    } catch (error) {
        console.error('获取数据失败:', error);
    }
}

// 修改切换图表函数为异步
async function switchChart(type) {
    const navItems = document.querySelectorAll('.nav-item');
    navItems.forEach(item => item.classList.remove('active'));
    event.target.classList.add('active');

    myChart.clear();
    switch(type) {
        case 'category_analysis':
            await renderCategoryAnalysis();
            break;
        case 'china_map':
            await renderChinaMap();
            break;
        case 'client_analysis':
            await renderClientAnalysis();
            break;
        case 'annual':
            await renderAnnual();
            break;
        case 'return_analysis':
            await renderReturnAnalysis();
            break;
        case 'monthly_sales':
            await renderMonthlySales();
            break;
        case 'product_analysis':
            await renderProductAnalysis();
            break;
        case 'return_by_category':
            await renderReturnByCategory();
            break;
        case 'discount_bar_analysis':
            await renderDiscountAnalysis();
            break;
        case 'predict':
            await renderPredict();
            break;
    }
}

// 修改初始化调用
(async () => {
    await renderCategoryAnalysis();
})();

// 处理退出登录
// 会话检查函数
async function checkSession() {
    const token = localStorage.getItem('token');
    if (!token) {
        window.location.href = '/';
        return;
    }

    try {
        const response = await fetch('/api/check_session', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ token })
        });

        if (!response.ok) {
            alert('您的账号已在其他地方登录');
            handleLogout();
        }
    } catch (error) {
        console.error('会话检查失败:', error);
    }
}

// 定期检查会话
setInterval(checkSession, 30000);  // 每30秒检查一次

// 修改登出函数
async function handleLogout() {
    const token = localStorage.getItem('token');
    try {
        await fetch('/api/logout', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ token })
        });
    } finally {
        localStorage.removeItem('isLoggedIn');
        localStorage.removeItem('username');
        localStorage.removeItem('token');
        window.location.href = '/';
    }
}

// 初始化显示柱状图
renderCategoryAnalysis();

// 监听窗口大小变化，调整图表大小
window.addEventListener('resize', () => {
    myChart.resize();
});