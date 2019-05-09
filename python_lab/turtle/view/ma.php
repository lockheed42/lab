<?php
//直接在界面生成K线和买点卖点

//参数
$code = $_GET['code']??'';
$model = $_GET['model']??'';

function mysql_read($sql)
{
    $DB_HOST = '127.0.0.1';
    $DB_USER = 'root';
    $DB_PWD = '';
    $DB_NAME = 'test';

    $link = mysqli_connect($DB_HOST, $DB_USER, $DB_PWD) or die('Could not connect: ' . mysqli_error($link));
    mysqli_select_db($link, $DB_NAME) or die ('Can\'t use foo : ' . mysqli_error($link));
    mysqli_set_charset($link, 'UTF-8');
    return mysqli_query($link, $sql);
}

$kData = '[';
$rs = mysql_read("select * from src_base_day where code = '{$code}'");
while ($row = mysqli_fetch_assoc($rs)) {
    $kData .= "['${row['date']}', ${row['open']}, ${row['close']}, ${row['low']}, ${row['high']}],\n";
}
$kData .= "]";


$trigger = '[';
$rs = mysql_read('select buy_date, sell_date, buy_trigger,sell_trigger  from rpt_test_detail where code = "' . $code . '" and model_code = "' . $model . '"');
while ($row = mysqli_fetch_assoc($rs)) {
    $trigger .= "{name: 'XX',coord: ['" . $row['buy_date'] . "', " . $row['buy_trigger'] . "],value: " . $row['buy_trigger'] . ",itemStyle: {normal: {color: 'rgb(30,144,255)'}}},\n";
    $trigger .= "{name: 'XX',coord: ['" . $row['sell_date'] . "', " . $row['sell_trigger'] . "],value: " . $row['sell_trigger'] . ",itemStyle: {normal: {color: 'rgb(255,99,71)'}}},\n";
}
$trigger .= ']';

?>
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>ECharts</title>
    <!-- 引入 echarts.js -->
    <script src="echarts/echarts.js"></script>
</head>
<body>
    <!-- 为ECharts准备一个具备大小（宽高）的Dom -->
    <div id="main" style="width: 1500px;height:800px;"></div>
    <script type="text/javascript">
        // 基于准备好的dom，初始化echarts实例
        var myChart = echarts.init(document.getElementById('main'));

        var upColor = '#ec0000';
        var upBorderColor = '#8A0000';
        var downColor = '#00da3c';
        var downBorderColor = '#008F28';


        // 数据意义：开盘(open)，收盘(close)，最低(lowest)，最高(highest)
        var data0 = splitData(<?=$kData?>);


        function splitData(rawData) {
            var categoryData = [];
            var values = []
            for (var i = 0; i < rawData.length; i++) {
                categoryData.push(rawData[i].splice(0, 1)[0]);
                values.push(rawData[i])
            }
            return {
                categoryData: categoryData,
                values: values
            };
        }

        function calculateMA(dayCount) {
            var result = [];
            for (var i = 0, len = data0.values.length; i < len; i++) {
                if (i < dayCount) {
                    result.push('-');
                    continue;
                }
                var sum = 0;
                for (var j = 0; j < dayCount; j++) {
                    sum += data0.values[i - j][1];
                }
                result.push(sum / dayCount);
            }
            return result;
        }



        option = {
            title: {
                text: '上证指数',
                left: 0
            },
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'cross'
                }
            },
            legend: {
                data: ['日K', 'MA20', 'MA30', 'MA60']
            },
            grid: {
                left: '10%',
                right: '10%',
                bottom: '15%'
            },
            xAxis: {
                type: 'category',
                data: data0.categoryData,
                scale: true,
                boundaryGap : false,
                axisLine: {onZero: false},
                splitLine: {show: false},
                splitNumber: 20,
                min: 'dataMin',
                max: 'dataMax'
            },
            yAxis: {
                scale: true,
                splitArea: {
                    show: true
                }
            },
            dataZoom: [
                {
                    type: 'inside',
                    start: 50,
                    end: 100
                },
                {
                    show: true,
                    type: 'slider',
                    y: '90%',
                    start: 50,
                    end: 100
                }
            ],
            series: [
                {
                    name: '日K',
                    type: 'candlestick',
                    data: data0.values,
                    itemStyle: {
                        normal: {
                            color: upColor,
                            color0: downColor,
                            borderColor: upBorderColor,
                            borderColor0: downBorderColor
                        }
                    },
                    markPoint: {
                        label: {
                            normal: {
                                formatter: function (param) {
                                    return param != null ? param.value : '';
                                }
                            }
                        },
                        data: <?=$trigger?>,
                        tooltip: {
                            formatter: function (param) {
                                return param.name + '<br>' + (param.data.coord || '');
                            }
                        }
                    },
                },
                {
                    name: 'MA20',
                    type: 'line',
                    data: calculateMA(20),
                    smooth: true,
                    lineStyle: {
                        normal: {opacity: 0.5}
                    }
                },
                {
                    name: 'MA30',
                    type: 'line',
                    data: calculateMA(30),
                    smooth: true,
                    lineStyle: {
                        normal: {opacity: 0.5}
                    }
                },
                {
                    name: 'MA60',
                    type: 'line',
                    data: calculateMA(60),
                    smooth: true,
                    lineStyle: {
                        normal: {opacity: 0.5}
                    }
                },

            ]
        };


        console.log(option);
        // 使用刚指定的配置项和数据显示图表。
        myChart.setOption(option);
    </script>
</body>
</html>