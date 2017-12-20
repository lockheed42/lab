<?php
/**
 * Created by PhpStorm.
 * User: lockheed
 * Date: 2017/11/23
 * Time: 16:22
 */

/**
 * https://baike.baidu.com/item/埃拉托色尼筛选法
 * 素数筛选法
 */


function gen($n)
{
    $array = [];
    for ($i = 3; $i < $n; $i += 2) {
        $array[] = $i;
    }

    return $array;
}

function prime($input)
{
    $return = [];
    while (count($input) != 0) {
        $current = array_shift($input);
        $return[] = $current;
        $input = array_filter($input, function ($x) use ($current) {
            return $x % $current != 0;
        });
    }
    return $return;
}

$array = gen(500000);
print_r(prime($array));