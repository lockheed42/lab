#!/usr/bin/env python3
# -*- coding:utf-8 -*-

from multiprocessing import Pool
import os, time, random

import multiprocessing
import time

'''
批量创建子进程 与 对应的pipe
'''


def proc(pipe):
    is_continue = True
    while is_continue:
        message = pipe.recv()
        if random.random() > 0.7:
            time.sleep(5)
        print("proc2 rev:", message)
        # print("pipe:", pipe)
        if message == '---end---':
            is_continue = False


process_num = 4
process_pool = Pool(process_num)
pipe_pool = {}
for i in range(process_num):
    time.sleep(0.5)
    pipe_pool[i] = multiprocessing.Pipe()
    # print(pipe_pool[i])
    process_pool.apply_async(proc, args=(pipe_pool[i][1],))

limit = 40
for i in range(limit):
    pipe_num = i % process_num
    # print(pipe_pool[pipe_num][0])
    pipe_pool[pipe_num][0].send(i)

    if i == limit - 1:
        for j in range(process_num):
            pipe_pool[j][0].send('---end---')

process_pool.close()
process_pool.join()

exit()


def long_time_task(name):
    print('Run task %s (%s)...' % (name, os.getpid()))
    start = time.time()
    time.sleep(random.random() * 1)
    end = time.time()
    print('Task %s runs %0.2f seconds.' % (name, (end - start)))


if __name__ == '__main__':
    print('Parent process %s.' % os.getpid())
    p = Pool(16)
    for i in range(16):
        p.apply_async(long_time_task, args=(i,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
