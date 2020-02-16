#!/usr/bin/env python
# -*- coding: utf-8 -*-


import getopt
import sys
import os
import json


__author__ = 'James Iter'
__date__ = '2018-12-01'
__contact__ = 'james.iter.cn@gmail.com'
__copyright__ = '(c) 2018 by James Iter.'


def incept_config():
    config = {
        'interval': '13'
    }

    def usage():
        print("Usage:%s [-s] [--data_source]" % sys.argv[0])
        print("-s --data_source, is the path of data file.")
        print("-o --output_file, is the output file. optional.")
        print("-i --interval, default is 13 minutes, delimiter is a comma. optional.")

    opts = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hs:o:i:',
                                   ['help', 'data_source=', 'output_file=', 'interval='])
    except getopt.GetoptError as e:
        print(str(e))
        usage()
        exit(e.message.__len__())

    for k, v in opts:
        if k in ("-h", "--help"):
            usage()
            exit()

        elif k in ("-s", "--data_source"):
            config['data_source'] = v

        elif k in ("-o", "--output_file"):
            config['output_dir'] = v

        elif k in ("-i", "--interval"):
            config['interval'] = v

        else:
            print("unhandled option")

    if 'data_source' not in config:
        print('Must specify the -s(data_source) arguments.')
        usage()
        exit(1)

    if 'output_file' not in config:
        inst_interval = int(config['interval'])*60
        config['output_file'] = '_'.join([os.path.basename(
            config['data_source']).split('_')[0], str(inst_interval)]) + '.json'


    config['interval'] = int(config['interval'])

    return config


def run():
    config = incept_config()

    save_path = os.path.join(os.getcwd(), config['output_file'])

    if not os.path.isdir(os.path.dirname(save_path)):
        os.makedirs(os.path.dirname(save_path), 0o755)

    with open(config['data_source']) as f:
        k_line = None

        for i, line in enumerate(f):
            # 忽略 csv 头
            # if i == 0:
            #     continue

            i += 1
            last_k_line = json.loads(line.strip())

            if k_line is None:
                k_line = last_k_line

            if i % config['interval'] == 0:
                k_line = last_k_line

                with open(save_path, 'a') as f2:
                    f2.writelines(json.dumps(k_line, ensure_ascii=False) + '\n')

            else:
                k_line['date_time'] = last_k_line['date_time']
                k_line['close'] = last_k_line['close']

                if last_k_line['high'] > k_line['high']:
                    k_line['high'] = last_k_line['high']

                elif last_k_line['low'] < k_line['low']:
                    k_line['low'] = last_k_line['low']

                else:
                    pass

    pass


if __name__ == '__main__':
    run()

