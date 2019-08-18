#!/usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = 'James Iter'
__date__ = '2018/4/21'
__contact__ = 'james.iter.cn@gmail.com'
__copyright__ = '(c) 2018 by James Iter.'


class MAPump(object):

    def __init__(self, step=None):
        self.step = step
        self.numbers = list()

    def process_data(self, k_line=None, field='close'):
        """
        :param k_line:
        :param field:
        :return:
        """

        assert isinstance(k_line, dict)

        number = k_line[field]
        assert isinstance(number, (int, float))

        date_time = k_line['date_time']

        self.numbers.append(number)

        if self.numbers.__len__() < self.step:
            return {'date_time': date_time, 'avg': float('%0.2f' % (float(sum(self.numbers)) / self.numbers.__len__()))}

        else:
            self.numbers = self.numbers[0 - self.step:]
            return {'date_time': date_time, 'avg': float('%0.2f' % (float(sum(self.numbers)) / self.numbers.__len__()))}

