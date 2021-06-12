import datetime
import os
import sys
import time
from typing import Union, Optional, List, Dict, Tuple, TextIO, Any

import numpy
import pandas
import requests
import threading


class NseConsts:
    url_oc: str = "https://www.nseindia.com/option-chain"
    url_index: str = "https://www.nseindia.com/api/option-chain-indices?symbol="
    url_stock: str = "https://www.nseindia.com/api/option-chain-equities?symbol="
    headers: Dict[str, str] = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                      'like Gecko) Chrome/80.0.3987.149 Safari/537.36',
        'accept-language': 'en,gu;q=0.9,hi;q=0.8',
        'accept-encoding': 'gzip, deflate, br'}
    units_str: str = 'in K'
    index = "NIFTY"


class NseData:
    def __init__(self) -> None:
        self.interval = 5
        self.stdout: TextIO = sys.stdout
        self.stderr: TextIO = sys.stderr
        self.previous_date: Optional[datetime.date] = None
        self.previous_time: Optional[datetime.time] = None
        self.first_run: bool = True
        self.stop: bool = False
        self.auto_stop: bool = False
        self.dates: List[str] = [""]
        self.logging = False
        self.output_columns: Tuple[str, str, str, str, str, str, str, str, str] = (
            'Time', 'Value', f'Call Sum\n({NseConsts.units_str})', f'Put Sum\n({NseConsts.units_str})',
            f'Difference\n({NseConsts.units_str})', f'Call Boundary\n({NseConsts.units_str})',
            f'Put Boundary\n({NseConsts.units_str})', 'Call ITM', 'Put ITM')
        self.csv_headers: Tuple[str, str, str, str, str, str, str, str, str] = (
            'Time', 'Value', f'Call Sum ({NseConsts.units_str})', f'Put Sum ({NseConsts.units_str})',
            f'Difference ({NseConsts.units_str})',
            f'Call Boundary ({NseConsts.units_str})', f'Put Boundary ({NseConsts.units_str})', 'Call ITM', 'Put ITM')
        self.session: requests.Session = requests.Session()
        self.cookies: Dict[str, str] = {}
        self.round_factor: int = 1000  # 10 for stocks
        self.expiry_date = "15-Apr-2021"
        self.index = NseConsts.index
        self.sp = 15500
        self.entire_oc: pandas.DataFrame = None
        self.str_current_time: str = ""
        self.points: float = "0"
        self.debug = True
        self.trace = False
        self.error = True

    def print_debug(self, message):
        if self.debug:
            print(message)

    def print_trace(self, message):
        if self.trace:
            print(message)

    # noinspection PyUnusedLocal
    def get_data(self) -> Optional[Tuple[Optional[requests.Response], Any]]:
        if self.first_run:
            return self.get_data_first_run()
        else:
            return self.get_data_refresh()

    def refresh_session(self):
        request: Optional[requests.Response] = None
        # Close previous session if applicable
        if not self.first_run:
            self.session.close()
        # Create a new session
        try:
            self.session = requests.Session()
            request = self.session.get(NseConsts.url_oc, headers=NseConsts.headers, timeout=5)
            self.cookies = dict(request.cookies)
            self.print_debug("reset cookies done...")
        except Exception as err:
            self.print_trace(request)
            print(err, sys.exc_info()[0], "1")

    def get_data_first_run(self) -> Optional[Tuple[Optional[requests.Response], Any]]:
        self.refresh_session()
        request: Optional[requests.Response] = None
        response: Optional[requests.Response] = None
        url: str = NseConsts.url_index + self.index
        try:
            response = self.session.get(url, headers=NseConsts.headers, timeout=5, cookies=self.cookies)
        except Exception as err:
            self.print_debug(request)
            self.print_debug(response)
            print(err, sys.exc_info()[0], "1")
            self.dates.clear()
            self.dates = [""]
            return
        json_data: Any
        if response is not None:
            try:
                json_data = response.json()
                self.print_trace(json_data)
            except Exception as err:
                self.print_debug(response)
                print(err, sys.exc_info()[0], "2")
                json_data = {}
        else:
            json_data = {}
        if json_data == {}:
            self.dates.clear()
            self.dates = [""]
            return
        self.dates.clear()
        for dates in json_data['records']['expiryDates']:
            self.dates.append(dates)

        self.expiry_date = self.dates[0]
        self.print_debug("Date configured as :" + str(self.expiry_date))
        return response, json_data

    def get_data_refresh(self) -> Optional[Tuple[Optional[requests.Response], Any]]:
        request: Optional[requests.Response] = None
        response: Optional[requests.Response] = None
        # For stock =>  url: str = self.url_stock + self.stock
        url: str = NseConsts.url_index + self.index
        try:
            response = self.session.get(url, headers=NseConsts.headers, timeout=5, cookies=self.cookies)
            if response.status_code == 401:
                self.refresh_session()
                response = self.session.get(url, headers=NseConsts.headers, timeout=5, cookies=self.cookies)
        except Exception as err:
            self.print_debug(request)
            self.print_debug(response)
            print(err, sys.exc_info()[0], "4")
            try:
                self.refresh_session()
                response = self.session.get(url, headers=NseConsts.headers, timeout=5, cookies=self.cookies)
            except Exception as err:
                self.print_debug(request)
                self.print_debug(response)
                print(err, sys.exc_info()[0], "5")
                return
        if response is not None:
            try:
                json_data: Any = response.json()
            except Exception as err:
                self.print_debug(str(response))
                print(err, sys.exc_info()[0], "6")
                json_data = {}
        else:
            json_data = {}
        if json_data == {}:
            return

        return response, json_data

    def get_dataframe(self) -> Optional[Tuple[pandas.DataFrame, str, float]]:
        try:
            response: Optional[requests.Response]
            json_data: Any
            response, json_data = self.get_data()
        except TypeError:
            return
        if response is None or json_data is None:
            return

        pandas.set_option('display.max_rows', None)
        pandas.set_option('display.max_columns', None)
        pandas.set_option('display.width', 400)

        df: pandas.DataFrame = pandas.read_json(response.text)
        df = df.transpose()

        ce_values: List[dict] = [data['CE'] for data in json_data['records']['data'] if
                                 "CE" in data and str(data['expiryDate'].lower() == str(self.expiry_date).lower())]
        pe_values: List[dict] = [data['PE'] for data in json_data['records']['data'] if
                                 "PE" in data and str(data['expiryDate'].lower() == str(self.expiry_date).lower())]
        points: float = pe_values[0]['underlyingValue']
        if points == 0:
            for item in pe_values:
                if item['underlyingValue'] != 0:
                    points = item['underlyingValue']
                    break
        ce_data: pandas.DataFrame = pandas.DataFrame(ce_values)
        pe_data: pandas.DataFrame = pandas.DataFrame(pe_values)
        ce_data_f: pandas.DataFrame = ce_data.loc[ce_data['expiryDate'] == self.expiry_date]
        pe_data_f: pandas.DataFrame = pe_data.loc[pe_data['expiryDate'] == self.expiry_date]
        if ce_data_f.empty:
            print("Invalid Expiry Date.\nPlease restart and enter a new Expiry Date.")
            return
        columns_ce: List[str] = ['openInterest', 'changeinOpenInterest', 'totalTradedVolume', 'impliedVolatility',
                                 'lastPrice',
                                 'change', 'bidQty', 'bidprice', 'askPrice', 'askQty', 'strikePrice']
        columns_pe: List[str] = ['strikePrice', 'bidQty', 'bidprice', 'askPrice', 'askQty', 'change', 'lastPrice',
                                 'impliedVolatility', 'totalTradedVolume', 'changeinOpenInterest', 'openInterest']
        ce_data_f = ce_data_f[columns_ce]
        pe_data_f = pe_data_f[columns_pe]
        merged_inner: pandas.DataFrame = pandas.merge(left=ce_data_f, right=pe_data_f, left_on='strikePrice',
                                                      right_on='strikePrice')
        merged_inner.columns = ['Open Interest', 'Change in Open Interest', 'Traded Volume', 'Implied Volatility',
                                'Last Traded Price', 'Net Change', 'Bid Quantity', 'Bid Price', 'Ask Price',
                                'Ask Quantity', 'Strike Price', 'Bid Quantity', 'Bid Price', 'Ask Price',
                                'Ask Quantity', 'Net Change', 'Last Traded Price', 'Implied Volatility',
                                'Traded Volume', 'Change in Open Interest', 'Open Interest']
        current_time: str = df['timestamp']['records']
        return merged_inner, current_time, points

    def set_values(self) -> None:
        self.old_max_call_oi_sp: numpy.float64
        self.old_max_call_oi_sp_2: numpy.float64
        self.old_max_put_oi_sp: numpy.float64
        self.old_max_put_oi_sp_2: numpy.float64

        self.new_max_call_oi_sp: numpy.bool
        self.new_max_call_oi_sp_2: numpy.bool
        self.new_max_put_oi_sp: numpy.bool
        self.new_max_put_oi_sp_2: numpy.bool

        if self.first_run or self.old_max_call_oi_sp == self.max_call_oi_sp:
            self.old_max_call_oi_sp = self.max_call_oi_sp
            self.new_max_call_oi_sp = False
        else:
            self.old_max_call_oi_sp = self.max_call_oi_sp
            self.new_max_call_oi_sp = True

        if self.first_run or self.old_max_call_oi_sp_2 == self.max_call_oi_sp_2:
            self.old_max_call_oi_sp_2 = self.max_call_oi_sp_2
            self.new_max_call_oi_sp_2 = False
        else:
            self.old_max_call_oi_sp_2 = self.max_call_oi_sp_2
            self.new_max_call_oi_sp_2 = True

        if self.first_run or self.old_max_put_oi_sp == self.max_put_oi_sp:
            self.old_max_put_oi_sp = self.max_put_oi_sp
            self.new_max_put_oi_sp = False
        else:
            self.old_max_put_oi_sp = self.max_put_oi_sp
            self.new_max_put_oi_sp = True

        if self.first_run or self.old_max_put_oi_sp_2 == self.max_put_oi_sp_2:
            self.old_max_put_oi_sp_2 = self.max_put_oi_sp_2
            self.new_max_put_oi_sp_2 = False
        else:
            self.old_max_put_oi_sp_2 = self.max_put_oi_sp_2
            self.new_max_put_oi_sp_2 = True

    def execute_one_step(self) -> None:
        if self.stop:
            return
        try:
            entire_oc, current_time, self.points = self.get_dataframe()
        except TypeError:
            return

        self.entire_oc = entire_oc
        self.str_current_time: str = current_time.split(" ")[1]
        current_date: datetime.date = datetime.datetime.strptime(current_time.split(" ")[0], '%d-%b-%Y').date()
        current_time: datetime.time = datetime.datetime.strptime(current_time.split(" ")[1], '%H:%M:%S').time()
        if self.first_run:
            self.previous_date = current_date
            self.previous_time = current_time
        elif current_date > self.previous_date:
            self.previous_date = current_date
            self.previous_time = current_time
        elif current_date == self.previous_date:
            if current_time > self.previous_time:
                self.previous_time = current_time
            else:
                return

        call_oi_list: List[int] = []
        for i in range(len(entire_oc)):
            int_call_oi: int = int(entire_oc.iloc[i, [0]][0])
            call_oi_list.append(int_call_oi)
        call_oi_index: int = call_oi_list.index(max(call_oi_list))
        self.max_call_oi: float = round(max(call_oi_list) / self.round_factor, 1)
        self.max_call_oi_sp: numpy.float64 = entire_oc.iloc[call_oi_index]['Strike Price']

        put_oi_list: List[int] = []
        for i in range(len(entire_oc)):
            int_put_oi: int = int(entire_oc.iloc[i, [20]][0])
            put_oi_list.append(int_put_oi)
        put_oi_index: int = put_oi_list.index(max(put_oi_list))
        self.max_put_oi: float = round(max(put_oi_list) / self.round_factor, 1)
        self.max_put_oi_sp: numpy.float64 = entire_oc.iloc[put_oi_index]['Strike Price']

        sp_range_list: List[numpy.float64] = []
        for i in range(put_oi_index, call_oi_index + 1):
            sp_range_list.append(entire_oc.iloc[i]['Strike Price'])

        self.max_call_oi_2: float
        self.max_call_oi_sp_2: numpy.float64
        self.max_put_oi_2: float
        self.max_put_oi_sp_2: numpy.float64
        if self.max_call_oi_sp == self.max_put_oi_sp:
            self.max_call_oi_2 = self.max_call_oi
            self.max_call_oi_sp_2 = self.max_call_oi_sp
            self.max_put_oi_2 = self.max_put_oi
            self.max_put_oi_sp_2 = self.max_put_oi_sp
        elif len(sp_range_list) == 2:
            self.max_call_oi_2 = round((entire_oc[entire_oc['Strike Price'] == self.max_put_oi_sp].iloc[0, 0]) /
                                       self.round_factor, 1)
            self.max_call_oi_sp_2 = self.max_put_oi_sp
            self.max_put_oi_2 = round((entire_oc[entire_oc['Strike Price'] == self.max_call_oi_sp].iloc[0, 20]) /
                                      self.round_factor, 1)
            self.max_put_oi_sp_2 = self.max_call_oi_sp
        else:
            call_oi_list_2: List[int] = []
            for i in range(put_oi_index, call_oi_index):
                int_call_oi_2: int = int(entire_oc.iloc[i, [0]][0])
                call_oi_list_2.append(int_call_oi_2)
            call_oi_index_2: int = put_oi_index + call_oi_list_2.index(max(call_oi_list_2))
            self.max_call_oi_2 = round(max(call_oi_list_2) / self.round_factor, 1)
            self.max_call_oi_sp_2 = entire_oc.iloc[call_oi_index_2]['Strike Price']

            put_oi_list_2: List[int] = []
            for i in range(put_oi_index + 1, call_oi_index + 1):
                int_put_oi_2: int = int(entire_oc.iloc[i, [20]][0])
                put_oi_list_2.append(int_put_oi_2)
            put_oi_index_2: int = put_oi_index + 1 + put_oi_list_2.index(max(put_oi_list_2))
            self.max_put_oi_2 = round(max(put_oi_list_2) / self.round_factor, 1)
            self.max_put_oi_sp_2 = entire_oc.iloc[put_oi_index_2]['Strike Price']

        total_call_oi: int = sum(call_oi_list)
        total_put_oi: int = sum(put_oi_list)
        self.put_call_ratio: float
        try:
            self.put_call_ratio = round(total_put_oi / total_call_oi, 2)
        except ZeroDivisionError:
            self.put_call_ratio = 0

        try:
            index: int = int(entire_oc[entire_oc['Strike Price'] == self.sp].index.tolist()[0])
        except IndexError as err:
            print(err, sys.exc_info()[0], "10")
            print("Incorrect Strike Price.\nPlease enter correct Strike Price.")
            return

        a: pandas.DataFrame = entire_oc[['Change in Open Interest']][entire_oc['Strike Price'] == self.sp]
        b1: pandas.Series = a.iloc[:, 0]
        c1: numpy.int64 = b1.get(index)
        b2: pandas.Series = entire_oc.iloc[:, 1]
        c2: numpy.int64 = b2.get((index + 1), 'Change in Open Interest')
        b3: pandas.Series = entire_oc.iloc[:, 1]
        c3: numpy.int64 = b3.get((index + 2), 'Change in Open Interest')
        if isinstance(c2, str):
            c2 = 0
        if isinstance(c3, str):
            c3 = 0
        self.call_sum: numpy.float64 = round((c1 + c2 + c3) / self.round_factor, 1)
        if self.call_sum == -0:
            self.call_sum = 0.0
        self.call_boundary: numpy.float64 = round(c3 / self.round_factor, 1)

        o1: pandas.Series = a.iloc[:, 1]
        p1: numpy.int64 = o1.get(index)
        o2: pandas.Series = entire_oc.iloc[:, 19]
        p2: numpy.int64 = o2.get((index + 1), 'Change in Open Interest')
        p3: numpy.int64 = o2.get((index + 2), 'Change in Open Interest')
        self.p4: numpy.int64 = o2.get((index + 4), 'Change in Open Interest')
        o3: pandas.Series = entire_oc.iloc[:, 1]
        self.p5: numpy.int64 = o3.get((index + 4), 'Change in Open Interest')
        self.p6: numpy.int64 = o3.get((index - 2), 'Change in Open Interest')
        self.p7: numpy.int64 = o2.get((index - 2), 'Change in Open Interest')
        if isinstance(p2, str):
            p2 = 0
        if isinstance(p3, str):
            p3 = 0
        if isinstance(self.p4, str):
            self.p4 = 0
        if isinstance(self.p5, str):
            self.p5 = 0
        self.put_sum: numpy.float64 = round((p1 + p2 + p3) / self.round_factor, 1)
        self.put_boundary: numpy.float64 = round(p1 / self.round_factor, 1)
        self.difference: float = float(round(self.call_sum - self.put_sum, 1))
        self.call_itm: numpy.float64
        if self.p5 == 0:
            self.call_itm = 0.0
        else:
            self.call_itm = round(self.p4 / self.p5, 1)
            if self.call_itm == -0:
                self.call_itm = 0.0
        if isinstance(self.p6, str):
            self.p6 = 0
        if isinstance(self.p7, str):
            self.p7 = 0
        self.put_itm: numpy.float64
        if self.p7 == 0:
            self.put_itm = 0.0
        else:
            self.put_itm = round(self.p6 / self.p7, 1)
            if self.put_itm == -0:
                self.put_itm = 0.0

        if self.stop:
            return

        self.set_values()
        self.first_run = False
        return

    def print_data(self) -> None:
        print(f"Time: {self.str_current_time}. Point: {self.points}")
        self.print_debug(self.entire_oc)
        pass

    def run_one_step(self):
        self.execute_one_step()
        self.print_data()

    def run_capture(self):
        while True:
            if self.str_current_time == '15:30:00' and not self.stop and self.auto_stop \
                    and self.previous_date == datetime.datetime.strptime(time.strftime("%d-%b-%Y", time.localtime()),
                                                                         "%d-%b-%Y").date():
                self.stop = True
                self.options.entryconfig(self.options.index(0), label="Start")
                self.print_debug("Retrieving new data has been stopped.")
                return

            self.run_one_step()
            time.sleep(20)

    def start_capture(self):
        #self.run_capture()
        self.run_one_step()

    def stop_capture(self):
        self.stop = True


if __name__ == '__main__':
    data_source = NseData()
    data_source.start_capture()
