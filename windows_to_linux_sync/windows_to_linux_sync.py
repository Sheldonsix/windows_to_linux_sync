#-*- coding:  utf-8-*-
"""
自动同步目录下的文件到 linux 服务器的指定文件夹
"""
import json
import os
import queue
import re
import time
from collections import OrderedDict
from pathlib import Path
import paramiko
import base64
import logging
import threading

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"

logging.basicConfig(filename='my.log', level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)


class LinuxSynchronizer():
    def __init__(self, host, port, username, password, local_dir, remote_dir, file_suffix_tuple_exluded=('.pyc', '.log', '.gz'), file_volume_limit=1000 * 1000,
                 path_pattern_exluded_tuple=('/.git/', '/.idea/'), only_upload_within_the_last_modify_time=7 * 24 * 60 * 60, cycle_interval=10,):
        """
        :param host: 服务器 ip 地址
        :param port: 服务器 ssh sftp 端口
        :param username: 服务器用户名
        :param password: 服务器密码
        :param local_dir: 本机绝对路径
        :param remote_dir: Linux 服务器的绝对路径
        :param file_suffix_tuple_exluded: 排除以这些后缀结尾的文件
        :param file_volume_limit: 最大文件容量能够限制，如果超过此大小，则该文件不上传
        :param path_pattern_exluded_tuple: 排除这些文件
        :param only_upload_within_the_last_modify_time: 时间阈值，超过这个阈值的不上传
        :param cycle_interval: 扫描周期
        """
        self._host = host
        self._port = port
        self._username = username
        self._password = base64.b64decode(password)
        self._local_dir = str(local_dir).replace('\\', '/')
        self._remote_dir = remote_dir
        self._file_suffix_tuple_exluded = file_suffix_tuple_exluded
        self._path_pattern_exluded_tuple = path_pattern_exluded_tuple
        self._only_upload_within_the_last_modify_time = eval(only_upload_within_the_last_modify_time)
        self._cycle_interval = cycle_interval
        self._file_volume_limit = eval(file_volume_limit)
        self.filename__filesize_map = dict()
        self.filename__st_mtime_map = dict()
        self.build_connect()

    # noinspection PyAttributeOutsideInit
    def build_connect(self):
        print('正在建立 Linux 连接...')
        logging.warning('正在建立 Linux 连接...')
        # self.logger.warning('建立linux连接')
        # noinspection PyTypeChecker
        for __ in range(10):
            try:
                t = paramiko.Transport((self._host, self._port))
                t.connect(username=self._username, password=self._password)
                self.sftp = paramiko.SFTPClient.from_transport(t)
                ssh = paramiko.SSHClient()
                ssh.load_system_host_keys()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(self._host, port=self._port, username=self._username, password=self._password, compress=True)
                self.ssh = ssh
                print('连接成功:)')
                logging.warning('连接成功:)')
                break
            except Exception as e:
                print('连接失败:(')
                logging.warning('连接失败:(')
                print(e)

    # @decorators.tomorrow_threads(1)
    def ftp_upload(self, file: str):
        # file = file.replace('\\', '/')
        pattern_str = self._local_dir
        file_remote = file.replace(pattern_str, self._remote_dir)
        for _ in range(10):
            try:
                time_start = time.time()
                self.sftp.put(file, file_remote)
                print(f'{file_remote} 上传成功,大小是 {int(os.path.getsize(file) / 1024 * 1000) / 1000} kb,上传时间是 {round(time.time() - time_start, 2)}')
                logging.debug(f'{file_remote} 上传成功,大小是 {int(os.path.getsize(file) / 1024 * 1000) / 1000} kb,上传时间是 {round(time.time() - time_start, 2)}')
                break
            except FileNotFoundError:
                cmd = 'mkdir -p ' + str(Path(file_remote).parent).replace('\\', '/')
                # self.logger.info(cmd)
                print(cmd)
                logging.info(cmd)
                tdin, stdout, stderr = self.ssh.exec_command(cmd)
                stderr_bytes = stderr.read()
                # self.logger.debug(stderr_bytes)
                logging.debug(stderr_bytes)
                if stderr_bytes != b'':
                    # self.logger.debug(stderr_bytes)
                    print(stderr_bytes)
            except OSError as e:
                # self.logger.exception(e)
                print(e)
                logging.exception(e)
                pass
                self.build_connect()     # OSError: Socket is closed

    def _judge_need_filter_a_file(self, filename: str):
        ext = filename.split('.')[-1]
        if '.' + ext in self._file_suffix_tuple_exluded:
            return True
        for path_pattern_exluded in self._path_pattern_exluded_tuple:
            if re.search(path_pattern_exluded, filename):
                return True
        return False

    def find_all_files_meet_the_conditions(self):
        total_volume = 0
        self.filename__filesize_map.clear()
        # os.walk 输出当前目录的文件名、文件夹名
        for parent, dirnames, filenames in os.walk(self._local_dir):
            for filename in filenames:
                file_full_name = os.path.join(parent, filename).replace('\\', '/')
                if not self._judge_need_filter_a_file(file_full_name):
                    # self.logger.debug(os.stat(file_full_name).st_mtime)
                    logging.debug(os.stat(file_full_name).st_mtime)
                    file_st_mtime = os.stat(file_full_name).st_mtime
                    volume = os.path.getsize(file_full_name)
                    # 如果当前时间减去修改时间小于修改阈值
                    if time.time() - file_st_mtime < self._only_upload_within_the_last_modify_time and volume < self._file_volume_limit and (file_full_name not in self.filename__st_mtime_map or time.time() - file_st_mtime < 10 * 60):
                        self.filename__filesize_map[file_full_name] = {'volume': volume, 'last_modify_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(file_st_mtime))}
                        self.filename__st_mtime_map[file_full_name] = file_st_mtime
                        total_volume += volume
        filename__filesize_map_ordered_by_lsat_modify_time = OrderedDict()
        for k, v in sorted(self.filename__filesize_map.items(), key=lambda item: item[1]['last_modify_time']):
            filename__filesize_map_ordered_by_lsat_modify_time[k] = v
        self.filename__filesize_map = filename__filesize_map_ordered_by_lsat_modify_time
        if len(self.filename__filesize_map):
            print(f'需要上传的所有文件数量是 {len(self.filename__filesize_map)} ,总大小是 {int(total_volume / 1024 * 1000) / 1000} kb ，文件分别是 {json.dumps(self.filename__filesize_map, ensure_ascii=False,indent=4)}')
            logging.debug(f'需要上传的所有文件数量是 {len(self.filename__filesize_map)} ,总大小是 {int(total_volume / 1024 * 1000) / 1000} kb ，文件分别是 {json.dumps(self.filename__filesize_map, ensure_ascii=False,indent=4)}')
        else:
            print('没有需要同步的文件。')
            logging.debug('没有需要同步的文件。')

    def start_upload_files(self, is_continue):
        if is_continue == 1:
            while 1 == 1:
                time.sleep(self._cycle_interval)
                self._start_upload_files()
        elif is_continue == 0:
            self._start_upload_files()
        else:
            print("上传失败")
            logging.error("上传失败")


    def _start_upload_files(self):
        self.find_all_files_meet_the_conditions()
        for file in self.filename__filesize_map:
            self.ftp_upload(file)



if __name__ == '__main__':
    is_continue = input("是否持续同步文件？（y/N）")
    if is_continue == "y" or is_continue == "Y":
        print("正在持续同步文件，需要手动关闭程序。")
        try:
            for config_item in json.load(Path('windows_to_linux_sync_config.json').open()):
                LinuxSynchronizer(**config_item).start_upload_files(1)
        except Exception as e:
            print('上传失败:(')
            print(e)
            logging.error('上传失败:(')
            logging.exception(e)
    elif is_continue == "n" or is_continue == "N" or is_continue == "":
        print("同步文件开始")
        try:
            for config_item in json.load(Path('windows_to_linux_sync_config.json').open()):
                LinuxSynchronizer(**config_item).start_upload_files(0)
        except Exception as e:
            print('上传失败:(')
            print(e)
            logging.error('上传失败:(')
            logging.exception(e)
    else:
        print("输入有误，请重新输入。")
        logging.error("输入有误，请重新输入。")
        # print(config_item)
        # RepeatingTimer(10, LinuxSynchronizer(**config_item).start_upload_files())