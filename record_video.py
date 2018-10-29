import socket
import time
import os
import json
import threading


class Mythread(threading.Thread):
    def __init__(self, cfg):
        threading.Thread.__init__(self)
        self.cfg = cfg

    def run(self):
        multil_stream(self.cfg)

def parse_channel_cfg_file(file):
    with open(file, 'r') as f:
        data = json.load(f)
        cfg_list = data['channelList']
        print(cfg_list)
        return cfg_list

def del_old_file(path, days):
    dirs = os.listdir(path)
    for x in dirs:
        if -1 != x.find(".ts"):
            print(x)
            statinfo = os.stat(x)
            print(statinfo.st_mtime)
            print(time.time())
            if (statinfo.st_mtime <= time.time() - days*24*60*60):
                os.remove(x)

def get_file_name(name):
    return name + "_"+ time.strftime("%Y-%m-%d %H%M%S", time.localtime()) + ".ts"

def multil_stream(channelcfg):
    name = channelcfg['streamName']
    url = channelcfg['streamURL']
    path = channelcfg['recordRootPath']
    days = channelcfg['recordStorageDays']
    file_size = channelcfg['recordSinglFileSizeMB']
    

    url_tmp = url.split(':')
    print(url_tmp)
    ip = url_tmp[1][2:]
    port = int(url_tmp[2])

    print('\n')
    print("multil_stream ip=", ip, "port=", port)

    localip = '0.0.0.0'
    sc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sc.bind((localip, port))
    sc.setsockopt(socket.IPPROTO_IP,socket.IP_ADD_MEMBERSHIP, socket.inet_aton(ip)+socket.inet_aton(localip))
    sc.setblocking(0)

    file_recv_len = 0
    #os.mkdir(name)
    #os.chdir(name)
    f = open(get_file_name(name), "wb")
   
    while True:
        try:
            message, addr = sc.recvfrom(2048)
        except:
            pass
            ##print("while receive message error occur")
        else:
            if 0 == file_recv_len:
                del_old_file('./', days)
                
            data = message[12:]
            #print("message: ", data)
            file_recv_len += len(data)
            f.write(data)

            if file_recv_len >= file_size*1024*1024:
                file_recv_len = 0
                f.close
                f = open(get_file_name(name), "wb")
                f.write(data)

    f.close


if __name__ == "__main__":
    
    cfg_list = parse_channel_cfg_file('channel_config.json')

    it = iter(cfg_list)
    threads = []
    for x in it:
        print(x)
        thread = Mythread(x)
        thread.start()
        threads.append(thread)

    for t in threads:
        t.join()
        
