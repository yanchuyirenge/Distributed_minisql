import socket
import time
from kazoo.client import KazooClient
import warnings
warnings.filterwarnings("ignore")

master='127.0.0.1:2181'
client_path="zookeeper Client"
MAX_client=100
read_path="zookeeper Read"
write_path="zookeeper Write"
MAX_write=1000
class watcher:
    def __init__(self,zk,path,maxc):
        self.path=path
        self.name=""
        self.maxc=maxc
        self.r_socket=socket.socket()
        self.zk=zk
        self.able=0
    
    def watcher_start(self):
        self.zk.ensure_path(self.path)
        children_read=self.zk.get_children(self.path)
        for i in range(self.maxc):
            self.name="read"+str(i)
            if self.name not in children_read:
                self.zk.create(self.path+"/"+self.name)
                break;
        
        @self.zk.DataWatch(self.path+"/"+self.name,send_event=True)
        def write_watch(data, stat, event=None):
            if event!=None:
                words=data.decode('utf8').split(" ")
                if len(words)==2:
                    print(words)
                    ip=words[0]
                    port=int(words[1])
                    print(">>>connect with region,ip:",ip,"port:",port)
                    self.r_socket.connect((ip,port))
                    self.able=1
        self.zk.add_listener(self.listener)
        time.sleep(10)
                        
    def listener(self,state):
        if state == self.zk.KazooState.LOST or state == self.zk.KazooState.SUSPENDED:
            print(">>>region lost,reconnecting")
            self.watcher_start()
def run():
    print(">>>distribute sql client start!")
    zk = KazooClient(hosts=master, read_only=True)
    zk.start()
    
    zk.ensure_path(client_path)
    children_client=zk.get_children(client_path)
    name=""
    for i in range(MAX_client):
        name="client"+str(i)
        if name not in children_client:
            zk.create(client_path+"/"+name)
            print(">>>client create in zookeeper!name:",name)
            break
    
    read_flag=0
    r_watcher=watcher(zk,read_path,MAX_client)
    while True:
        line="";
        string=input(">>>")
        while not string[len(string)-1]==";":
            line=line+string+" "
            string=input(">>>")
        line=line+string[0:len(string)-1]
        
        if line=="quit":
            zk.delete(client_path+"/"+name)
            print(">>>client quit!bye")
            break
        
        command=""
        for i in range(len(line)):
            if i>0 and (line[i]==' ' or line[i]==';') and line[i-1]==' ':
                continue
            elif i==0 and line[i]==' ':
                continue
            else:
                command=command+line[i]
        command=command.rstrip()
        
        if check(command)==1:
            words=command.split(" ")
            if words[0]=="select":
                if read_flag==0:
                    read_flag=1
                    r_watcher.watcher_start()
                try:
                    command="[client][0]["+command+"]"
                    r_watcher.r_socket.sendall(command.encode('utf8'))
                except Exception as e:
                    print(">>>message send error!:",e)
                    continue
                result=r_watcher.r_socket.recv(1024).decode('utf8')
                print(result)
                if result[12]=='0':
                    result=result[15:]
                    result=result.rstrip("]")
                    lines=result.split(";")
                    print(lines)
                    for j in range(len(lines)):
                        words=lines[j].split(",")
                        for k in range(len(lines)):
                            print(words[k],end='\t')
                        print('')
                else:
                    result=result[15:]
                    result=result.rstrip("]")
                    print(result)
            else:
                zk.ensure_path(write_path)
                children_write=zk.get_children(write_path)
                for i in range(MAX_write):
                    name="write"+str(i)
                    if name not in children_write:
                        zk.create(write_path+"/"+name,bytes(command,encoding = "utf8"))
                        break
                @zk.DataWatch(write_path+"/"+name)
                def write_watch(data, stat, event=None):
                    print(">>>write command success!")
        else:
            print(">>>sql command error!please input a correct command again")
def check(command):
    words=command.split(" ")
    if words[0]=="select":
        if words.count("from")==0:
            return 0
        num=words.index("from")+1
        if num<len(words):
            return 1
        else :
            return 0
    elif words[0]=="drop" or words[0]=="delete" or words[0]=="insert" or words[0]=="create":
        if words[1]=="index":
            if words.count("on")==0:
                return 0
            num=words.index("on")+1
            if num<len(words):
                return 1
            else :
                return 0
        elif words[1]=="table":
            if 2<len(words):
                return 1
            else :
                return 0
        else:
            return 0
    else:
        return 0;
if __name__ == '__main__':
    run()