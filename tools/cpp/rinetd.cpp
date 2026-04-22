#include "_public.h"
using namespace idc;

// 代理路由参数的结构体。
struct st_route
{
    int    srcport;           // 源端口。
    char dstip[31];        // 目标主机的地址。
    int    dstport;          // 目标主机的端口。
    int    listensock;      // 监听源端口的socket。
}stroute;
vector<struct st_route> vroute;       // 代理路由的容器。
bool loadroute(const char *inifile);  // 把代理路由参数加载到vroute容器。

int initserver(int port);     // 初始化服务端的监听端口。

int epollfd=0;                 
int tfd=0;                      

#define MAXSOCK  1024
int clientsocks[MAXSOCK];       // 存放每个socket连接对端的socket的值。
int clientatime[MAXSOCK];       // 存放每个socket连接最后一次收发报文的时间。
string clientbuffer[MAXSOCK]; // 存放每个socket发送内容的buffer。

int cmdlistensock=0;                // 命令通道监听的socket。
int cmdconnsock=0;                 // 命令通道连接的socket。

void EXIT(int sig);                      // 进程退出函数。

clogfile logfile;

cpactive pactive;                        // 进程心跳。

int main(int argc,char *argv[])
{
    if (argc != 4)
    {
        printf("\n");
        printf("Using :./rinetd logfile inifile cmdport\n\n");
        printf("Sample:./rinetd /tmp/rinetd.log /etc/rinetd.conf 5001\n\n");
        printf("        /project/tools/bin/procctl 5 /project/tools/bin/rinetd /tmp/rinetd.log /etc/rinetd.conf 5001\n\n");
        printf("logfile 本程序运行的日志文件名。\n");
        printf("inifile 代理路由参数配置文件。\n");
        printf("cmdport 与内网代理程序的通讯端口。\n\n");
        return -1;
    }

    // 关闭全部的信号和输入输出。
    // 设置信号,在shell状态下可用 "kill + 进程号" 正常终止些进程。
    // 但请不要用 "kill -9 +进程号" 强行终止。
    closeioandsignal(); signal(SIGINT,EXIT); signal(SIGTERM,EXIT);

    // 打开日志文件。
    if (logfile.open(argv[1])==false)
    {
        printf("打开日志文件失败（%s）。\n",argv[1]); return -1;
    }

    pactive.addpinfo(30,"rinetd");       // 设置进程的心跳超时间为30秒。

    // 把代理路由参数加载到vroute容器。
    if (loadroute(argv[2])==false) return -1;

    logfile.write("加载代理路由参数成功(%d)。\n",vroute.size());

    // 初始化命令通道的监听端口。
    if ( (cmdlistensock=initserver(atoi(argv[3]))) < 0 )
    {   
        logfile.write("initserver(%s) failed.\n",argv[3]); EXIT(-1);
    }

    // 等待内网程序的连接请求
    struct sockaddr_in client;
    socklen_t len = sizeof(client);
    cmdconnsock = accept(cmdlistensock,(struct sockaddr*)&client,&len);
    if (cmdconnsock < 0)
    {
        logfile.write("accept() failed.\n"); EXIT(-1);
    }
    logfile.write("与内部的命令通道已建立(cmdconnsock=%d)。\n",cmdconnsock);

    // 初始化服务端用于监听外网的socket。
    for (int ii=0;ii<vroute.size();ii++)
    {
        if ( (vroute[ii].listensock=initserver(vroute[ii].srcport)) < 0 )
        {
            logfile.write("initserver(%d) failed.\n",vroute[ii].srcport); EXIT(-1);
        }

        fcntl(vroute[ii].listensock,F_SETFL,fcntl(vroute[ii].listensock,F_GETFD,0)|O_NONBLOCK);
    }

    epollfd=epoll_create(1);

    struct epoll_event ev;  

    // 为监听外网的socket准备可读事件。
    for (int ii=0;ii<vroute.size();ii++)
    {
        ev.events=EPOLLIN;                       
        ev.data.fd=vroute[ii].listensock;  
        epoll_ctl(epollfd,EPOLL_CTL_ADD,vroute[ii].listensock,&ev);  
    }

    // 创建定时器。
    tfd=timerfd_create(CLOCK_MONOTONIC,TFD_NONBLOCK|TFD_CLOEXEC);  
    struct itimerspec timeout;
    memset(&timeout,0,sizeof(struct itimerspec));
    timeout.it_value.tv_sec = 20;                  // 超时时间为20秒。
    timeout.it_value.tv_nsec = 0;
    timerfd_settime(tfd,0,&timeout,NULL); 
  
    // 为定时器准备事件。
    ev.events=EPOLLIN;   
    ev.data.fd=tfd;
    epoll_ctl(epollfd,EPOLL_CTL_ADD,tfd,&ev);      

    struct epoll_event evs[10];    

    while (true)
    {
        int infds=epoll_wait(epollfd,evs,10,-1);

        if (infds < 0) { logfile.write("epoll() failed。"); EXIT(-1); }

        for (int ii=0;ii<infds;ii++)
        {
            ////////////////////////////////////////////////////////
            // 定时器的时间已到
            if (evs[ii].data.fd==tfd)
            {
                // logfile.write("定时器时间已到。\n");

                timerfd_settime(tfd,0,&timeout,0);  // 重新开始计时。

                pactive.uptatime();        // 更新进程心跳；

                // 向命令通道发送心跳报文；
                char buffer[256];
                strcpy(buffer,"<activetest>");
                if (send(cmdconnsock,buffer,strlen(buffer),0)<=0)
                {
                    logfile.write("与内网程序的命令通道已断开。\n"); EXIT(-1);
                }

                // 清理空闲的客户端socket。
                for (int jj=0;jj<MAXSOCK;jj++)
                {
                    // 如果客户端socket空闲的时间超过80秒就关掉它。
                    if ( (clientsocks[jj]>0) && ((time(0)-clientatime[jj])>80) )
                    {
                        logfile.write("client(%d,%d) timeout。\n",clientsocks[jj],clientsocks[clientsocks[jj]]);
                        close(clientsocks[jj]);  close(clientsocks[clientsocks[jj]]);
                        clientsocks[clientsocks[jj]]=0;
                        clientsocks[jj]=0;
                    }
                }

                continue;
            }
            ////////////////////////////////////////////////////////
      
            ////////////////////////////////////////////////////////
            // 发生事件的是监听的listensock
            int jj=0;
            for (jj=0;jj<vroute.size();jj++)
            {
                if (evs[ii].data.fd==vroute[jj].listensock)
                {
                    // 从已连接队列中获取一个已准备好的外网客户端的socket。
                    struct sockaddr_in client;
                    socklen_t len = sizeof(client);
                    int srcsock = accept(vroute[jj].listensock,(struct sockaddr*)&client,&len);
                    if (srcsock<0) break;
                    if (srcsock>=MAXSOCK) 
                    {
                        logfile.write("连接数已超过最大值%d。\n",MAXSOCK); close(srcsock); break;
                    }

                    // 通过命令通道向内网程序发送命令，把路由参数传给它。
                    char buffer[256];
                    memset(buffer,0,sizeof(buffer));
                    sprintf(buffer,"<dstip>%s</dstip><dstport>%d</dstport>",vroute[jj].dstip,vroute[jj].dstport);
                    if (send(cmdconnsock,buffer,strlen(buffer),0)<=0)
                    {
                        logfile.write("与内网的命令通道已断开。\n"); EXIT(-1);
                    }

                    // 接受内网程序的连接
                    int dstsock=accept(cmdlistensock,(struct sockaddr*)&client,&len);
                    if (dstsock<0) { close(srcsock); break; }
                    if (dstsock>=MAXSOCK)
                    {
                        logfile.write("连接数已超过最大值%d。\n",MAXSOCK); close(srcsock); close(dstsock); break;
                    }

                    // 把内网和外网客户端的socket对接在一起。

                    ev.data.fd=srcsock; ev.events=EPOLLIN;
                    epoll_ctl(epollfd,EPOLL_CTL_ADD,srcsock,&ev);
                    ev.data.fd=dstsock; ev.events=EPOLLIN;
                    epoll_ctl(epollfd,EPOLL_CTL_ADD,dstsock,&ev);

                    clientsocks[srcsock]=dstsock;    clientatime[srcsock]=time(0); 
                    clientsocks[dstsock]=srcsock;    clientatime[dstsock]=time(0);

                    logfile.write("accept port %d client(%d,%d) ok。\n",vroute[jj].srcport,srcsock,dstsock);

                    break;
                }
            }

            if (jj<vroute.size()) continue;
            ////////////////////////////////////////////////////////

            ////////////////////////////////////////////////////////

            // 如果从通道一端的socket读取到了数据，把数据存放在对端socket的缓冲区中。
            if (evs[ii].events&EPOLLIN)    
            {
                char buffer[5000];     
                int    buflen=0;          

                // 从通道的一端读取数据。
                if ( (buflen=recv(evs[ii].data.fd,buffer,sizeof(buffer),0)) <= 0 )
                {
                    // 如果连接已断开，需要关闭通道两端的socket。
                    logfile.write("client(%d,%d) disconnected。\n",evs[ii].data.fd,clientsocks[evs[ii].data.fd]);
                    close(evs[ii].data.fd);                                     
                    close(clientsocks[evs[ii].data.fd]);                   
                    clientsocks[clientsocks[evs[ii].data.fd]]=0;       
                    clientsocks[evs[ii].data.fd]=0;                           

                    continue;
                }
      
                // 成功的读取到了数据
                // logfile.write("from %d to %d,%d bytes。\n",evs[ii].data.fd,clientsocks[evs[ii].data.fd],buflen);
                // send(clientsocks[evs[ii].data.fd],buffer,buflen,0);

                logfile.write("from %d,%d bytes\n",evs[ii].data.fd,buflen);

                clientbuffer[clientsocks[evs[ii].data.fd]].append(buffer,buflen);

                ev.data.fd=clientsocks[evs[ii].data.fd];
                ev.events=EPOLLIN|EPOLLOUT;
                epoll_ctl(epollfd,EPOLL_CTL_MOD,ev.data.fd,&ev);

                clientatime[evs[ii].data.fd]=time(0); 
                clientatime[clientsocks[evs[ii].data.fd]]=time(0);  
            }

            if (evs[ii].events&EPOLLOUT)
            {
                // 把socket缓冲区中的数据发送出去。
                int writen=send(evs[ii].data.fd,clientbuffer[evs[ii].data.fd].data(),clientbuffer[evs[ii].data.fd].length(),0);

                //int ilen;
                //if (clientbuffer[evs[ii].data.fd].length()>10) ilen=10;
                //else ilen=clientbuffer[evs[ii].data.fd].length();
                //int writen=send(evs[ii].data.fd,clientbuffer[evs[ii].data.fd].data(),ilen,0);

                logfile.write("to %d,%d bytes\n",evs[ii].data.fd,writen);

                clientbuffer[evs[ii].data.fd].erase(0,writen);

                if (clientbuffer[evs[ii].data.fd].length()==0)
                {
                    ev.data.fd=evs[ii].data.fd;
                    ev.events=EPOLLIN;
                    epoll_ctl(epollfd,EPOLL_CTL_MOD,ev.data.fd,&ev);
                }
            }
        }
    }

    return 0;
}

// 初始化服务端的监听端口。
int initserver(const int port)
{
    int sock = socket(AF_INET,SOCK_STREAM,0);
    if (sock < 0)
    {
        logfile.write("socket(%d) failed.\n",port); return -1;
    }

    int opt = 1; unsigned int len = sizeof(opt);
    setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&opt,len);

    struct sockaddr_in servaddr;
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);

    if (bind(sock,(struct sockaddr *)&servaddr,sizeof(servaddr)) < 0 )
    {
        logfile.write("bind(%d) failed.\n",port); close(sock); return -1;
    }

    if (listen(sock,5) != 0 )
    {
        logfile.write("listen(%d) failed.\n",port); close(sock); return -1;
    }

    return sock;
}

// 把代理路由参数加载到vroute容器。
bool loadroute(const char *inifile)
{
    cifile ifile;

    if (ifile.open(inifile)==false)
    {
        logfile.write("打开代理路由参数文件(%s)失败。\n",inifile); return false;
    }

    string strbuffer;
    ccmdstr cmdstr;

   while (true)
    {
        if (ifile.readline(strbuffer)==false) break;

        auto pos=strbuffer.find("#");
        if (pos!=string::npos) strbuffer.resize(pos);

        replacestr(strbuffer,"  "," ",true);    
        deletelrchr(strbuffer,' ');              

        cmdstr.splittocmd(strbuffer," ");
        if (cmdstr.size()!=3) continue;

        memset(&stroute,0,sizeof(struct st_route));
        cmdstr.getvalue(0,stroute.srcport);        
        cmdstr.getvalue(1,stroute.dstip);          
        cmdstr.getvalue(2,stroute.dstport);      

        vroute.push_back(stroute);
    }

    return true;
}

void EXIT(int sig)
{
    logfile.write("程序退出，sig=%d。\n\n",sig);

    close(cmdlistensock);

    close(cmdconnsock);

    for (auto &aa:vroute)
        if (aa.listensock>0) close(aa.listensock);

    for (auto aa:clientsocks)
        if (aa>0) close(aa);

    close(epollfd);

    close(tfd);      

  exit(0);
}