#include "_public.h"
using namespace idc;

int  cmdconnsock;  // 内网程序与外网程序的命令通道的socket。

int epollfd=0;   
int tfd=0;           

#define MAXSOCK  1024
int clientsocks[MAXSOCK];       
int clientatime[MAXSOCK];       
string clientbuffer[MAXSOCK]; 

// 向目标ip和端口发起socket连接，bio取值：false-非阻塞io，true-阻塞io。
int conntodst(const char *ip,const int port,bool bio=false);

void EXIT(int sig);   // 进程退出函数。

clogfile logfile;

cpactive pactive;     // 进程心跳。

int main(int argc,char *argv[])
{
    if (argc != 4)
    {
        printf("\n");
        printf("Using :./rinetdin logfile ip port\n\n");
        printf("Sample:./rinetdin /tmp/rinetdin.log 192.168.193.128 5001\n\n");
        printf("        /project/tools/bin/procctl 5 /project/tools/bin/rinetdin /tmp/rinetdin.log 192.168.193.128 5001\n\n");
        printf("logfile 本程序运行的日志文件名。\n");
        printf("ip      外网代理程序的ip地址。\n");
        printf("port    外网代理程序的通讯端口。\n\n\n");
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

    // pactive.addpinfo(30,"inetd");       // 设置进程的心跳超时间为30秒。

    // 建立与外网程序的命令通道。
    if ((cmdconnsock=conntodst(argv[2],atoi(argv[3]),true))<0)
    {
        logfile.write("tcpclient.connect(%s,%s) 失败。\n",argv[2],argv[3]); return -1;
    }

    logfile.write("与外部的命令通道已建立(cmdconnsock=%d)。\n",cmdconnsock);

    fcntl(cmdconnsock,F_SETFL,fcntl(cmdconnsock,F_GETFD,0)|O_NONBLOCK);

    epollfd=epoll_create(1);

    struct epoll_event ev;  

    ev.events=EPOLLIN;
    ev.data.fd=cmdconnsock;
    epoll_ctl(epollfd,EPOLL_CTL_ADD,cmdconnsock,&ev);

    // 创建定时器。
    tfd=timerfd_create(CLOCK_MONOTONIC,TFD_NONBLOCK|TFD_CLOEXEC); 
    struct itimerspec timeout;
    memset(&timeout,0,sizeof(struct itimerspec));
    timeout.it_value.tv_sec = 20;           // 超时时间为20秒。
    timeout.it_value.tv_nsec = 0;
    timerfd_settime(tfd,0,&timeout,0);  
  
    // 为定时器准备事件。
    ev.events=EPOLLIN;               
    ev.data.fd=tfd;
    epoll_ctl(epollfd,EPOLL_CTL_ADD,tfd,&ev);     

    // pactive.addpinfo(30,"rinetdin");   // 设置进程的心跳超时间为30秒。

    struct epoll_event evs[10];      

    while (true)
    {
        int infds=epoll_wait(epollfd,evs,10,-1);

        if (infds < 0) { logfile.write("epoll() failed。\n"); EXIT(-1); }

        for (int ii=0;ii<infds;ii++)
        {
            ////////////////////////////////////////////////////////
            // 定时器的时间已到
            if (evs[ii].data.fd==tfd)
            {
                // logfile.write("定时器时间已到。\n");

                timerfd_settime(tfd,0,&timeout,NULL);  // 重新开始计时。

                pactive.uptatime();        // 更新进程心跳。

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

            // 发生事件的是命令通道。
            if (evs[ii].data.fd==cmdconnsock)
            {
                // 读取命令通道socket报文内容。
                char buffer[256];
                memset(buffer,0,sizeof(buffer));
                if (recv(cmdconnsock,buffer,sizeof(buffer),0)<=0)
                {
                    logfile.write("与外网的命令通道已断开。\n"); EXIT(-1);
                }

                // 如果收到的是心跳报文。
                if (strcmp(buffer,"<activetest>")==0) continue;

                // 如果收到的是新建连接的命令。
                int srcsock=conntodst(argv[2],atoi(argv[3]));
                if (srcsock<0) continue;
                if (srcsock>=MAXSOCK)
                {
                    logfile.write("连接数已超过最大值%d。\n",MAXSOCK); close(srcsock); continue;
                }

                // 从命令报文内容中获取目标服务器的地址和端口。
                char dstip[11];
                int  dstport;
                getxmlbuffer(buffer,"dstip",dstip,30);
                getxmlbuffer(buffer,"dstport",dstport);

                // 向目标服务器的地址和端口发起socket连接。
                int dstsock=conntodst(dstip,dstport);
                if (dstsock<0) { close(srcsock); continue; }
                if (dstsock>=MAXSOCK)
                { 
                    logfile.write("连接数已超过最大值%d。\n",MAXSOCK); close(srcsock); close(dstsock); continue;
                } 

                // 把内网和外网的socket对接在一起。
                logfile.write("新建内外网通道(%d,%d) ok。\n",srcsock,dstsock);

                ev.data.fd=srcsock; ev.events=EPOLLIN;
                epoll_ctl(epollfd,EPOLL_CTL_ADD,srcsock,&ev);
                ev.data.fd=dstsock; ev.events=EPOLLIN;
                epoll_ctl(epollfd,EPOLL_CTL_ADD,dstsock,&ev);

                clientsocks[srcsock]=dstsock; clientsocks[dstsock]=srcsock;
                clientatime[srcsock]=time(0); clientatime[dstsock]=time(0);

                continue;
            }
            ////////////////////////////////////////////////////////

            ////////////////////////////////////////////////////////
            // 客户端连接的socke有事件

            if (evs[ii].events&EPOLLIN)   
            {
                char buffer[5000];     
                int    buflen=0;         

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

// 向目标地址和端口发起socket连接。
int conntodst(const char *ip,const int port,bool bio)
{
    int sockfd;
    if ( (sockfd = socket(AF_INET,SOCK_STREAM,0))==-1) return -1; 

    struct hostent* h;
    if ( (h = gethostbyname(ip)) == 0 ) { close(sockfd); return -1; }
  
    struct sockaddr_in servaddr;
    memset(&servaddr,0,sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port); 
    memcpy(&servaddr.sin_addr,h->h_addr,h->h_length);

    if (bio==false) fcntl(sockfd,F_SETFL,fcntl(sockfd,F_GETFD,0)|O_NONBLOCK);

    if (connect(sockfd, (struct sockaddr *)&servaddr,sizeof(servaddr))<0)
    {
        if (errno!=EINPROGRESS)
        {
            logfile.write("connect(%s,%d) failed.\n",ip,port); return -1;
        }
    }

    return sockfd;
}

void EXIT(int sig)
{
    logfile.write("程序退出，sig=%d。\n\n",sig);

    close(cmdconnsock);

    for (auto aa:clientsocks)
        if (aa>0) close(aa);

    close(epollfd);   

    close(tfd);      

    exit(0);
}