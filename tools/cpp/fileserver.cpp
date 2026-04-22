#include "_public.h"
using namespace idc;


struct st_arg
{
    int clienttype;
    char ip[31];
    int port;
    char clientpath[256];
    int ptype;
    char clientpathbak[256];
    bool andchild;
    char matchname[256];
    char srvpath[256];
    char srvpathbak[256];
    int timetvl;
    int timeout;
    char pname[51];
}starg;


clogfile logfile;
ctcpserver tcpserver;

void FathEXIT(int sig);
void ChldEXIT(int sig);

void recvfilesmain();
void sendfilesmain();

//接受文件的内容
bool recvfile(const string &filename,const string &mtime,int filesize);

bool _tcpputfiles(bool &bcontinue);

bool ackmessage(const string &strrecvbuffer);

bool sendfile(const string &filename,const int filesize);

void EXIT(int sig);

bool activetest();


bool clientlogin();
string strsendbuffer;
string strrecvbuffer;

cpactive pactive;
int main(int argc,char *argv[])
{
    if (argc!=3)
    {
        printf("/project/tools/bin/procctl 10 /project/tools/bin/fileserver 5005 /log/idc/fileserver.log\n\n\n");
        return -1;
    }

   signal(SIGINT,FathEXIT);signal(SIGTERM,FathEXIT);
   
   if (logfile.open(argv[2])==false)
   {
        printf("logfile.open(%s) failed\n",argv[2]);
        return -1;
   }

   if (tcpserver.initserver(atoi(argv[1]))==false)
   {
        logfile.write("tcpserver.initserver(%s)failed\n",argv[1]);
        return -1;
   }

   while (true)
   {
        if (tcpserver.accept()==false)
        {
            logfile.write("tcpserver.accept() failed\n");
            FathEXIT(-1);
        }

        logfile.write("客户端 (%s)已连接\n",tcpserver.getip());

        if (fork()>0) {tcpserver.closeclient();continue;}

        signal(SIGINT,ChldEXIT);signal(SIGTERM,ChldEXIT);

        tcpserver.closelisten();

        if (clientlogin()==false) ChldEXIT(-1);
        
        //把进程的心跳信息写入共享内存
        pactive.addpinfo(starg.timeout,starg.pname);
        
        //如果starg.clienttype==1,调用上传文件的主函数
        if (starg.clienttype==1) recvfilesmain();

        //如果starg.clienttype==2,调用下载文件的主函数
        if (starg.clienttype==2) sendfilesmain();



        ChldEXIT(0);
   }
}

void FathEXIT(int sig)
{
    signal(SIGINT,SIG_IGN);signal(SIGTERM,SIG_IGN);

    logfile.write("父进程退出,sig=%d,\n",sig);

    tcpserver.closelisten();

    kill(0,15);

    exit(0);
}

void ChldEXIT(int sig)
{

    signal(SIGINT,SIG_IGN);signal(SIGTERM,SIG_IGN);

    logfile.write("子进程退出,sig=%d\n",sig);

    tcpserver.closeclient();

    exit(0);
}

void recvfilesmain()
{
    while (true)
    {
        pactive.uptatime();

        if (tcpserver.read(strrecvbuffer,starg.timetvl+10)==false)
        {
            logfile.write("tcpserver.read() failed\n");return;
        }
        // xxxxxxx logfile.write("strrecvbuffer=%s\n",strrecvbuffer.c_str());
    
        //处理心跳报文
        if (strrecvbuffer=="<activetest>ok</activetest>")
        {
            strsendbuffer="ok";
            // xxxxxxxx logfile.write("strsendbuffer=%s\n",strsendbuffer.c_str());
            if (tcpserver.write(strsendbuffer)==false)
            {
                logfile.write("tcpserver.write() failed\n");
                return;
            }
        }
        
        //处理上传文件的请求报文
        if (strrecvbuffer.find("<filename>") != string::npos)
        {
            //解析上传文件请求报文的xml
            string clientfilename; //对端的文件名
            string mtime;          //文件的时间
            int filesize=0;        //文件大小
            getxmlbuffer(strrecvbuffer,"filename",clientfilename);
            getxmlbuffer(strrecvbuffer,"mtime",mtime);
            getxmlbuffer(strrecvbuffer,"size",filesize);

            //接收文件的内容
            string serverfilename; //服务端的文件名
            serverfilename=clientfilename;
            replacestr(serverfilename,starg.clientpath,starg.srvpath,false);

            logfile.write("recv %s(%d)...",serverfilename.c_str(),filesize);
            if (recvfile(serverfilename,mtime,filesize)==true)
            {
                logfile << "ok\n";
                sformat(strsendbuffer,"<filename>%s</filename><result>ok</result>",clientfilename.c_str());
            }
            else
            {
                logfile << "failed\n";
                sformat(strsendbuffer,"<filename>%s</filename><result>failed</result>",clientfilename.c_str());
            }
            //假设成功接受了文件的内容,拼接确认报文的内容
            //sformat(strsendbuffer,"<filename>%s</filename><result>ok</result>",clientfilename.c_str());

            //把确认报文返回给对端
            // xxxxxxxxxx logfile.write("strsendbuffer=%s\n",strsendbuffer.c_str());
            if (tcpserver.write(strsendbuffer)==false)
            {
                logfile.write("tcpserver write() failed\n");
                return;
            }
        }
    }
    
}

bool recvfile(const string &filename,const string &mtime,int filesize)
{
    int totalbytes=0;
    int onread=0;
    char buffer[1000];
    cofile ofile;

    if (ofile.open(filename,true,ios::out|ios::binary)==false) return false;

    while(true)
    {
        memset(buffer,0,sizeof(buffer));

        //计算本次应该接受的字节数
        if (filesize-totalbytes>1000) onread=1000;
        else onread=filesize-totalbytes;

        //接收文件内容
        if (tcpserver.read(buffer,onread)==false) return false;

        //把接收到的内容写入文件
        ofile.write(buffer,onread);

        //计算已接收文件的总字节数,如果文件接收完,跳出循环
        totalbytes=totalbytes+onread;

        if (totalbytes==filesize) break;
    }

    ofile.closeandrename();

    //文件时间用当前时间没有意义,应该与对端文件时间保持一致
    setmtime(filename,mtime);
    return true;
}
bool clientlogin()
{

    if (tcpserver.read(strrecvbuffer,10)==false)
    {
        logfile.write("tcpserver.read() failed\n");return false;
    }
    // xxxxxxxxxx logfile.write("strrecvbuffer=%s\n",strrecvbuffer.c_str());

    memset(&starg,0,sizeof(struct st_arg));
    getxmlbuffer(strrecvbuffer,"clienttype",starg.clienttype);
    getxmlbuffer(strrecvbuffer,"clientpath",starg.clientpath);
    getxmlbuffer(strrecvbuffer,"srvpath",starg.srvpath);
    getxmlbuffer(strrecvbuffer,"srvpathbak",starg.srvpathbak);
    getxmlbuffer(strrecvbuffer,"andchild",starg.andchild);
    getxmlbuffer(strrecvbuffer,"ptype",starg.ptype);
    getxmlbuffer(strrecvbuffer,"matchname",starg.matchname);

    getxmlbuffer(strrecvbuffer,"timetvl",starg.timetvl);  //执行任务的周期
    getxmlbuffer(strrecvbuffer,"timeout",starg.timeout);  //进程超时的时间
    getxmlbuffer(strrecvbuffer,"pname",starg.pname);      //进程名

    if ((starg.clienttype!=1)&&(starg.clienttype!=2))
        strsendbuffer="falied";
    else
        strsendbuffer="ok";

    if (tcpserver.write(strsendbuffer)==false)
    {
        logfile.write("tcpserver.write() falied\n");
        return false;   
    }

    logfile.write("%s login %s \n %s\n",tcpserver.getip(),strsendbuffer.c_str(),strrecvbuffer.c_str());
    return true;
}





//下载文件的主函数
void sendfilesmain()
{
    pactive.addpinfo(starg.timeout,starg.pname);

    bool bcontinue=true;  //如果调用_tcpputfiles()发送了文件,bcontinue为true,否则为false.初始化为true.
    
    while(true)
    {
        if (_tcpputfiles(bcontinue)==false) {logfile.write("_tcpputfiles() failed\n");EXIT(-1);}        

        if (bcontinue==false)
        {
            sleep(starg.timetvl);
            
            //发送心跳报文
            if (activetest()==false) break;
        }

        pactive.uptatime();
    }
}



bool _tcpputfiles(bool &bcontinue)
{
    bcontinue=false;

    cdir dir;

    //打开starg.srvpath目录
    if (dir.opendir(starg.srvpath,starg.matchname,10000,starg.andchild)==false)
    {
        logfile.write("dir.opendir(%s)failed\n",starg.clientpath);return false;
    }

    int delayed=0;       //未收到对端确认报文的文件数量,发送一个文件就加1,接收到了一个回应就减1
    
    //遍历目录中的每个文件
    while (dir.readdir())
    {
        bcontinue=true;
        //把文件名,修改时间,文件大小组成报文发送给对端
        sformat(strsendbuffer,"<filename>%s</filename><mtime>%s</mtime><size>%d</size>",
                  dir.m_ffilename.c_str(),dir.m_mtime.c_str(),dir.m_filesize);
        
        // xxxxxxxxxxxxx logfile.write("strsendbuffer=%s\n",strsendbuffer.c_str());
        if (tcpserver.write(strsendbuffer)==false)
        {
            logfile.write("tcpserver.write() failed\n");return false;
        }

        //发送文件内容
        logfile.write("send %s(%d)...",dir.m_ffilename.c_str(),dir.m_filesize);
        if (sendfile(dir.m_ffilename,dir.m_filesize)==true)
        {
            logfile << "ok\n"; delayed++;
        }
        else
        {
            logfile << "failed\n";tcpserver.closeclient();return false;
        }

        pactive.uptatime();

        //接受服务端确认报文
        while (delayed>0)
        {
            if (tcpserver.read(strrecvbuffer,-1)==false) break;
            // xxxxxxxxxxxx logfile.write("strrecvbuffer=%s\n",strrecvbuffer.c_str());

            delayed--;
            //处理服务端的确认报文
            ackmessage(strrecvbuffer);
        }
    }

    //继续接收对端的确认报文
    while (delayed>0)
    {
        if (tcpserver.read(strrecvbuffer,10)==false) break;
        // xxxxxxxxxxxxx logfile.write("strrecvbuffer=%s\n",strrecvbuffer.c_str());

        //处理服务端的确认报文
        delayed--;
        ackmessage(strrecvbuffer);
    }

    return true;
}


//处理传输文件的响应报文
bool ackmessage(const string &strrecvbuffer)
{
    string filename; //本地文件名
    string result;   //对端接受文件的结果
    getxmlbuffer(strrecvbuffer,"filename",filename);
    getxmlbuffer(strrecvbuffer,"result",result);

    if (result!="ok") return true;

    //如果starg.ptype==1,删除文件
    if (starg.ptype==1)
    {
        if (remove(filename.c_str())!=0) {logfile.write("remove(%s) failed\n",filename.c_str());return false;}
    }

    //如果starg.ptype==2,移动到备份目录
    if (starg.ptype==2)
    {
        //生成转存后的备份目录文件名 
        string bakfilename=filename;
        replacestr(bakfilename,starg.srvpath,starg.srvpathbak,false); 
        if (renamefile(filename,bakfilename)==false)
        {
            logfile.write("renamefile(%s,%s)failed\n",filename.c_str(),bakfilename.c_str());
            return false;
        }
    }
    return true;
}

//把文件的内容发给对端
bool sendfile(const string &filename,const int filesize)
{
    int onread=0;      
    char buffer[1000];  
    int totalbytes=0;   
    cifile ifile;       

    if (ifile.open(filename,ios::in|ios::binary)==false) return false;

    while (true)
    {
        memset(buffer,0,sizeof(buffer));

        //计算本次应该读取的字节数
        if (filesize-totalbytes>1000) onread=1000;
        else onread=filesize-totalbytes;

        ifile.read(buffer,onread);

        //把读取的数据发送给对端
        if (tcpserver.write(buffer,onread)==false) { return false;}

        totalbytes=totalbytes+onread;

        if (totalbytes==filesize) break;
    }

    return true;
}


bool activetest()
{
    strsendbuffer="<activetest>ok</activetest>";
    
    //  xxxxxxxxxx logfile.write("发送:%s\n",strsendbuffer.c_str());
    if (tcpserver.write(strsendbuffer)==false) return false;

    if (tcpserver.read(strrecvbuffer,10)==false) return false;
    //  xxxxxxxxxx logfile.write("接受:%s\n",strrecvbuffer.c_str());

    return true;
}

void EXIT(int sig)
{
    logfile.write("程序退出,sig=%d\n\n",sig);

    exit(0);
}
