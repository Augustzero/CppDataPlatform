#include "_public.h"
using namespace idc;


struct st_arg
{
    int clienttype;
    char ip[31];
    int port;
    char clientpath[256];
    int ptype;                  // 文件上传成功后对本地文件处理模式: 1-删除文件 2-移动到备份文件
    char clientpathbak[256];
    bool andchild;
    char matchname[256];
    char srvpath[256];
    int timetvl;
    int timeout;
    char pname[51];
}starg;

void _help();

bool _xmltoarg(const char *strxmlbuffer);
clogfile logfile;
ctcpclient tcpclient;

bool login(const char *argv);

bool _tcpputfiles(bool &bcontinue);

//把文件的内容发送给对端
bool sendfile(const string &filename,const int filesize);

//处理传输文件的响应报文 (删除或者转存本地的文件)
bool ackmessage(const string &strrecvbuffer);

void EXIT(int sig);

bool activetest();

string strsendbuffer;
string strrecvbuffer;

cpactive pactive;   //进程心跳
int main(int argc,char *argv[])
{
    if (argc !=3 )
    {
        _help();
        return -1;
    }

    signal(SIGINT,EXIT);signal(SIGTERM,EXIT);

    if (logfile.open(argv[1])==false)
    {
        printf("打开日志文件失败(%s)\n",argv[1]);
        return -1;
    }

    if (_xmltoarg(argv[2])==false) return -1;

    pactive.addpinfo(starg.timeout,starg.pname); //把进程的心跳信息写入共享内存
    if (tcpclient.connect(starg.ip,starg.port)==false)
    {
        logfile.write("tcpclient.connect(%s,%d) failed\n",starg.ip,starg.port);
        EXIT(-1);
    }

    if (login(argv[2])==false) {logfile.write("login() failed\n");EXIT(-1);}

    bool bcontinue=true;  
    
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

    EXIT(0);

}

bool activetest()
{
    strsendbuffer="<activetest>ok</activetest>";
    
    //  xxxxxxxxxx logfile.write("发送:%s\n",strsendbuffer.c_str());
    if (tcpclient.write(strsendbuffer)==false) return false;

    if (tcpclient.read(strrecvbuffer,10)==false) return false;
    //  xxxxxxxxxx logfile.write("接受:%s\n",strrecvbuffer.c_str());

    return true;
}

void EXIT(int sig)
{
    logfile.write("程序退出,sig=%d\n\n",sig);

    exit(0);
}

void _help()
{
    printf("/project/tools/bin/procctl 20 /project/tools/bin/tcpputfiles /log/idc/tcpputfiles_surfdata.log "\
           "\"<ip>192.168.193.128</ip><port>5005</port>"\
           "<clientpath>/tmp/client</clientpath><ptype>1</ptype>"\
           "<srvpath>/tmp/server</srvpath>"\
           "<andchild>true</andchild><matchname>*.xml,*txt</matchname><timetvl>10</timetvl>"\
           "<timeout>50</timeout><pname>tcpputfiles_surfdata</pname>\"\n\n");
}

bool _xmltoarg(const char *strxmlbuffer)
{
        memset(&starg,0,sizeof(struct st_arg));

    getxmlbuffer(strxmlbuffer,"ip",starg.ip);
    if (strlen(starg.ip)==0) { logfile.write("ip is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"port",starg.port);
    if ( starg.port==0) { logfile.write("port is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"ptype",starg.ptype);
    if ((starg.ptype!=1)&&(starg.ptype!=2)) { logfile.write("ptype not in (1,2).\n"); return false; }

    getxmlbuffer(strxmlbuffer,"clientpath",starg.clientpath);
    if (strlen(starg.clientpath)==0) { logfile.write("clientpath is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"clientpathbak",starg.clientpathbak);
    if ((starg.ptype==2)&&(strlen(starg.clientpathbak)==0)) { logfile.write("clientpathbak is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"andchild",starg.andchild);

    getxmlbuffer(strxmlbuffer,"matchname",starg.matchname);
    if (strlen(starg.matchname)==0) { logfile.write("matchname is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"srvpath",starg.srvpath);
    if (strlen(starg.srvpath)==0) { logfile.write("srvpath is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"timetvl",starg.timetvl);
    if (starg.timetvl==0) { logfile.write("timetvl is null.\n"); return false; }

    // 扫描本地文件的时间间隔(执行上传任务的时间间隔)  单位: 秒
    if (starg.timetvl>30) starg.timetvl=30;

    // 进程心跳的超时时间一定要超过starg.timetvl
    getxmlbuffer(strxmlbuffer,"timeout",starg.timeout);
    if (starg.timeout==0) { logfile.write("timeout is null.\n"); return false; }
    if (starg.timeout<50)  starg.timeout=50;

    getxmlbuffer(strxmlbuffer,"pname",starg.pname,50);
    //if (strlen(starg.pname)==0) { logfile.write("pname is null.\n"); return false; }

    return true;
}

bool login(const char *argv)
{
    sformat(strsendbuffer,"%s<clienttype>1</clienttype>",argv);
    // xxxxxxxxxx logfile.write("发送: %s\n",strsendbuffer.c_str());
    if (tcpclient.write(strsendbuffer)==false) return false;

    if (tcpclient.read(strrecvbuffer,10)==false) return false;
    // xxxxxxxxxx logfile.write("接收: %s\n",strrecvbuffer.c_str());

    logfile.write("登录(%s:%d)成功\n",starg.ip,starg.port);

    return true;
}

bool _tcpputfiles(bool &bcontinue)
{
    bcontinue=false;

    cdir dir;

    //打开starg.clientpath目录
    if (dir.opendir(starg.clientpath,starg.matchname,10000,starg.andchild)==false)
    {
        logfile.write("dir.opendir(%s)failed\n",starg.clientpath);return false;
    }

    int delayed=0;       
    
    //遍历目录中的每个文件
    while (dir.readdir())
    {
        bcontinue=true;
        //把文件名,修改时间,文件大小组成报文发送给对端
        sformat(strsendbuffer,"<filename>%s</filename><mtime>%s</mtime><size>%d</size>",
                  dir.m_ffilename.c_str(),dir.m_mtime.c_str(),dir.m_filesize);
        
        // xxxxxxxxxxxxx logfile.write("strsendbuffer=%s\n",strsendbuffer.c_str());
        if (tcpclient.write(strsendbuffer)==false)
        {
            logfile.write("tcpclient.write() failed\n");return false;
        }

        //发送文件内容
        logfile.write("send %s(%d)...",dir.m_ffilename.c_str(),dir.m_filesize);
        if (sendfile(dir.m_ffilename,dir.m_filesize)==true)
        {
            logfile << "ok\n"; delayed++;
        }
        else
        {
            logfile << "failed\n";tcpclient.close();return false;
        }

        pactive.uptatime();

        //接受服务端确认报文
        while (delayed>0)
        {
            if (tcpclient.read(strrecvbuffer,-1)==false) break;
            // xxxxxxxxxxxx logfile.write("strrecvbuffer=%s\n",strrecvbuffer.c_str());

            //处理服务端的确认报文(删除本地文件或把本地文件移动到备份目录)
            delayed--;
            ackmessage(strrecvbuffer);
        }
    }

    //继续接收对端的确认报文
    while (delayed>0)
    {
        if (tcpclient.read(strrecvbuffer,10)==false) break;
        // xxxxxxxxxxxxx logfile.write("strrecvbuffer=%s\n",strrecvbuffer.c_str());

        //处理服务端的确认报文(删除本地文件或把本地文件移动到备份目录)
        delayed--;
        ackmessage(strrecvbuffer);
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

        if (filesize-totalbytes>1000) onread=1000;
        else onread=filesize-totalbytes;

        ifile.read(buffer,onread);

        if (tcpclient.write(buffer,onread)==false) { return false;}

        totalbytes=totalbytes+onread;

        if (totalbytes==filesize) break;
    }

    return true;
}

//处理传输文件的响应报文
bool ackmessage(const string &strrecvbuffer)
{
    string filename; 
    string result;   
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
        replacestr(bakfilename,starg.clientpath,starg.clientpathbak,false); 
        if (renamefile(filename,bakfilename)==false)
        {
            logfile.write("renamefile(%s,%s)failed\n",filename.c_str(),bakfilename.c_str());
            return false;
        }
    }
    return true;
}