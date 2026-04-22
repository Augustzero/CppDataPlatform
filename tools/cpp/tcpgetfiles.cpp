#include "_public.h"
using namespace idc;

struct st_arg
{
    int    clienttype;               
    char ip[31];                      
    int    port;                        
    char srvpath[256];          
	int    ptype;                     
    char srvpathbak[256];    
    bool andchild;                
    char matchname[256];    
    char clientpath[256];      
    int    timetvl;                  
    int    timeout;                 
    char pname[51];             
} starg;

clogfile logfile;

void EXIT(int sig);

void _help();

bool _xmltoarg(const char *strxmlbuffer);

ctcpclient tcpclient;

bool login(const char *argv);   

string strrecvbuffer;  
string strsendbuffer;   

void _tcpgetfiles();

bool recvfile(const string &filename,const string &mtime,int filesize);

cpactive pactive; 

int main(int argc,char *argv[])
{
    if (argc!=3) { _help(); return -1; }


    signal(SIGINT,EXIT); signal(SIGTERM,EXIT);

    if (logfile.open(argv[1])==false)
    {
        printf("打开日志文件失败（%s）。\n",argv[1]); return -1;
    }

    if (_xmltoarg(argv[2])==false) return -1;

    pactive.addpinfo(starg.timeout,starg.pname); 

    if (tcpclient.connect(starg.ip,starg.port)==false)
    {
        logfile.write("tcpclient.connect(%s,%d) failed.\n",starg.ip,starg.port); EXIT(-1);
    }

    if (login(argv[2])==false) { logfile.write("login() failed.\n"); EXIT(-1); }

    _tcpgetfiles();

    EXIT(0);
}


bool login(const char *argv)    
{
    sformat(strsendbuffer,"%s<clienttype>2</clienttype>",argv);
    // xxxxxxxxxxxxxx logfile.write("发送：%s\n",strsendbuffer.c_str());
    if (tcpclient.write(strsendbuffer)==false) return false; 

    if (tcpclient.read(strrecvbuffer,10)==false) return false;
    // xxxxxxxxxxxxxx logfile.write("接收：%s\n",strrecvbuffer.c_str());

    logfile.write("登录(%s:%d)成功。\n",starg.ip,starg.port); 

    return true;
}

void EXIT(int sig)
{
    logfile.write("程序退出，sig=%d\n\n",sig);

    exit(0);
}

void _help()
{
    printf("\n");
    printf("Using:/project/tools/bin/tcpgetfiles logfilename xmlbuffer\n\n");

    printf("Sample:/project/tools/bin/procctl 20 /project/tools/bin/tcpgetfiles /log/idc/tcpgetfiles_surfdata.log "
              "\"<ip>192.168.150.128</ip><port>5005</port>"\
              "<clientpath>/tmp/client</clientpath>"
              "<ptype>1</ptype><srvpath>/tmp/server</srvpath>"\
              "<andchild>true</andchild><matchname>*</matchname>"
              "<timetvl>10</timetvl><timeout>50</timeout><pname>tcpgetfiles_surfdata</pname>\"\n");
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

    getxmlbuffer(strxmlbuffer,"srvpath",starg.srvpath);
    if (strlen(starg.srvpath)==0) { logfile.write("srvpath is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"srvpathbak",starg.srvpathbak);
    if ((starg.ptype==2)&&(strlen(starg.srvpathbak)==0)) { logfile.write("srvpathbak is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"andchild",starg.andchild);

    getxmlbuffer(strxmlbuffer,"matchname",starg.matchname);
    if (strlen(starg.matchname)==0) { logfile.write("matchname is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"clientpath",starg.clientpath);
    if (strlen(starg.clientpath)==0) { logfile.write("clientpath is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"timetvl",starg.timetvl);
    if (starg.timetvl==0) { logfile.write("timetvl is null.\n"); return false; }


    if (starg.timetvl>30) starg.timetvl=30;

    getxmlbuffer(strxmlbuffer,"timeout",starg.timeout);
    if (starg.timeout==0) { logfile.write("timeout is null.\n"); return false; }
    if (starg.timeout<=starg.timetvl)  { logfile.write("starg.timeout(%d) <= starg.timetvl(%d).\n",starg.timeout,starg.timetvl); return false; }

    getxmlbuffer(strxmlbuffer,"pname",starg.pname,50);
    //if (strlen(starg.pname)==0) { logfile.write("pname is null.\n"); return false; }

    return true;
}


void _tcpgetfiles()
{
    while (true)
    {
        pactive.uptatime();

        if (tcpclient.read(strrecvbuffer,starg.timetvl+10)==false)
        {
            logfile.write("tcpclient.read() failed.\n"); return;
        }
        // logfile.write("strrecvbuffer=%s\n",strrecvbuffer.c_str());

        if (strrecvbuffer=="<activetest>ok</activetest>")
        {
            strsendbuffer="ok";
            // xxxxxxxxxxx logfile.write("strsendbuffer=%s\n",strsendbuffer.c_str());
            if (tcpclient.write(strsendbuffer)==false)
            {
                logfile.write("tcpclient.write() failed.\n"); return;
            }
        }

        if (strrecvbuffer.find("<filename>") != string::npos)
        {
            string serverfilename;
            string mtime; 
            int  filesize=0;
            getxmlbuffer(strrecvbuffer,"filename",serverfilename);
            getxmlbuffer(strrecvbuffer,"mtime",mtime);
            getxmlbuffer(strrecvbuffer,"size",filesize);

            string clientfilename;
            clientfilename=serverfilename;
            replacestr(clientfilename,starg.srvpath,starg.clientpath,false);

            logfile.write("recv %s(%d) ...",clientfilename.c_str(),filesize);
            if (recvfile(clientfilename,mtime,filesize)==true)
            {
                logfile << "ok.\n";
                sformat(strsendbuffer,"<filename>%s</filename><result>ok</result>",serverfilename.c_str());
            }
            else
            {
                logfile << "failed.\n";
                sformat(strsendbuffer,"<filename>%s</filename><result>failed</result>",serverfilename.c_str());
            }

            // xxxxxxxxxxx logfile.write("strsendbuffer=%s\n",strsendbuffer.c_str());
            if (tcpclient.write(strsendbuffer)==false)
            {
                logfile.write("tcpclient.write() failed.\n"); return;
            }
        }
    }
}

bool recvfile(const string &filename,const string &mtime,int filesize)
{
    int  totalbytes=0;       
    int  onread=0;           
    char buffer[1000];       
    cofile ofile;

    newdir(filename);

    if (ofile.open(filename,true,ios::out|ios::binary)==false) return false;

    while (true)
    {
        memset(buffer,0,sizeof(buffer));

        if (filesize-totalbytes>1000) onread=1000;
        else onread=filesize-totalbytes;

        if (tcpclient.read(buffer,onread)==false)  return false; 

        ofile.write(buffer,onread);

        totalbytes=totalbytes+onread;

        if (totalbytes==filesize) break;
    }

    ofile.closeandrename();

    setmtime(filename,mtime);

    return true;
}