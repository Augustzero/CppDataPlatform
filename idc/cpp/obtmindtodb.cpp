//#include "_public.h"
//#include "_ooci.h"
#include "idcapp.h"
using namespace idc;

clogfile logfile;
connection conn;
cpactive pactive;

//业务处理主函数
bool _obtmindtodb(const char *pathname,const char *connstr,const char *charset);
void EXIT(int sig);

int main(int argc,char *argv[])
{

    if (argc!=5)
    {
        printf("\n");
        printf("Using:./obtmindtodb pathname connstr charset logfile\n");

        printf("Example:/project/tools/bin/procctl 10 /project/idc/bin/obtmindtodb /idcdata/surfdata "\
                  "\"idc/idcpwd@snorcl11g_128\" \"Simplified Chinese_China.AL32UTF8\" /log/idc/obtmindtodb.log\n\n");

        return -1;
    }

    //closeioandsignal(true);
    signal(SIGINT,EXIT);signal(SIGTERM,EXIT);

    if (logfile.open(argv[4])==false)
    {
        printf("打开日志文件失败 (%s)\n",argv[4]);return -1;
    }

    //业务处理主函数
    _obtmindtodb(argv[1],argv[2],argv[3]);
    return 0;
}

void EXIT(int sig)
{
    logfile.write("程序退出,sig=%d\n\n",sig);

    conn.rollback();
    conn.disconnect();

    exit(0);
}

bool _obtmindtodb(const char *pathname,const char *connstr,const char *charset)
{
    cdir dir;
    if (dir.opendir(pathname,"*.xml,*.csv")==false)
    {
        logfile.write("dir.opendir(%s) failed\n",pathname);
        return false;
    }

    CZHOBTMIND ZHOBTMIND(conn,logfile);     //操作数据观测表的对象
    sqlstatement stmt;
    while (true)
    {
        //读取一个气象观测数据文件 (只处理*.xml和*.csv)
        if (dir.readdir()==false) break;
        if (conn.isopen()==false)
        {
            if (conn.connecttodb(connstr,charset)!=0)
            {
                logfile.write("connect database(%s) failed\n%s\n",connstr,conn.message());return false;
            }

            logfile.write("connect database(%s) ok\n",connstr);

        }


        cifile ifile;
        if (ifile.open(dir.m_ffilename)==false)
        {
            logfile.write("file.open(%s) failed\n",dir.m_ffilename.c_str());return false;
        }

        int totalcount=0;
        int insertcount=0;
        ctimer timer;
        bool bisxml=matchstr(dir.m_ffilename,"*xml"); 

        string strbuffer;   //存放从文件中读取的一行数据

        //如果是csv文件,扔掉第一行
        if (bisxml==false) ifile.readline(strbuffer);

        //读取文件中的每一行
        while(true)
        {
            if (bisxml==true)
            {
                if (ifile.readline(strbuffer,"<endl/>")==false) break;   
            }
            else
            {
                if (ifile.readline(strbuffer)==false) break;             
            }

            totalcount++;

            //解析行的内容 
            ZHOBTMIND.splitbuffer(strbuffer,bisxml);
            //把解析后的数据入库
            if (ZHOBTMIND.inserttable()==true) insertcount++;  
    
        }
        ifile.closeandremove();
        conn.commit();
        logfile.write("已处理文件%s (totalcount=%d,insertcount=%d),耗时%.2f秒 \n",\
                        dir.m_ffilename.c_str(),totalcount,insertcount,timer.elapsed());
    }

    return true;
}
