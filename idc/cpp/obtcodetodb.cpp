#include "_public.h"
#include "_ooci.h"  
using namespace idc;

clogfile logfile;
connection conn;
cpactive pactive;

struct st_stcode
{
    char provname[31];
    char obtid[11];
    char cityname[31];
    char lat[11];
    char lon[11];
    char height[11];
}stcode;
list<struct st_stcode> stcodelist;
bool loadstcode(const string &infile);
void EXIT(int sig);
int main(int argc,char *argv[])
{
    if (argc !=5)
    {
        printf("\n");
        printf("Using:./obtcodetodb inifile connstr charset logfile\n");

        printf("Example:/project/tools/bin/procctl 120 /project/idc/bin/obtcodetodb /project/idc/ini/stcode.ini"\
                " \"idc/idcpwd@snorcl11g_128\" \"Simplified Chinese_China.AL32UTF8\" /log/idc/obtcodetodb.log\n\n");

        return -1;
    }

    signal(SIGINT,EXIT);signal(SIGTERM,EXIT);

    if (logfile.open(argv[4])==false)
    {
        printf("打开日志文件失败 (%s)\n",argv[4]); return -1;
    }

    //调式程序时,禁用以下代码,防止被杀
    pactive.addpinfo(10,"obtcodetodb");
    if (loadstcode(argv[1])==false) EXIT(-1);

    if (conn.connecttodb(argv[2],argv[3])!=0)
    {
        logfile.write("connect database(%s) failed\n%s\n",argv[2],conn.message());EXIT(-1);
    }

    logfile.write("connect database(%s) ok\n",argv[2]);

    sqlstatement stmtins(&conn);
    stmtins.prepare("\
        insert into T_ZHOBTCODE(obtid,cityname,provname,lat,lon,height,keyid)\
                            values(:1,:2,:3,:4*100,:5*100,:6*10,SEQ_ZHOBTCODE.nextval)");
    stmtins.bindin(1,stcode.obtid,5);
    stmtins.bindin(2,stcode.cityname,30);
    stmtins.bindin(3,stcode.provname,30);
    stmtins.bindin(4,stcode.lat,10);
    stmtins.bindin(5,stcode.lon,10);
    stmtins.bindin(6,stcode.height,10);

    sqlstatement stmtupt(&conn);
    stmtupt.prepare("\
        update T_ZHOBTCODE set cityname=:1,provname=:2,lat=:3*100,lon=:4*100,height=:5*10,upttime=sysdate \
         where obtid=:6");
    stmtupt.bindin(1,stcode.cityname,30);
    stmtupt.bindin(2,stcode.provname,30);
    stmtupt.bindin(3,stcode.lat,10);
    stmtupt.bindin(4,stcode.lon,10);
    stmtupt.bindin(5,stcode.height,10);
    stmtupt.bindin(6,stcode.obtid,5);

    int inscount=0,uptcount=0; //插入记录数和更新记录数
    ctimer timer;              //操作数据库消耗的时间

    for (auto &aa:stcodelist)
    {

        stcode=aa;

        if (stmtins.execute()!=0)
        {
            if (stmtins.rc()==1)
            {
                if (stmtupt.execute()!=0)
                {
                    logfile.write("stmtupt.execute() failed.\n%s\n%s\n",stmtupt.sql(),stmtupt.message()); EXIT(-1);
                }
                else
                    uptcount++;
            }
            else
            {
                logfile.write("stmtins.execute() failed\n%s\n%s\n",stmtins.sql(),stmtins.message()); EXIT(-1);
            }                   
        }
        else
            inscount++;
    }

    logfile.write("总记录数=%d.插入=%d.更新=%d.耗时=%.2f秒.\n",stcodelist.size(),inscount,uptcount,timer.elapsed());

    conn.commit();

    return 0;
}

void EXIT(int sig)
{
    logfile.write("程序退出,sig=%d\n\n",sig);

    conn.rollback();
    conn.disconnect();

    exit(0);
}

bool loadstcode(const string &inifile)
{
    cifile ifile;
    if (ifile.open(inifile) == false)
    {
        logfile.write("ifile.open(%s) failed\n",inifile);return false;
    }

    string strbuffer;
    ccmdstr cmdstr;


    while (true)
    {
        //logfile.write("strbuffer=%s\n",strbuffer.c_str());
        if (ifile.readline(strbuffer)==false) break;

        cmdstr.splittocmd(strbuffer,",");

        if (cmdstr.cmdcount()!=6) continue;

        memset(&stcode,0,sizeof(struct st_stcode));
        cmdstr.getvalue(0,stcode.provname,30);
        cmdstr.getvalue(1,stcode.obtid,5);
        cmdstr.getvalue(2,stcode.cityname,30);
        cmdstr.getvalue(3,stcode.lat,10);
        cmdstr.getvalue(4,stcode.lon,10);
        cmdstr.getvalue(5,stcode.height,10);

        stcodelist.push_back(stcode);
    }

    //for (auto &aa:stlist)
    //{
    //  logfile.write("provname=%s,obtid=%s,obtname=%s,lat=%.2f,log=%.2f,height=%.2f\n",\
    //  aa.provname,aa.obtid,aa.obtname,aa.lat,aa.log,aa.height);
    //}

    return true;
}