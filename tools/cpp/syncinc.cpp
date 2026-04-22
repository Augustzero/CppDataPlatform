#include "_tools.h"

struct st_arg
{
    char localconnstr[101];        // 本地数据库的连接参数。
    char charset[51];                  // 数据库的字符集。
    char lnktname[31];              // 远程表名
    char localtname[31];           // 本地表名。
    char remotecols[1001];       // 远程表的字段列表。
    char localcols[1001];           // 本地表的字段列表。
    char where[1001];               // 同步数据的条件。
    char remoteconnstr[101];   // 远程数据库的连接参数。
    char remotetname[31];       // 远程表名。
    char remotekeycol[31];       // 远程表的自增字段名。
    char localkeycol[31];           // 本地表的自增字段名。  
    int  maxcount;                     // 每批同步操作的记录数。
    int  timetvl;                          // 同步时间间隔，单位：秒，取值1-30。
    int  timeout;                        // 本程序运行时的超时时间。
    char pname[51];                  // 本程序运行时的程序名。
} starg;


void _help(char *argv[]);


bool _xmltoarg(char *strxmlbuffer);

clogfile logfile;

connection connloc;   // 本地数据库连接。
connection connrem;   // 远程数据库连接。

bool _syncinc(bool &bcontinue);
 

long maxkeyvalue=0;
bool loadmaxkey();

void EXIT(int sig);

cpactive pactive;

int main(int argc,char *argv[])
{
    if (argc!=3) { _help(argv); return -1; }

    // 关闭全部的信号和输入输出，处理程序退出的信号。
    // closeioandsignal(true); 
    signal(SIGINT,EXIT); signal(SIGTERM,EXIT);

    if (logfile.open(argv[1])==false)
    {
        printf("打开日志文件失败（%s）。\n",argv[1]); return -1;
    }


    if (_xmltoarg(argv[2])==false) return -1;

    pactive.addpinfo(starg.timeout,starg.pname);

    if (connloc.connecttodb(starg.localconnstr,starg.charset) != 0)     // 连接本地数据库。
    {
        logfile.write("connect database(%s) failed.\n%s\n",starg.localconnstr,connloc.message()); EXIT(-1);
    }

    // logfile.write("connect database(%s) ok.\n",starg.localconnstr);

    if (connrem.connecttodb(starg.remoteconnstr,starg.charset) != 0)  // 连接远程数据库。
    {
        logfile.write("connect database(%s) failed.\n%s\n",starg.remoteconnstr,connrem.message()); return false;
    }

    // logfile.write("connect database(%s) ok.\n",starg.remoteconnstr);

    if ( (strlen(starg.remotecols)==0) || (strlen(starg.localcols)==0) )
    {
        ctcols tcols;

        // 获取starg.localtname表的全部列。
        if (tcols.allcols(connloc,starg.localtname)==false)
        {
            logfile.write("表%s不存在。\n",starg.localtname); EXIT(-1); 
        }

        if (strlen(starg.remotecols)==0)  strcpy(starg.remotecols,tcols.m_allcols.c_str());
        if (strlen(starg.localcols)==0)      strcpy(starg.localcols,tcols.m_allcols.c_str());
    }

    
    bool bcontinue;    

    while (true)
    {
        if (_syncinc(bcontinue)==false) EXIT(-1);

        // 如果本次同步没有操作数据，就休眠。
        if (bcontinue==false) sleep(starg.timetvl);

        pactive.uptatime();
    }
}

// 显示程序的帮助
void _help(char *argv[])
{
    printf("Using:/project/tools/bin/syncinc logfilename xmlbuffer\n\n");

    printf("把T_ZHOBTMIND1@db128表中全部的记录增量同步到T_ZHOBTMIND2中。\n");
    printf("Sample:/project/tools/bin/procctl 10 /project/tools/bin/syncinc /log/idc/syncinc_ZHOBTMIND2.log "\
              "\"<localconnstr>idc/idcpwd@snorcl11g_128</localconnstr><remoteconnstr>idc/idcpwd@snorcl11g_128</remoteconnstr>"\
              "<charset>Simplified Chinese_China.AL32UTF8</charset>"\
              "<remotetname>T_ZHOBTMIND1</remotetname><lnktname>T_ZHOBTMIND1@db128</lnktname>"\
              "<localtname>T_ZHOBTMIND2</localtname>"\
              "<remotecols>obtid,ddatetime,t,p,u,wd,wf,r,vis,upttime,keyid</remotecols>"\
              "<localcols>stid,ddatetime,t,p,u,wd,wf,r,vis,upttime,recid</localcols>"\
              "<remotekeycol>keyid</remotekeycol><localkeycol>recid</localkeycol>"\
              "<maxcount>300</maxcount><timetvl>2</timetvl><timeout>50</timeout><pname>syncinc_ZHOBTMIND2</pname>\"\n\n");

    printf("把T_ZHOBTMIND1@db128表中满足\"and obtid like '57%%'\"的记录增量同步到T_ZHOBTMIND3中。\n");
    printf("       /project/tools/bin/procctl 10 /project/tools/bin/syncinc /log/idc/syncinc_ZHOBTMIND3.log "\
              "\"<localconnstr>idc/idcpwd@snorcl11g_128</localconnstr><remoteconnstr>idc/idcpwd@snorcl11g_128</remoteconnstr>"\
              "<charset>Simplified Chinese_China.AL32UTF8</charset>"\
              "<remotetname>T_ZHOBTMIND1</remotetname><lnktname>T_ZHOBTMIND1@db128</lnktname>"\
              "<localtname>T_ZHOBTMIND3</localtname>"\
              "<remotecols>obtid,ddatetime,t,p,u,wd,wf,r,vis,upttime,keyid</remotecols>"\
              "<localcols>stid,ddatetime,t,p,u,wd,wf,r,vis,upttime,recid</localcols>"\
              "<where>and obtid like '54%%%%'</where>"\
              "<remotekeycol>keyid</remotekeycol><localkeycol>recid</localkeycol>"\
              "<maxcount>300</maxcount><timetvl>2</timetvl><timeout>30</timeout><pname>syncinc_ZHOBTMIND3</pname>\"\n\n");
}

// 把xml解析到参数starg结构中
bool _xmltoarg(char *strxmlbuffer)
{
    memset(&starg,0,sizeof(struct st_arg));

    // 本地数据库的连接参数。
    getxmlbuffer(strxmlbuffer,"localconnstr",starg.localconnstr,100);
    if (strlen(starg.localconnstr)==0) { logfile.write("localconnstr is null.\n"); return false; }

    // 数据库的字符集
    getxmlbuffer(strxmlbuffer,"charset",starg.charset,50);
    if (strlen(starg.charset)==0) { logfile.write("charset is null.\n"); return false; }

    // lnktname表名。
    getxmlbuffer(strxmlbuffer,"lnktname",starg.lnktname,30);
    if (strlen(starg.lnktname)==0) { logfile.write("lnktname is null.\n"); return false; }

    // 本地表名。
    getxmlbuffer(strxmlbuffer,"localtname",starg.localtname,30);
    if (strlen(starg.localtname)==0) { logfile.write("localtname is null.\n"); return false; }

    // 远程表的字段列表
    getxmlbuffer(strxmlbuffer,"remotecols",starg.remotecols,1000);

    // 本地表的字段列表
    getxmlbuffer(strxmlbuffer,"localcols",starg.localcols,1000);

    // 同步数据的条件
    getxmlbuffer(strxmlbuffer,"where",starg.where,1000);

    // 远程数据库的连接参数。
    getxmlbuffer(strxmlbuffer,"remoteconnstr",starg.remoteconnstr,100);
    if (strlen(starg.remoteconnstr)==0) { logfile.write("remoteconnstr is null.\n"); return false; }

    // 远程表名。
    getxmlbuffer(strxmlbuffer,"remotetname",starg.remotetname,30);
    if (strlen(starg.remotetname)==0) { logfile.write("remotetname is null.\n"); return false; }

    // 远程表的自增字段名。
    getxmlbuffer(strxmlbuffer,"remotekeycol",starg.remotekeycol,30);
    if (strlen(starg.remotekeycol)==0) { logfile.write("remotekeycol is null.\n"); return false; }

    // 本地表的自增字段名。
    getxmlbuffer(strxmlbuffer,"localkeycol",starg.localkeycol,30);
    if (strlen(starg.localkeycol)==0) { logfile.write("localkeycol is null.\n"); return false; }

    // 每批执行一次同步操作的记录数。
    getxmlbuffer(strxmlbuffer,"maxcount",starg.maxcount);
    if (starg.maxcount==0) { logfile.write("maxcount is null.\n"); return false; }

    // 执行同步的时间间隔
    getxmlbuffer(strxmlbuffer,"timetvl",starg.timetvl);
    if (starg.timetvl<=0) { logfile.write("timetvl is null.\n"); return false; }
    if (starg.timetvl>30) starg.timetvl=30;      

    // 本程序的超时时间
    getxmlbuffer(strxmlbuffer,"timeout",starg.timeout);
    if (starg.timeout==0) { logfile.write("timeout is null.\n"); return false; }

    if (starg.timeout<starg.timetvl+10) starg.timeout=starg.timetvl+10;

    // 本程序运行时的进程名
    getxmlbuffer(strxmlbuffer,"pname",starg.pname,50);
    if (strlen(starg.pname)==0) { logfile.write("pname is null.\n"); return false; }

    return true;
}

void EXIT(int sig)
{
    logfile.write("程序退出，sig=%d\n\n",sig);

    connloc.disconnect();

    connrem.disconnect();

    exit(0);
}

// 业务处理主函数。
bool _syncinc(bool &bcontinue)
{
    ctimer timer;

    bcontinue=false;

    // 从本地表starg.localtname获取自增字段的最大值，存放在maxkeyvalue全局变量中。
    if (loadmaxkey()==false) return false;

    // 从远程表查找需要同步的记录（自增字段的值大于maxkeyvalue的记录）。
    char remrowidvalue[21];       // 从远程表查到的需要同步记录的rowid。
    sqlstatement stmtsel(&connrem);
    stmtsel.prepare("select rowid from %s where %s>:1 %s order by %s",starg.remotetname,starg.remotekeycol,starg.where,starg.remotekeycol);
    stmtsel.bindin(1,maxkeyvalue);
    stmtsel.bindout(1,remrowidvalue,20);

    string bindstr;    // 绑定插入SQL语句参数的字符串。
    for (int ii=0;ii<starg.maxcount;ii++)
    {
        bindstr=bindstr+sformat(":%lu,",ii+1);
    }
    deleterchr(bindstr,',');         

    char rowidvalues[starg.maxcount][21];   // 存放rowid的值。

    sqlstatement stmtins(&connloc);    
    stmtins.prepare("insert into %s(%s) select %s from %s where rowid in (%s)",\
                                starg.localtname,starg.localcols,starg.remotecols,starg.lnktname,bindstr.c_str());
    for (int ii=0;ii<starg.maxcount;ii++)
    {
        stmtins.bindin(ii+1,rowidvalues[ii]);
    }

    int ccount=0;    

    memset(rowidvalues,0,sizeof(rowidvalues));

    if (stmtsel.execute()!=0)
    {
        logfile.write("stmtsel.execute() failed.\n%s\n%s\n",stmtsel.sql(),stmtsel.message()); return false;
    }

    while (true)
    {
        // 获取需要同步数据的结果集。
        if (stmtsel.next()!=0) break;

        strcpy(rowidvalues[ccount],remrowidvalue);

        ccount++;

        if (ccount==starg.maxcount)
        {
            if (stmtins.execute()!=0)
            {
                logfile.write("stmtins.execute() failed.\n%s\n%s\n",stmtins.sql(),stmtins.message()); return false;
            }

            // logfile.write("sync %s to %s(%d rows) in %.2fsec.\n",starg.lnktname,starg.localtname,ccount,timer.elapsed());

            connloc.commit();
  
            ccount=0;    

            memset(rowidvalues,0,sizeof(rowidvalues));

            pactive.uptatime();
        }
    }

    if (ccount>0)
    {
        if (stmtins.execute()!=0)
        {
            logfile.write("stmtins.execute() failed.\n%s\n%s\n",stmtins.sql(),stmtins.message()); return false;
        }

        // logfile.write("sync %s to %s(%d rows) in %.2fsec.\n",starg.lnktname,starg.localtname,ccount,timer.elapsed());

        connloc.commit();
    }

    if (stmtsel.rpc()>0) 
    {
        logfile.write("sync %s to %s(%d rows) in %.2fsec.\n",starg.lnktname,starg.localtname,stmtsel.rpc(),timer.elapsed());
        bcontinue=true;
    }

    return true;
}

// 从本地表starg.localtname获取自增字段的最大值，存放在maxkeyvalue全局变量中。
bool loadmaxkey()
{
    maxkeyvalue=0;

    sqlstatement stmt(&connloc);
    stmt.prepare("select max(%s) from %s",starg.localkeycol,starg.localtname);
    stmt.bindout(1,maxkeyvalue);
  
    if (stmt.execute()!=0)
    {
        logfile.write("stmt.execute() failed.\n%s\n%s\n",stmt.sql(),stmt.message()); return false;
    }  

    stmt.next();

    // logfile.write("maxkeyvalue=%ld\n",maxkeyvalue);

    return true;
}