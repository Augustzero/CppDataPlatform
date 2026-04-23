#include "_public.h"
#include "_ooci.h"
using namespace idc;

//程序运行参数的结构体
struct st_arg
{
    char connstr[101];       // 数据库的连接参数
    char charset[51];         // 数据库的字符集
    char selectsql[1024];   // 从数据源数据库抽取数据的SQL语句
    char fieldstr[501];        // 抽取数据的SQL语句输出结果集字段名,字段名之间用逗号分隔
    char fieldlen[501];       // 抽取数据的SQL语句输出结果集字段的长度,用逗号分隔
    char bfilename[31];     // 输出xml文件的前缀
    char efilename[31];     // 输出xml文件的后缀
    char outpath[256];      // 输出xml文件存放的目录
    int  maxcount;           // 输出xml文件最大记录数,0表示无限制
    char starttime[52];      // 程序运行的时间区间
    char incfield[31];         // 递增字段名
    char incfilename[256]; // 已抽取数据的递增字段最大值存放的文件
    char connstr1[101];     // 已抽取数据的递增字段最大值存放的数据库的连接参数
    int  timeout;              // 进程心跳的超时时间
    char pname[51];          // 进程名,建议用"dminingoracle_后缀"的方式
} starg;

ccmdstr fieldname;      //结果集字段名数组
ccmdstr fieldlen;       //结果集字段长度数组

connection conn;        //数据源数据库

clogfile logfile;


void EXIT(int sig);

void _help();

//把xml解析到参数starg结构中
bool _xmltoarg(const char *strxmlbuffer);

//判断当前时间是否在程序运行的时间区间中
bool instarttime();

//数据抽取的主函数
bool _dminingoracle();

long imaxincvalue;      //递增字段的最大值
int incfieldpos=-1;     //递增字段在结果集数组中的位置
bool readincfield();    //从数据库表中或starg.incfilename文件中加载上次已抽取数据的最大值
bool writeincfield();   //把已抽取数据的最大值写入数据库表或starg.incfilename文件

cpactive pactive;      
int main(int argc, char *argv[])\
{
    if (argc!=3) {_help();return -1;}


    //closeioandsignal(true);
    signal(SIGINT,EXIT);signal(SIGTERM,EXIT);

    if (logfile.open(argv[1])==false)
    {
        printf("打开日志文件失败 (%s)\n",argv[1]); return -1;
    }

    if (_xmltoarg(argv[2])==false) EXIT(-1);

    //判断当前时间是否在程序运行的时间区间内
    if (instarttime()==false) return 0;

    pactive.addpinfo(starg.timeout,starg.pname); 

    if (conn.connecttodb(starg.connstr,starg.charset)!=0)
    {
        logfile.write("connect database(%s) failed\n%s\n",starg.connstr,conn.message());EXIT(-1);
    }
    logfile.write("connect database(%s) ok \n",starg.connstr);

    // 从数据库中或starg.incfilename文件中获取已抽取数据的最大值
    if (readincfield()==false) EXIT(-1);

    _dminingoracle();   //数据抽取的主函数

    return 0;
}

bool _dminingoracle()
{
    sqlstatement stmt(&conn);
    stmt.prepare(starg.selectsql);

    // 绑定结果集的变量
    string strfieldvalue[fieldname.size()];
    for (int ii=1;ii<=fieldname.size();ii++)
    {
        stmt.bindout(ii,strfieldvalue[ii-1],stoi(fieldlen[ii-1]));
    }

    //绑定输入参数
    if (strlen(starg.incfield)!=0) stmt.bindin(1,imaxincvalue);

    if (stmt.execute()!=0)
    {
        logfile.write("stmt.execute() failed\n%s\n%^s\n",stmt.sql(),stmt.message()); return false;
    }

    pactive.uptatime();    

    string strxmlfilename; //输出的xml文件名
    int iseq=1;            //输出xml文件的序号
    cofile ofile;          //向xml文件中写入数据

    while (true)
    {
        if (stmt.next()!=0) break;     

        if (ofile.isopen()==false)
        { 
            sformat(strxmlfilename,"%s/%s_%s_%s_%d.xml",\
                starg.outpath,starg.bfilename,ltime1("yyyymmddhh24miss").c_str(),starg.efilename,iseq++);
            if (ofile.open(strxmlfilename)==false)
            {
                logfile.write("ofile.open(%s) failed\n",strxmlfilename.c_str()); return false;
            }
            ofile.writeline("<data>\n");    
        }

        //把结果集中的每个字符的值写入xml文件
        for (int ii=1;ii<=fieldname.size();ii++)
            ofile.writeline("<%s>%s</%s>",fieldname[ii-1].c_str(),strfieldvalue[ii-1].c_str(),fieldname[ii-1].c_str());
        ofile.writeline("<endl/>\n");    

        //关闭当前文件
        if ((starg.maxcount>0) && (stmt.rpc()%starg.maxcount==0))
        {
            ofile.writeline("</data>\n");  

            if (ofile.closeandrename()==false) 
            {
                logfile.write("ofile.closeandrename(%s) failed\n",strxmlfilename.c_str()); return false;
            }

            logfile.write("生成文件%s(%d),\n",strxmlfilename.c_str(),starg.maxcount);

            pactive.uptatime(); 
        }

        //更新递增字段的最大值
        if ((strlen(starg.incfield)!=0) && (imaxincvalue<stol(strfieldvalue[incfieldpos])))
            imaxincvalue=stol(strfieldvalue[incfieldpos]);
    }

    if (ofile.isopen()==true)
    {
      ofile.writeline("</data>\n");   
      if (ofile.closeandrename()==false)
      {
          logfile.write("ofile.closeandrename(%s) failed\n",strxmlfilename.c_str()); return false;
      }

      if (starg.maxcount==0)
          logfile.write("生成文件%s(%d) \n",strxmlfilename.c_str(),stmt.rpc());
      else
          logfile.write("生成文件%s(%d) \n",strxmlfilename.c_str(),stmt.rpc()%starg.maxcount);

    }

    //把已抽取数据的最大值写入数据库表或starg.incfilename文件
    if (stmt.rpc()>0) writeincfield();

    return true;
}


//判断当前时间是否在程序运行的时间区间中
bool instarttime()
{  
    if (strlen(starg.starttime)!=0)
    {
        string strhh24=ltime1("hh24"); 
        if (strstr(starg.starttime,strhh24.c_str())==0) return false;
            //闲时: 12-14时和00-06时.
    }

    return true;
}
void EXIT(int sig)
{
    printf("程序退出,sig=%d\n\n",sig);

    exit(0);
}

void _help()
{
    printf("Using:/project/tools/bin/dminingoracle logfilename xmlbuffer\n\n");

    printf("Sample:/project/tools/bin/procctl 3600 /project/tools/bin/dminingoracle /log/idc/dminingoracle_ZHOBTCODE.log "
              "\"<connstr>idc/idcpwd@snorcl11g_128</connstr><charset>Simplified Chinese_China.AL32UTF8</charset>"\
              "<selectsql>select obtid,cityname,provname,lat,lon,height from T_ZHOBTCODE where obtid like '5%%%%'</selectsql>"\
              "<fieldstr>obtid,cityname,provname,lat,lon,height</fieldstr><fieldlen>5,30,30,10,10,10</fieldlen>"\
              "<bfilename>ZHOBTCODE</bfilename><efilename>togxpt</efilename><outpath>/idcdata/dmindata</outpath>"\
              "<timeout>30</timeout><pname>dminingoracle_ZHOBTCODE</pname>\"\n\n");
    printf("       /project/tools/bin/procctl   30 /project/tools/bin/dminingoracle /log/idc/dminingoracle_ZHOBTMIND.log "\
              "\"<connstr>idc/idcpwd@snorcl11g_128</connstr><charset>Simplified Chinese_China.AL32UTF8</charset>"\
              "<selectsql>select obtid,to_char(ddatetime,'yyyymmddhh24miss'),t,p,u,wd,wf,r,vis,keyid from T_ZHOBTMIND where keyid>:1 and obtid like '5%%%%'</selectsql>"\
              "<fieldstr>obtid,ddatetime,t,p,u,wd,wf,r,vis,keyid</fieldstr><fieldlen>5,19,8,8,8,8,8,8,8,15</fieldlen>"\
              "<bfilename>ZHOBTMIND</bfilename><efilename>togxpt</efilename><outpath>/idcdata/dmindata</outpath>"\
              "<starttime></starttime><incfield>keyid</incfield>"\
              "<incfilename>/idcdata/dmining/dminingoracle_ZHOBTMIND_togxpt.keyid</incfilename>"\
              "<timeout>30</timeout><pname>dminingoracle_ZHOBTMIND_togxpt</pname>"\
              "<maxcount>1000</maxcount><connstr1>scott/abc@snorcl11g_128</connstr1>\"\n\n");
}

//把xml解析到参数starg结构中
bool _xmltoarg(const char *strxmlbuffer)
{
    memset(&starg,0,sizeof(struct st_arg));

    getxmlbuffer(strxmlbuffer,"connstr",starg.connstr,100);      
    if (strlen(starg.connstr)==0) { logfile.write("connstr is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"charset",starg.charset,50);       
    if (strlen(starg.charset)==0) { logfile.write("charset is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"selectsql",starg.selectsql,1000);  
    if (strlen(starg.selectsql)==0) { logfile.write("selectsql is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"fieldstr",starg.fieldstr,500);         
    if (strlen(starg.fieldstr)==0) { logfile.write("fieldstr is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"fieldlen",starg.fieldlen,500);        
    if (strlen(starg.fieldlen)==0) { logfile.write("fieldlen is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"bfilename",starg.bfilename,30);   
    if (strlen(starg.bfilename)==0) { logfile.write("bfilename is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"efilename",starg.efilename,30);   
    if (strlen(starg.efilename)==0) { logfile.write("efilename is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"outpath",starg.outpath,255);       
    if (strlen(starg.outpath)==0) { logfile.write("outpath is null.\n"); return false; }

    getxmlbuffer(strxmlbuffer,"maxcount",starg.maxcount);       // 可选参数

    getxmlbuffer(strxmlbuffer,"starttime",starg.starttime,50);     // 可选参数

    getxmlbuffer(strxmlbuffer,"incfield",starg.incfield,30);          // 可选参数

    getxmlbuffer(strxmlbuffer,"incfilename",starg.incfilename,255);  // 可选参数

    getxmlbuffer(strxmlbuffer,"connstr1",starg.connstr1,100);          // 可选参数

    getxmlbuffer(strxmlbuffer,"timeout",starg.timeout);      
    if (starg.timeout==0) { logfile.write("timeout is null.\n");  return false; }

    getxmlbuffer(strxmlbuffer,"pname",starg.pname,50);    
    if (strlen(starg.pname)==0) { logfile.write("pname is null.\n");  return false; }

    fieldname.splittocmd(starg.fieldstr,",");

    fieldlen.splittocmd(starg.fieldlen,",");

    if (fieldlen.size()!=fieldname.size())
    {
        logfile.write("fieldstr和fieldlen的元素个数不一致\n");return false;
    }

    if (strlen(starg.incfield)>0)
    {
        if ((strlen(starg.incfilename)==0) && (strlen(starg.connstr1)==0))
        {
            logfile.write("如果是增量抽取,incfilename和connstr1必二选一,不能都为空\n"); return false;
        }
    }

    return true;
}

bool readincfield()
{
    imaxincvalue=0;   //初始化递增字段的最大值

    if (strlen(starg.incfield)==0) return true;

    //查找递增字段在结果集中的位置
    for (int ii=0;ii<fieldname.size();ii++)
        if (fieldname[ii]==starg.incfield) {incfieldpos=ii;break;}

    if (incfieldpos==-1)
    {
        logfile.write("递增字段名%s不在列表%s中 \n",starg.incfield,starg.fieldstr); return false;
    }

    if (strlen(starg.connstr1)!=0)
    {
        //从数据库中加载递增字段的最大值
        connection conn1;
        if (conn1.connecttodb(starg.connstr1,starg.charset)!=0)
        {
            logfile.write("connect database(%s) failed\n%s\n",starg.connstr1,conn1.message()); return -1;
        }
        sqlstatement stmt(&conn1);
        stmt.prepare("select maxincvalue from T_MAXINCVALUE where pname=:1");
        stmt.bindin(1,starg.pname);
        stmt.bindout(1,imaxincvalue);
        stmt.execute();
        stmt.next();
    }
    else
    {
        //从文件中加载递增字段的最大值
        cifile ifile;

        if (ifile.open(starg.incfilename)==false) return true;

        // 文件中读取已抽取数据的最大值
        string strtemp;
        ifile.readline(strtemp);
        imaxincvalue=stoi(strtemp);
    }

    logfile.write("上次已抽取数据的位置 (%s=%ld) \n",starg.incfield,imaxincvalue);

    return true;
}

//把已抽取数据的最大值写入数据库表或starg.incfilename文件
bool writeincfield()
{
    if (strlen(starg.incfield)==0) return true;

    if (strlen(starg.connstr1)!=0)
    {
        connection conn1;
        if (conn1.connecttodb(starg.connstr1,starg.charset)!=0)
        {
            logfile.write("connect database(%s) failed.\n%s\n",starg.connstr1,conn1.message()); return false;
        }
        sqlstatement stmt(&conn1);
        stmt.prepare("update T_MAXINCVALUE set maxincvalue=:1 where pname=:2");
        stmt.bindin(1,imaxincvalue);
        stmt.bindin(2,starg.pname);
        if (stmt.execute()!=0)
        {
            if (stmt.rc()==942)  
            {
                conn1.execute("create table T_MAXINCVALUE(pname varchar2(50),maxincvalue number(15),primary key(pname))");
                conn1.execute("insert into T_MAXINCVALUE values('%s',%ld)",starg.pname,imaxincvalue);
                conn1.commit();
                return true;
            }
            else
            {
                logfile.write("stmt.execute() failed.\n%s\n%s\n",stmt.sql(),stmt.message()); return false;
            }
        }
        else
        {
            if (stmt.rpc()==0)
            {
			    // 如果记录不存在, 就插入新记录
                conn1.execute("insert into T_MAXINCVALUE values('%s',%ld)",starg.pname,imaxincvalue);
            }
            conn1.commit();
        }
    }
    else
    {
        //把递增字段的最大值写入文件
        cofile ofile;

        if (ofile.open(starg.incfilename,false)==false) 
        {
            logfile.write("ofile.open(%s) failed.\n",starg.incfilename); return false;
        }

        // 把已抽取数据的最大id写入文件d
        ofile.writeline("%ld",imaxincvalue);
    }

    return true;
}