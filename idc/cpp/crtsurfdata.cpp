#include "_public.h"
using namespace idc;

struct st_stcode
{
    char provname[31];
    char obtid[11];
    char obtname[31];
    double lat;
    double log;
    double height;
};
list<struct st_stcode> stlist;
bool loadstcode(const string &inifile);

struct st_sufdata
{
    char obtid[11];
    char ddatetime[15];
    int t;
    int p;
    int u;
    int wd;
    int wf;
    int r;
    int vis;
};
list<struct st_sufdata> datalist;
void crtsurfdata();
bool crtsurffile(const string &outpath,const string &datafmt);

char strddatetime[15];
clogfile logfile;

void EXIT(int sig);

cpactive pactive;

int main(int argc,char *argv[])
{
    if (argc != 5)
    {
        cout << "Using:./crtsurfdata inifile outpath logfile datafmt\n";
        cout << "Examples:/project/tools/bin/procctl 60 /project/idc/bin/crtsurfdata /project/idc/ini/stcode.ini /tmp/idc/surfdata /log/idc/crtsurfdata.log csv,xml,json\n";
        cout << "inifile 气象站点参数文件名\n";
        cout << "outpath 气象站点数据文件存放的目录\n";
        cout << "logfile  本程序运行的日志文件名\n";
        cout << "datafmt 输出文件的格式支持csv,xml,json,中间用逗号分隔\n\n";
        return -1;
    }
    closeioandsignal(true);
    signal(SIGINT,EXIT);signal(SIGTERM,EXIT);

    pactive.addpinfo(10,"crtsurfdata");
    
    if (logfile.open(argv[3])==false)
    {
        cout << "logfile open(" << argv[3] << ")failed,\n";return -1;
    }

    logfile.write("crtsurfdata 开始运行 \n");

    if (loadstcode(argv[1]) == false) EXIT(-1);
    
    memset(strddatetime,0,sizeof(strddatetime));
    ltime(strddatetime,"yyyymmddhh24miss");
    strncpy(strddatetime+12,"00",2);
    crtsurfdata();

    if (strstr(argv[4],"csv")!=0) crtsurffile(argv[2],"csv");
    if (strstr(argv[4],"xml")!=0) crtsurffile(argv[2],"xml");
    if (strstr(argv[4],"json")!=0) crtsurffile(argv[2],"json");


    logfile.write("crtsurfdata 运行结束 \n");

    return 0;
}

void EXIT(int sig)
{
    logfile.write("程序退出,sig=%d\n\n",sig);
    exit(0);
}

bool loadstcode(const string &inifile)
{
    cifile ifile;
    if (ifile.open(inifile) == false)
    {
        logfile.write("ifile.open(%s) failed\n",inifile.c_str());return false;
    }

    string strbuffer;

    ifile.readline(strbuffer);

    ccmdstr cmdstr;
    st_stcode stcode;

    while(ifile.readline(strbuffer))
    {
        //logfile.write("strbuffer=%s\n",strbuffer.c_str());

        cmdstr.splittocmd(strbuffer,",");

        memset(&stcode,0,sizeof(st_stcode));

        cmdstr.getvalue(0,stcode.provname,30);
        cmdstr.getvalue(1,stcode.obtid,10);
        cmdstr.getvalue(2,stcode.obtname,30);
        cmdstr.getvalue(3,stcode.lat);
        cmdstr.getvalue(4,stcode.log);
        cmdstr.getvalue(5,stcode.height);

        stlist.push_back(stcode);
    }

    //for (auto &aa:stlist)
    //{
    //  logfile.write("provname=%s,obtid=%s,obtname=%s,lat=%.2f,log=%.2f,height=%.2f\n",\
    //  aa.provname,aa.obtid,aa.obtname,aa.lat,aa.log,aa.height);
    //}

    return true;
}

void crtsurfdata()
{
    srand(time(0));

    st_sufdata stsufdata;

    for (auto &aa:stlist)
    {
        memset(&stsufdata,0,sizeof(st_sufdata));

        strcpy(stsufdata.obtid,aa.obtid);
        strcpy(stsufdata.ddatetime,strddatetime);
        stsufdata.t=rand()%350;
        stsufdata.p=rand()%265+10000;
        stsufdata.u=rand()%101;
        stsufdata.wd=rand()%360;
        stsufdata.wf=rand()%150;
        stsufdata.r=rand()%16;
        stsufdata.vis=rand()%5001+100000;

        datalist.push_back(stsufdata);
    }

    // for (auto &aa:datalist)
    // {
    //     logfile.write("%s,%s,%.1f,%.1f,%d,%d,%.1f,%.1f,%.1f\n",\
    //          aa.obtid,aa.ddatetime,aa.t/10.0,aa.p/10.0,aa.u,aa.wd,aa.wf/10.0,aa.r/10.0,aa.vis/10.0);
    // }
}

bool crtsurffile(const string& outpath,const string& datafmt)
{

    string strfilename=outpath+"/"+"SURF_ZH_"+strddatetime+"_"+to_string(getpid())+"."+datafmt;
 
    cofile ofile;

    if (ofile.open(strfilename)==false)
    {
        logfile.write("ofile.open(%s) failed.\n",strfilename.c_str());
        return false;
    }

    if (datafmt=="csv") ofile.writeline("站点代码,数据时间,气温,气压,相对温度,风向,风速,降雨量,能见度\n");
    if (datafmt=="xml") ofile.writeline("<data>\n");
    if (datafmt=="json") ofile.writeline("{\"data\":[\n");
    for (auto &aa:datalist)
    {
        if (datafmt=="csv")
           ofile.writeline("%s,%s,%.1f,%.1f,%d,%d,%.1f,%.1f,%.1f\n",\
                             aa.obtid,aa.ddatetime,aa.t/10.0,aa.p/10.0,aa.u,aa.wd,aa.wf/10.0,aa.r/10.0,aa.vis/10.0);
        
        if (datafmt=="xml")
        ofile.writeline("<obtid>%s</obtid><ddatetime>%s</ddatetime><t>%.1f</t><p>%.1f</p><u>%d</u><wd>%d</wd><wf>%.1f</wf><r>%.1f</r><vis>%.1f</vis><endl/>\n",\
            aa.obtid,aa.ddatetime,aa.t/10.0,aa.p/10.0,aa.u,aa.wd,aa.wf/10.0,aa.r/10.0,aa.vis/10.0);

        if (datafmt=="json")
        {
            ofile.writeline("{\"obtid\":\"%s\",\"ddatetime\":\"%s\",\"t\":\"%.1f,\"P\":\"%.1f,\"u\":\"%d,\"wd\":\"%d,\"wf\":\"%.1f,\"r\":\"%.1f,\"vis\":\"%.1f\"}",\
                aa.obtid,aa.ddatetime,aa.t/10.0,aa.p/10.0,aa.u,aa.wd,aa.wf/10.0,aa.r/10.0,aa.vis/10.0);

            static int ii = 0;
            if (ii<datalist.size()-1)
            {
                ofile.writeline(",\n");ii++;
            }
            else
            {
                ofile.writeline("\n");
            }

        }
    }

    if (datafmt=="xml") ofile.writeline("</data>\n");
    if (datafmt=="json") ofile.writeline("]}\n");

    ofile.closeandrename();

    logfile.write("生成数据文件%s成功,数据时间%s,记录数%d\n",strfilename.c_str(),strddatetime,datalist.size());

    return true;
}
 