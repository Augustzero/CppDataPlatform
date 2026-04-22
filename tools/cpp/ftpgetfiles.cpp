#include "_public.h"
#include "_ftp.h"


using namespace idc;

void EXIT(int sig);

void _help();

struct st_fileinfo
{
    string filename;
    string mtime;
    st_fileinfo()=default;
    st_fileinfo(const string &in_filename,const string &in_mtime):filename(in_filename),mtime(in_mtime){}
    void clear() {filename.clear();mtime.clear();}
};

map<string,string> mfromok;
list<struct st_fileinfo> vfromnlist;
list<struct st_fileinfo> vtook;
list<struct st_fileinfo> vdownload;

bool loadlistfile();
bool loadokfile();
bool compmap();
bool writetookfile();
bool appendtookfile(struct st_fileinfo &stfileinfo);
struct st_arg
{
    char host[31];
    int mode;
    char username[31];
    char password[31];
    char remotepath[256];
    char localpath[256];
    char matchname[256];
    int ptype;
    char remotepathbak[256];
    char okfilename[256];
    bool checkmtime;
    int timeout;
    char pname[51];
}starg;

bool _xmltoarg(const char *strxmlbuffer);


clogfile logfile;
cftpclient ftp;
cpactive pactive;
int main(int argc, char *argv[])
{

    if (argc != 3) {_help();return -1;}

    
    signal(SIGINT,EXIT);signal(SIGTERM,EXIT);

    if(logfile.open(argv[1])==false)
    {
        printf("打开日志文件失败 (%s)\n",argv[1]);
        return -1;
    }
    

    if (_xmltoarg(argv[2])==false) return -1;

    pactive.addpinfo(starg.timeout,starg.pname);

    if (ftp.login(starg.host,starg.username,starg.password,starg.mode)==false)
    {
        logfile.write("ftp.login(%s,%s,%s) failed\n%s\n",starg.host,starg.username,starg.password,ftp.response());return -1;
    }

    logfile.write("ftp.login ok \n");

    if (ftp.chdir(starg.remotepath)==false)
    {
        logfile.write("ftp.chdir(%s) failed\n%s\n",starg.remotepath,ftp.response());return -1;
    }

    if (ftp.nlist(".",sformat("/tmp/nlist/ftpgetfiles_%d.nlist",getpid()))==false)
    {
        logfile.write("ftp.nlist(%s) failed\n%s\n",starg.remotepath,ftp.response());return -1;
    }
    logfile.write("nlist(%s) ok\n",sformat("/tmp/nlist/ftpgetfiles_%d.nlist",getpid()).c_str());

    pactive.uptatime();
    if (loadlistfile()==false)
    {
        logfile.write("loadlistfile() failed\n");
        return -1;
    }

    if (starg.ptype==1)
    {
        loadokfile();

        compmap();

        writetookfile();
    }
    else
        vfromnlist.swap(vdownload);
    
        pactive.uptatime();
    string strremotefilename,strlocalfilename;

    for (auto &aa:vdownload)
    {
        sformat(strremotefilename,"%s/%s",starg.remotepath,aa.filename.c_str());
        sformat(strlocalfilename,"%s/%s",starg.localpath,aa.filename.c_str());

        logfile.write("get %s ...",strremotefilename.c_str());

        if(ftp.get(strremotefilename,strlocalfilename,starg.checkmtime)==false)
        {
            logfile << "failed\n" << ftp.response() << "\n";
            return -1;
        }

        logfile << "ok\n";

        pactive.uptatime();
        
        if (starg.ptype==1) appendtookfile(aa);

        if(starg.ptype==2)
        {
            if(ftp.ftpdelete(strremotefilename)==false)
            {
                logfile.write("ftp.ftpdelete(%s) failed\n%s\n",strremotefilename.c_str(),ftp.response());
                return -1;
            }
        }

        if(starg.ptype==3)
        {
            string strremotefilenamebak=sformat("%s/%s",starg.remotepathbak,aa.filename.c_str());
            if (ftp.ftprename(strremotefilename,strremotefilenamebak)==false)
            {
                logfile.write("ftp/ftprename(%s.%s) failed\n%s\n",strremotefilename.c_str(),strremotefilenamebak.c_str(),ftp.response());
                return -1;
            }
        }
    }
    return 0;
}

void EXIT(int sig)
{
    printf("程序退出,sig=%d\n\n",sig);

    exit(0);
}

void _help()
{
    printf("\n");
    // printf("/project/tools/bin/ftpgetfiles 30 /log/idc/ftpgetfiles_surfdata.log"\
    //        " \"<host>192.168.193.128</host><mode>1</mode>"\
    //        "<username>test1</username><password>4321</password>"\
    //        "<remotepath>/tmp/idc/surfdata</remotepath><localpath>/idcdata/surfdata</localpath>"\
    //        "<matchname>SURF_ZH*.XML,SURF_ZH*.CSV</matchname>"\
    //        "<ptype>3</ptype><remotepathbak>/tmp/idc/surfdatabak</remotepathbak>\"\n\n");

    printf("/project/tools/bin/ftpgetfiles 30 /log/idc/ftpgetfiles_surfdata.log"\
           " \"<host>192.168.193.128</host><mode>1</mode>"\
           "<username>test1</username><password>4321</password>"\
           "<remotepath>/tmp/ftp/server</remotepath><localpath>/tmp/ftp/client</localpath>"\
           "<matchname>*.TXT</matchname>"\
           "<ptype>1</ptype><okfilename>/idcdata/ftplist/ftpgetfiles_test.xml</okfilename>"\
           "<checkmtime>true</checkmtime>"\
           "<timeout>80</timeout><pname>ftpgetfiles_test</pname>\"\n\n\n");

}

bool _xmltoarg(const char *strxmlbuffer)
{
    memset(&starg,0,sizeof(struct st_arg));

    getxmlbuffer(strxmlbuffer,"host",starg.host,30);
    if(strlen(starg.host)==0)
    {logfile.write("host is null\n");return false;}

    getxmlbuffer(strxmlbuffer,"mode",starg.mode);
    if(starg.mode!=2) starg.mode=1;

    getxmlbuffer(strxmlbuffer,"username",starg.username,30);
    if(strlen(starg.username)==0)
    {logfile.write("username is null\n");return false;}

    getxmlbuffer(strxmlbuffer,"password",starg.password,30);
    if(strlen(starg.password)==0)
    {logfile.write("password is null\n"); return false;}

    getxmlbuffer(strxmlbuffer,"remotepath",starg.remotepath,255);
    if(strlen(starg.remotepath)==0)
    {logfile.write("remotepath is null\n"); return false;}

    getxmlbuffer(strxmlbuffer,"localpath",starg.localpath,255);
    if(strlen(starg.localpath)==0)
    {logfile.write("localpath is null\n"); return false;}


    getxmlbuffer(strxmlbuffer,"matchname",starg.matchname,100);
    if(strlen(starg.matchname)==0)
    {logfile.write("matchname is null\n"); return false;}

    getxmlbuffer(strxmlbuffer,"ptype",starg.ptype);
    if ((starg.ptype!=1)&&(starg.ptype!=2)&&(starg.ptype!=3))
    {logfile.write("ptype is error\n");return false;}

    if (starg.ptype==3)
    {
        getxmlbuffer(strxmlbuffer,"remotepathbak",starg.remotepathbak,255);
        if(strlen(starg.remotepathbak)==0){logfile.write("remotepathbak is null\n"); return false;}
    }

    if (starg.ptype==1)
    {
        getxmlbuffer(strxmlbuffer,"okfilename",starg.okfilename,255);
        if(strlen(starg.okfilename)==0){logfile.write("okfilename is null\n");return false;}

        getxmlbuffer(strxmlbuffer,"checkmtime",starg.checkmtime); 
    }

    getxmlbuffer(strxmlbuffer,"timeout",starg.timeout);
    if (starg.timeout==0) {logfile.write("timeout is null \n"); return false;}

    getxmlbuffer(strxmlbuffer,"pname",starg.pname,50);
    //if (strlen(starg.pname)==0) {logfile.write("pname is null\n"); return false;}
    return true;
}

bool loadlistfile()
{
    vfromnlist.clear();

    cifile ifile;
    if (ifile.open(sformat("/tmp/nlist/ftpgetfiles_%d.nlist",getpid()))==false)
    {
        logfile.write("ifile.open(%s)失败\n",sformat("/tmp/nlist/ftpgetfiles_%d.nlist",getpid())); return false;
    }

    string strfilename;

    while(true)
    {
        if(ifile.readline(strfilename)==false) break;

        if(matchstr(strfilename,starg.matchname)==false) continue;
        
        if((starg.ptype==1)&&(starg.checkmtime==true))
        {
            if(ftp.mtime(strfilename)==false)
            {
                logfile.write("ftp.mtime(%s)failed\n",strfilename.c_str());return false;
            }
        }
        vfromnlist.emplace_back(strfilename,ftp.m_mtime);

        
    }

    ifile.closeandremove();


    //for (auto &aa:vfromnlist)
        //logfile.write("filename=%s,mtime=%s\n",aa.filename.c_str(),aa.mtime.c_str());

        
    return true;
}

bool loadokfile()
{
    if (starg.ptype!=1) return true;

    mfromok.clear();

    cifile ifile;

    if (ifile.open(starg.okfilename)==false) return true;

    string strbuffer;

    struct st_fileinfo stfileinfo;

    while (true)
    {
        stfileinfo.clear();

        if (ifile.readline(strbuffer)==false) break;

        getxmlbuffer(strbuffer,"filename",stfileinfo.filename);
        getxmlbuffer(strbuffer,"mtime",stfileinfo.mtime);

        mfromok[stfileinfo.filename]=stfileinfo.mtime;
    }

    for (auto &aa:mfromok)
        logfile.write("filename=%s,mtime=%s\n",aa.first.c_str(),aa.second.c_str());

    return true;
}

bool compmap()
{
    vtook.clear();
    vdownload.clear();

    for (auto &aa:vfromnlist)
    {
        auto it=mfromok.find(aa.filename);
        if (it!=mfromok.end())
        {
            if (starg.checkmtime==true)
            {
                if (it->second==aa.mtime) vtook.push_back(aa);
                else vdownload.push_back(aa);
            }
            else
            {
                vtook.push_back(aa);
            }
        }
        else
        {
            vdownload.push_back(aa);
        }
    }

    return true;
}

bool writetookfile()
{
    cofile ofile;

    if (ofile.open(starg.okfilename)==false)
    {
        logfile.write("file open(%s) failed\n",starg.okfilename);return false;
    }

    for (auto &aa:vtook)
        ofile.writeline("<filename>%s</filename><mtime>%s</mtime>\n",aa.filename.c_str(),aa.mtime.c_str());

    ofile.closeandrename();

    return true;
}

bool appendtookfile(struct st_fileinfo &stfileinfo)
{
    cofile ofile;

    if (ofile.open(starg.okfilename,false,ios::app)==false)
    {
        logfile.write("file.open(%s) failed \n",starg.okfilename);return false;
    }

    ofile.writeline("<filename>%s</filename><mtime>%s</mtime>\n",stfileinfo.filename.c_str(),stfileinfo.mtime.c_str());

    return true;
}