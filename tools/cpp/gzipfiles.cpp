#include "_public.h"
using namespace idc;

void EXIT(int sig);

cpactive pactive;
int main(int argc,char *argv[])
{
    if (argc != 4)
    {
        printf("\n");
        printf("Example:/project/tools/bin/gzipfiles /tmp/idc/surfdata \"*.xml,*.json\" 0.01\n");
        cout << R"(      /project/tools/bin/gzipfiles /log/idc "*.log.20*" 0.02)" << endl;
    }

    closeioandsignal(true);
    signal(2,EXIT);signal(15,EXIT);
    
    pactive.addpinfo(120,"deletefiles");
    string strtimeout=ltime1("yyyymmddhh24miss",0-(int)(atof(argv[3])*24*60*60));
    //cout << "strtimeout=" << strtimeout << endl;

    cdir dir;
    if(dir.opendir(argv[1],argv[2],10000,true) == false)
    {
        printf("dir.opendir(%s) failed.\n",argv[1]);return -1;
    }
    
    while(dir.readdir()==true)
    {
        if ((dir.m_mtime < strtimeout) && (matchstr(dir.m_filename,"*.gz")==false))
        {
            string strcmd="/usr/bin/gzip -f "+ dir.m_ffilename + " 1>/dev/null 2>/dev/null";
            if (system(strcmd.c_str())==0)
                cout << "gzip" << dir.m_ffilename << "ok\n";
            else
                cout << "gzip" << dir.m_ffilename << "falied\n";

                pactive.uptatime();
        }
    }
    return 0;
}

void EXIT(int sig)
{
    printf("sig=%d\n",sig);
    exit(0);
}