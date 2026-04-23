#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

int main(int argc,char *argv[])
{
    if (argc<3)
    {
        printf("/project/tools/bin/procctl 10 /usr/bin/tar zcvf /tmp/tmp.tgz /usr/include\n");
        printf("/project/tools/bin/procctl 60 /project/idc/bin/crtsurfdata /project/idc/ini/stcode.ini /tmp/idc/surfdata /log/idc/crtsurfdata.log csv,xml,json\n");

        return -1;
    }

    for (int ii=0;ii<64;ii++)
    {
        signal(ii,SIG_IGN);
        close(ii);
    }

    if (fork()!=0) exit(0);

    signal(SIGCHLD,SIG_DFL);

    char *pargv[argc];
    for (int ii = 2; ii<argc;ii++)
        pargv[ii-2]=argv[ii];

    pargv[argc-2]=NULL;

    while(true)
    {
        if (fork()==0)
        {
            execv(argv[2],pargv);
            exit(0);
        }
        else
        {
            int status;
            wait(&status);
            sleep(atoi(argv[1]));
        }
    }
    
}