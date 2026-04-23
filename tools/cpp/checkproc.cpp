#include "_public.h"
using namespace idc;

int main(int argc,char *argv[])
{
    if (argc !=2)
    {
        printf("Example:/project/tools/bin/procctl 10 /project/tools/bin/checkproc /tmp/log/checkproc.log\n\n");
        return -1;
    }
    closeioandsignal(true);
    clogfile logfile;

    if (logfile.open(argv[1])==false)
    {
        printf("logfile.open(%s) failed\n",argv[1]);
        return -1;
    }

    int shmid=0;
    if ((shmid = shmget((key_t)SHMKEYP,MAXNUMP*sizeof(struct st_procinfo),0666|IPC_CREAT)) == -1)
    {
        logfile.write("创建/获取共享内存(%x)失败\n",SHMKEYP);
        return false;
    }

    struct st_procinfo *shm=(struct st_procinfo *)shmat(shmid,0,0);

    for (int ii=0;ii<MAXNUMP;ii++)
    {
        if (shm[ii].pid==0) continue;

       // logfile.write("ii=%d,pid=%d,pname=%s,timeout=%d,atime=%d\n",\
                   ii,shm[ii].pid,shm[ii].pname,shm[ii].timeout,shm[ii].atime);
        
        int iret=kill(shm[ii].pid,0);
        if (iret==-1)
        {
            logfile.write("进程pid=%d(%s)已经不存在\n",shm[ii].pid,shm[ii].pname);
            memset(&shm[ii],0,sizeof(struct st_procinfo));
            continue;
        }           
        time_t now=time(0);

        if (now-shm[ii].atime<shm[ii].timeout) continue;
        struct st_procinfo tmp=shm[ii];
        if (tmp.pid==0) continue;
        logfile.write("进程pid=%d(%s)已经超时\n",tmp.pid,tmp.pname);

        kill(tmp.pid,15);

        for (int jj=0;jj < 5;jj++)
        {
            sleep(1);
            iret=kill(tmp.pid,0);
            if (iret==-1) break;
        }

        if (iret==-1)
            logfile.write("进程pid=%d(%s)已经正常终止\n",tmp.pid,tmp.pname);
        else
        {
            kill(tmp.pid,9);
            logfile.write("进程pid=%d(%s)已经强制终止 \n",tmp.pid,tmp.pname);
            memset(shm+ii,0,sizeof(struct st_procinfo));
        }
    }
    return 0;
}