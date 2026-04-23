#include "_tools.h"

// 获取表全部的列和主键列信息的类。
ctcols::ctcols()
{
    initdata();  // 调用成员变量初始化函数。
}

void ctcols::initdata()  // 成员变量初始化。
{
    m_vallcols.clear();
    m_vpkcols.clear();
    m_allcols.clear();
    m_pkcols.clear();
}

// 获取指定表的全部字段信息。
bool ctcols::allcols(connection &conn,char *tablename)
{
    m_vallcols.clear();    // 清空m_vallcols容器。
    m_allcols.clear();      // 清空字符串。

    struct st_columns stcolumns;

    sqlstatement stmt;
    stmt.connect(&conn);
    stmt.prepare("\
            select lower(column_name),lower(data_type),data_length from USER_TAB_COLUMNS\
            where table_name=upper(:1) order by column_id",tablename);
    stmt.bindin(1,tablename,30);
    stmt.bindout(1,stcolumns.colname,30);
    stmt.bindout(2,stcolumns.datatype,30);
    stmt.bindout(3,stcolumns.collen);

    if (stmt.execute()!=0) return false;

    while (true)
    {
        memset(&stcolumns,0,sizeof(struct st_columns));
  
        if (stmt.next()!=0) break;


        // 各种字符串类型
        if (strcmp(stcolumns.datatype,"char")==0)            strcpy(stcolumns.datatype,"char");
        if (strcmp(stcolumns.datatype,"nchar")==0)          strcpy(stcolumns.datatype,"char");
        if (strcmp(stcolumns.datatype,"varchar2")==0)     strcpy(stcolumns.datatype,"char");
        if (strcmp(stcolumns.datatype,"nvarchar2")==0)   strcpy(stcolumns.datatype,"char");
        if (strcmp(stcolumns.datatype,"rowid")==0)        { strcpy(stcolumns.datatype,"char");  stcolumns.collen=18; }

        // 日期时间类型
        if (strcmp(stcolumns.datatype,"date")==0)             stcolumns.collen=14; 
    
        // 数字类型
        if (strcmp(stcolumns.datatype,"number")==0)      strcpy(stcolumns.datatype,"number");
        if (strcmp(stcolumns.datatype,"integer")==0)       strcpy(stcolumns.datatype,"number");
        if (strcmp(stcolumns.datatype,"float")==0)           strcpy(stcolumns.datatype,"number");  

        if ( (strcmp(stcolumns.datatype,"char")!=0) &&
             (strcmp(stcolumns.datatype,"date")!=0) &&
             (strcmp(stcolumns.datatype,"number")!=0) ) continue;

        // 字段类型是number
        if (strcmp(stcolumns.datatype,"number")==0) stcolumns.collen=22;

        m_allcols = m_allcols + stcolumns.colname + ",";       

        m_vallcols.push_back(stcolumns);                                
    }

    if (stmt.rpc()>0) deleterchr(m_allcols,',');         

    return true;
}

// 获取指定表的主键字段信息。
bool ctcols::pkcols(connection &conn,char *tablename)
{
    m_vpkcols.clear();      
    m_pkcols.clear();     

    struct st_columns stcolumns;

    sqlstatement stmt;
    stmt.connect(&conn);
    stmt.prepare("select lower(column_name),position from USER_CONS_COLUMNS\
         where table_name=upper(:1)\
           and constraint_name=(select constraint_name from USER_CONSTRAINTS\
                               where table_name=upper(:2) and constraint_type='P'\
                                 and generated='USER NAME')\
         order by position");
    stmt.bindin(1,tablename,30);
    stmt.bindin(2,tablename,30);
    stmt.bindout(1,stcolumns.colname,30);
    stmt.bindout(2,stcolumns.pkseq);

    if (stmt.execute() != 0) return false;

    while (true)
    {
        memset(&stcolumns,0,sizeof(struct st_columns));

        if (stmt.next() != 0) break;

        m_pkcols = m_pkcols + stcolumns.colname + ",";        

        m_vpkcols.push_back(stcolumns);                                  
    }

    if (stmt.rpc()>0) deleterchr(m_pkcols,',');   

    // 更新m_vallcols中的pkseq成员
    for (auto &aa : m_vpkcols)
    {
        for (auto &bb : m_vallcols)
        {
            if (strcmp(aa.colname,bb.colname)==0)
            {
                bb.pkseq=aa.pkseq; break;
            }
        }
    }
    
    return true;
}