#启动守护模块
#/project/tools/bin/procctl 10 /project/tools/bin/checkproc /tmp/log/checkproc.log

#生成测试数据
/project/tools/bin/procctl 60 /project/idc/bin/crtsurfdata /project/idc/ini/stcode.ini /tmp/idc/surfdata /log/idc/crtsurfdata.log csv,xml,json

#清理历史数据
/project/tools/bin/procctl 300 /project/tools/bin/deletefiles /tmp/idc/surfdata "*" 0.02

#压缩文件
/project/tools/bin/procctl 300 /project/tools/bin/gzipfiles /log/idc "*.log.20*" 0.02

#从服务端下载文件
/project/tools/bin/procctl 30 /project/tools/bin/ftpgetfiles /log/idc/ftpgetfiles_surfdata.log "<host>192.168.193.128:21</host><mode>1</mode><username>test1</username><password>4321</password><localpath>/idcdata/surfdata</localpath><remotepath>/tmp/idc/surfdata</remotepath><matchname>SURF_ZH*.XML,SURF_ZH*.CSV</matchname><listfilename>/idcdata/ftplist/ftpgetfiles_surfdata.list</listfilename><ptype>1</ptype><okfilename>/idcdata/ftplist/ftpgetfiles_surfdata.xml</okfilename><checkmtime>true</checkmtime><timeout>80</timeout><pname>ftpgetfiles_surfdata</pname>"

#清理下载的文件
/project/tools/bin/procctl 300 /project/tools/bin/deletefiles /idcdata/surfdata "*" 0.04

#上传给服务端文件
/project/tools/bin/procctl 30 /project/tools/bin/ftpputfiles /log/idc/ftpputfiles_surfdata.log "<host>192.168.193.128:21</host><mode>1</mode><username>test1</username><password>4321</password><localpath>/tmp/idc/surfdata</localpath><remotepath>/tmp/ftpputest</remotepath><matchname>SURF_ZH*.JSON</matchname><ptype>1</ptype><okfilename>/idcdata/ftplist/ftpputfiles_surfdata.xml</okfilename><timeout>80</timeout><pname>ftpputfiles_surfdata</pname>"

#清理上传的文件
/project/tools/bin/procctl 300 /project/tools/bin/deletefiles /tmp/ftpputest "*" 0.04

#文件传输的服务端程序
/project/tools/bin/procctl 10 /project/tools/bin/fileserver 5005 /log/idc/fileserver.log

# 把目录/tmp/ftpputest中的文件上传到/tmp/tcpputest目录中。
/project/tools/bin/procctl 20 /project/tools/bin/tcpputfiles /log/idc/tcpputfiles_surfdata.log "<ip>192.168.193.128</ip><port>5005</port><ptype>1</ptype><clientpath>/tmp/ftpputest</clientpath><andchild>true</andchild><matchname>*.XML,*.CSV,*.JSON</matchname><srvpath>/tmp/tcpputest</srvpath><timetvl>10</timetvl><timeout>50</timeout><pname>tcpputfiles_surfdata</pname>"

# 把目录/tmp/tcpputest中的文件下载到/tmp/tcpgetest目录中。
/project/tools/bin/procctl 20 /project/tools/bin/tcpgetfiles /log/idc/tcpgetfiles_surfdata.log "<ip>192.168.193.128</ip><port>5005</port><ptype>1</ptype><srvpath>/tmp/tcpputest</srvpath><andchild>true</andchild><matchname>*.XML,*.CSV,*.JSON</matchname><clientpath>/tmp/tcpgetest</clientpath><timetvl>10</timetvl><timeout>50</timeout><pname>tcpgetfiles_surfdata</pname>"

# 清理/tmp/tcpgetest目录中的历史数据文件。
/project/tools/bin/procctl 300 /project/tools/bin/deletefiles /tmp/tcpgetest "*" 0.02

# 把站点参数数据入库到ZHOBTCODE表中，如果站点不存在则插入，站点已存在则更新。
/project/tools/bin/procctl 120 /project/idc/bin/obtcodetodb /project/idc/ini/stcode.ini "idc/idcpwd@snorcl11g_128" "Simplified Chinese_China.AL32UTF8" /log/idc/obtcodetodb.log

# 把/idcdata/surfdata目录中的气象观测数据文件入库到T_ZHOBTMIND表中
/project/tools/bin/procctl 10 /project/idc/bin/obtmindtodb /idcdata/surfdata "idc/idcpwd@snorcl11g_128" "Simplified Chinese_China.AL32UTF8" /log/idc/obtmindtodb.log

# 执行/project/idc/sql/deletetable.sql脚本,删除T_ZHOBTMIND表两小时之前的数据
/project/tools/bin/procctl 120 /oracle/home/bin/sqlplus idc/idcpwd@snorcl11g_128 @/project/idc/sql/deletetable.sql

# 每隔1小时把T_ZHOBTCODE表中全部的数据抽取出来。
/project/tools/bin/procctl 3600 /project/tools/bin/dminingoracle /log/idc/dminingoracle_ZHOBTCODE.log "<connstr>idc/idcpwd@snorcl11g_128</connstr><charset>Simplified Chinese_China.AL32UTF8</charset><selectsql>select obtid,cityname,provname,lat,lon,height from T_ZHOBTCODE</selectsql><fieldstr>obtid,cityname,provname,lat,lon,height</fieldstr><fieldlen>10,30,30,10,10,10</fieldlen><bfilename>ZHOBTCODE</bfilename><efilename>toidc</efilename><outpath>/idcdata/dmindata</outpath><timeout>30</timeout><pname>dminingoracle_ZHOBTCODE</pname>"

# 每30秒从T_ZHOBTMIND表中增量抽取数据。
/project/tools/bin/procctl 30 /project/tools/bin/dminingoracle /log/idc/dminingoracle_ZHOBTMIND.log "<connstr>idc/idcpwd@snorcl11g_128</connstr><charset>Simplified Chinese_China.AL32UTF8</charset><selectsql>select obtid,to_char(ddatetime,'yyyymmddhh24miss'),t,p,u,wd,wf,r,vis,keyid from T_ZHOBTMIND where keyid>:1 and obtid like '5%%'</selectsql><fieldstr>obtid,ddatetime,t,p,u,wd,wf,r,vis,keyid</fieldstr><fieldlen>5,19,8,8,8,8,8,8,8,15</fieldlen><bfilename>ZHOBTMIND</bfilename><efilename>togxpt</efilename><outpath>/idcdata/dmindata</outpath><starttime></starttime><incfield>keyid</incfield><incfilename>/idcdata/dmining/dminingoracle_ZHOBTMIND_togxpt.keyid</incfilename><timeout>30</timeout><pname>dminingoracle_ZHOBTMIND_togxpt</pname><maxcount>1000</maxcount><connstr1>scott/abc@snorcl11g_128</connstr1>"

# 清理/idcdata/dmindata目录中文件，防止把空间撑满。
/project/tools/bin/procctl 300 /project/tools/bin/deletefiles /idcdata/dmindata "*" 0.02

# 把/idcdata/dmindata目录中的xml文件发送到/idcdata/xmltodb/vip，交给入库程序。
/project/tools/bin/procctl 20 /project/tools/bin/tcpputfiles /log/idc/tcpputfiles_togxpt.log "<ip>192.168.193.128</ip><port>5005</port><ptype>1</ptype><clientpath>/idcdata/dmindata</clientpath><srvpath>/idcdata/xmltodb/vip</srvpath><andchild>true</andchild><matchname>*.XML</matchname><timetvl>10</timetvl><timeout>50</timeout><pname>tcpputfiles_togxpt</pname>"

# 把/idcdata/xmltodb/vip目录中的xml文件入库到T_ZHOBTCODE1和T_ZHOBTMIND1。
/project/tools/bin/procctl 10 /project/tools/bin/xmltodb /log/idc/xmltodb_vip.log "<connstr>idc/idcpwd@snorcl11g_128</connstr><charset>Simplified Chinese_China.AL32UTF8</charset><inifilename>/project/idc/ini/xmltodb.xml</inifilename><xmlpath>/idcdata/xmltodb/vip</xmlpath><xmlpathbak>/idcdata/xmltodb/vipbak</xmlpathbak><xmlpatherr>/idcdata/xmltodb/viperr</xmlpatherr><timetvl>5</timetvl><timeout>50</timeout><pname>xmltodb_vip</pname>"
# 注意，观测数据源源不断的入库到T_ZHOBTMIND1中，为了防止表空间被撑满，在/project/idc/sql/deletetable.sql中要配置清理T_ZHOBTMIND1表中历史数据的脚本。

# 清理/idcdata/xmltodb/vipbak和/idcdata/xmltodb/viperr目录中文件。
/project/tools/bin/procctl 300 /project/tools/bin/deletefiles /idcdata/xmltodb/vipbak "*" 0.02
/project/tools/bin/procctl 300 /project/tools/bin/deletefiles /idcdata/xmltodb/viperr "*" 0.02
