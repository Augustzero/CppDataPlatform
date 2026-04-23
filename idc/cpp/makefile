PUBINCL = -I/project/public

PUBCPP = /project/public/_public.cpp

ORAINCL = -I$(ORACLE_HOME)/rdbms/public -I/project/public/db/oracle

ORALIB = -L$(ORACLE_HOME)/lib -L.

ORALIBS = -lclntsh

ORACPP = /project/public/db/oracle/_ooci.cpp

CFLAGS = -g

all:crtsurfdata obtcodetodb obtmindtodb

crtsurfdata:crtsurfdata.cpp
	g++ $(CFLAGS) -o crtsurfdata crtsurfdata.cpp $(PUBINCL) $(PUBCPP)
	cp -f crtsurfdata ../bin/.

obtcodetodb:obtcodetodb.cpp
	g++ $(CFLAGS) -o obtcodetodb obtcodetodb.cpp $(PUBINCL) $(PUBCPP) $(ORAINCL) $(ORALIB) $(ORALIBS) $(ORACPP)
	cp -f obtcodetodb ../bin/.

obtmindtodb:obtmindtodb.cpp idcapp.cpp
	g++ $(CFLAGS) -o obtmindtodb obtmindtodb.cpp idcapp.cpp $(PUBINCL) $(PUBCPP) $(ORAINCL) $(ORALIB) $(ORALIBS) $(ORACPP)
	cp -f obtmindtodb ../bin/.
clean:
	rm -f crtsurfdata obtcodetodb obtmindtodb