killall -9 procctl

killall crtsurfdata deletefiles gzipfiles ftpgetfiles ftpputfiles
killall fileserver tcpputfiles tcpgetfiles obtcodetodb obtmindtodb
killall dminingoracle xmltodb

sleep 5

killall -9 crtsurfdata deletefiles gzipfiles ftpgetfiles ftpputfiles
killall -9 fileserver tcpputfiles tcpgetfiles obtcodetodb obtmindtodb
killall -9 dminingoracle xmltodb