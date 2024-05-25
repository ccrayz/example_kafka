#!/bin/bash

# Install Go
wget https://dl.google.com/go/go1.22.3.linux-amd64.tar.gz
sudo tar -C /usr/local -xvf go1.22.3.linux-amd64.tar.gz
echo "export PATH=\$PATH:/usr/local/go/bin" >> $HOME/.profile
source ~/.profile
go version

# Install GVM
bash < <(curl -s -S -L https://raw.githubusercontent.com/moovweb/gvm/master/binscripts/gvm-installer)
source $HOME/.gvm/scripts/gvm
gvm version

