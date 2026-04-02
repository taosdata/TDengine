#!/bin/bash
wget 'https://bootstrap.pypa.io/get-pip.py'
sudo python get-pip.py
sudo pip install --upgrade httpie
sudo pip install -U httpie-jwt-auth
